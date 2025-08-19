"""
Автоматический выбор оптимального метода интерполяции временных рядов.
Использует cross-validation и метрики качества для выбора лучшего метода.
"""

import pandas as pd
import numpy as np
from interpolation_controller import InterpolationController
from pathlib import Path
import random


def mean_squared_error(y_true, y_pred):
    """Реализация MSE без sklearn"""
    return np.mean((y_true - y_pred) ** 2)


def mean_absolute_error(y_true, y_pred):
    """Реализация MAE без sklearn"""
    return np.mean(np.abs(y_true - y_pred))


def r2_score(y_true, y_pred):
    """Реализация R² без sklearn"""
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    return 1 - (ss_res / ss_tot) if ss_tot != 0 else 0


def calculate_interpolation_metrics(original_values, interpolated_values):
    """
    Вычисляет метрики качества интерполяции.
    
    Args:
        original_values: Исходные значения
        interpolated_values: Интерполированные значения
        
    Returns:
        dict: Словарь с метриками качества
    """
    # Убираем NaN значения для корректного сравнения
    mask = ~(np.isnan(original_values) | np.isnan(interpolated_values))
    orig_clean = original_values[mask]
    interp_clean = interpolated_values[mask]
    
    if len(orig_clean) < 2:
        return {
            'mse': float('inf'),
            'mae': float('inf'),
            'r2': -float('inf'),
            'valid_points': len(orig_clean)
        }
    
    mse = mean_squared_error(orig_clean, interp_clean)
    mae = mean_absolute_error(orig_clean, interp_clean)
    r2 = r2_score(orig_clean, interp_clean)
    
    return {
        'mse': mse,
        'mae': mae,
        'r2': r2,
        'rmse': np.sqrt(mse),
        'valid_points': len(orig_clean)
    }


def cross_validation_interpolation(data, test_ratio=0.2, random_seed=42):
    """
    Проводит cross-validation для оценки качества методов интерполяции.
    
    Args:
        data: DataFrame с колонками ['id', 'date', 'value']
        test_ratio: Доля данных для тестирования (по умолчанию 20%)
        random_seed: Seed для воспроизводимости результатов
        
    Returns:
        dict: Результаты оценки для каждого метода
    """
    random.seed(random_seed)
    np.random.seed(random_seed)
    
    # Копируем данные
    data_copy = data.copy()
    
    # Случайно выбираем точки для скрытия (кроме первой и последней)
    available_indices = list(range(1, len(data_copy) - 1))
    n_test = max(1, int(len(available_indices) * test_ratio))
    test_indices = random.sample(available_indices, min(n_test, len(available_indices)))
    
    # Сохраняем оригинальные значения тестовых точек
    test_values = data_copy.iloc[test_indices]['value'].values
    test_dates = data_copy.iloc[test_indices]['date'].values
    
    # Скрываем тестовые точки (заменяем на NaN)
    data_with_gaps = data_copy.copy()
    data_with_gaps.iloc[test_indices, data_with_gaps.columns.get_loc('value')] = np.nan
    
    # Тестируем каждый метод
    controller = InterpolationController()
    methods = controller.list_methods()
    results = {}
    
    # Создаем временный файл
    temp_path = Path("data") / "temp"
    temp_path.mkdir(exist_ok=True)
    temp_file = temp_path / "cv_test.csv"
    
    try:
        data_with_gaps.to_csv(temp_file, index=False)
        
        for method in methods:
            try:
                # Выполняем интерполяцию
                interpolated = controller.interpolate_series(
                    "cv_test", method, input_dir="data/temp", save_results=False
                )
                
                # Находим интерполированные значения для тестовых точек
                interpolated_test_values = []
                for test_date in test_dates:
                    # Ищем ближайшую дату в интерполированных данных
                    date_diff = abs(interpolated['date'] - pd.to_datetime(test_date))
                    closest_idx = date_diff.idxmin()
                    interpolated_test_values.append(interpolated.iloc[closest_idx]['value'])
                
                interpolated_test_values = np.array(interpolated_test_values)
                
                # Вычисляем метрики
                metrics = calculate_interpolation_metrics(test_values, interpolated_test_values)
                metrics['method'] = method
                metrics['test_points'] = len(test_values)
                
                results[method] = metrics
                
                print(f"[CV] {method}: MSE={metrics['mse']:.2f}, MAE={metrics['mae']:.2f}, R2={metrics['r2']:.3f}")
                
            except Exception as e:
                print(f"[CV] Ошибка в методе {method}: {e}")
                results[method] = {
                    'mse': float('inf'),
                    'mae': float('inf'), 
                    'r2': -float('inf'),
                    'method': method,
                    'error': str(e)
                }
    finally:
        # Удаляем временный файл
        if temp_file.exists():
            temp_file.unlink()
    
    return results


def analyze_data_characteristics(data):
    """
    Анализирует характеристики временного ряда для рекомендации метода.
    
    Args:
        data: DataFrame с временным рядом
        
    Returns:
        dict: Характеристики данных
    """
    values = data['value'].dropna()
    
    if len(values) < 3:
        return {'recommendation': 'linear', 'reason': 'Недостаточно данных'}
    
    # Анализ тренда
    x = np.arange(len(values))
    trend_coef = np.polyfit(x, values, 1)[0]
    
    # Анализ монотонности
    diffs = np.diff(values)
    monotonic_increasing = np.all(diffs >= 0)
    monotonic_decreasing = np.all(diffs <= 0)
    
    # Анализ экспоненциального роста (проверяем log-linear зависимость)
    if np.all(values > 0):
        log_values = np.log(values)
        log_trend_r2 = r2_score(log_values, np.polyfit(x, log_values, 1)[0] * x + np.polyfit(x, log_values, 1)[1])
    else:
        log_trend_r2 = 0
    
    # Анализ нелинейности (полиномиальная vs линейная)
    linear_fit = np.polyval(np.polyfit(x, values, 1), x)
    poly_fit = np.polyval(np.polyfit(x, values, 2), x)
    
    linear_r2 = r2_score(values, linear_fit)
    poly_r2 = r2_score(values, poly_fit)
    
    # Анализ изменчивости
    volatility = np.std(diffs) / np.mean(np.abs(values)) if np.mean(np.abs(values)) > 0 else 0
    
    characteristics = {
        'trend_coefficient': trend_coef,
        'monotonic_increasing': monotonic_increasing,
        'monotonic_decreasing': monotonic_decreasing,
        'log_trend_r2': log_trend_r2,
        'linear_r2': linear_r2,
        'polynomial_r2': poly_r2,
        'volatility': volatility,
        'data_points': len(values),
        'positive_values_only': np.all(values > 0)
    }
    
    # Рекомендация на основе характеристик
    if log_trend_r2 > 0.8 and characteristics['positive_values_only']:
        recommendation = 'log'
        reason = f'Экспоненциальный тренд (log R2={log_trend_r2:.3f})'
    elif poly_r2 - linear_r2 > 0.1 and volatility < 0.2:
        recommendation = 'polynomial'
        reason = f'Нелинейный тренд (poly R2={poly_r2:.3f} vs linear R2={linear_r2:.3f})'
    elif volatility > 0.3:
        recommendation = 'spline'
        reason = f'Высокая изменчивость (volatility={volatility:.3f})'
    else:
        recommendation = 'linear'
        reason = f'Линейный тренд подходит (R2={linear_r2:.3f})'
    
    characteristics['recommendation'] = recommendation
    characteristics['reason'] = reason
    
    return characteristics


def auto_select_method(data, use_cv=True, cv_weight=0.7):
    """
    Автоматически выбирает оптимальный метод интерполяции.
    
    Args:
        data: DataFrame с временным рядом
        use_cv: Использовать ли cross-validation
        cv_weight: Вес CV результатов vs анализа характеристик
        
    Returns:
        dict: Результат выбора с обоснованием
    """
    print("Автоматический выбор метода интерполяции...")
    
    # Анализ характеристик данных
    characteristics = analyze_data_characteristics(data)
    char_recommendation = characteristics['recommendation']
    
    print(f"Анализ характеристик: {char_recommendation} ({characteristics['reason']})")
    
    if not use_cv or len(data) < 10:
        return {
            'selected_method': char_recommendation,
            'confidence': 'medium',
            'reason': f"Основано на анализе характеристик: {characteristics['reason']}",
            'characteristics': characteristics
        }
    
    # Cross-validation
    print("Проведение cross-validation...")
    cv_results = cross_validation_interpolation(data)
    
    # Находим лучший метод по CV
    valid_results = {k: v for k, v in cv_results.items() if v['mse'] != float('inf')}
    
    if not valid_results:
        return {
            'selected_method': char_recommendation,
            'confidence': 'low',
            'reason': "CV не дал результатов, используем анализ характеристик",
            'characteristics': characteristics
        }
    
    # Выбираем лучший по комбинации метрик (MSE важнее, но учитываем R²)
    best_cv_method = min(valid_results.keys(), 
                        key=lambda m: valid_results[m]['mse'] - valid_results[m]['r2'])
    
    print(f"Лучший по CV: {best_cv_method}")
    
    # Комбинируем результаты
    if best_cv_method == char_recommendation:
        confidence = 'high'
        final_method = best_cv_method
        reason = f"CV и анализ характеристик согласуются: {best_cv_method}"
    else:
        # Взвешенное решение
        cv_score = valid_results[best_cv_method]['r2'] - valid_results[best_cv_method]['mse'] * 0.01
        char_score = 1.0 if char_recommendation in valid_results else 0.5
        
        if cv_weight * cv_score > (1 - cv_weight) * char_score:
            final_method = best_cv_method
            confidence = 'medium'
            reason = f"CV выбрал {best_cv_method}, характеристики предлагали {char_recommendation}"
        else:
            final_method = char_recommendation
            confidence = 'medium'
            reason = f"Характеристики выбрали {char_recommendation}, CV предлагал {best_cv_method}"
    
    return {
        'selected_method': final_method,
        'confidence': confidence,
        'reason': reason,
        'cv_results': cv_results,
        'characteristics': characteristics,
        'all_methods_performance': valid_results
    }


if __name__ == "__main__":
    # Тестирование
    from data_analyzer import analyze_data, get_series_data
    
    print("Тестирование автоматического выбора метода...")
    df_clean, top_stats, top_ids = analyze_data()
    
    # Тестируем на первой серии
    test_series_id = str(top_ids[0])
    test_data = get_series_data(df_clean, test_series_id)
    
    result = auto_select_method(test_data)
    
    print(f"\n=== РЕЗУЛЬТАТ ===")
    print(f"Рекомендуемый метод: {result['selected_method']}")
    print(f"Уверенность: {result['confidence']}")
    print(f"Обоснование: {result['reason']}")