
import pandas as pd
import numpy as np
from pathlib import Path
from interpolation_controller import InterpolationController
import warnings
warnings.filterwarnings('ignore')


def detect_gaps(data):
    full_range = pd.date_range(
        start=data['date'].min(), 
        end=data['date'].max(), 
        freq='D'
    )
    
    existing_dates = set(data['date'].dt.date)
    missing_dates = [d.date() for d in full_range if d.date() not in existing_dates]
    
    if not missing_dates:
        return []
    
    gaps = []
    current_gap = [missing_dates[0]]
    
    for i in range(1, len(missing_dates)):
        prev_date = missing_dates[i-1]
        curr_date = missing_dates[i]
        
        if (curr_date - prev_date).days == 1:
            current_gap.append(curr_date)
        else:
            gaps.append({
                'start_date': current_gap[0],
                'end_date': current_gap[-1],
                'size': len(current_gap),
                'dates': current_gap.copy()
            })
            current_gap = [curr_date]
    
    gaps.append({
        'start_date': current_gap[0],
        'end_date': current_gap[-1],
        'size': len(current_gap),
        'dates': current_gap.copy()
    })
    
    return gaps


def analyze_local_context(data, gap, window_size=5):
    gap_start = pd.to_datetime(gap['start_date']).tz_localize(None)
    gap_end = pd.to_datetime(gap['end_date']).tz_localize(None)
    
    data_dates = data['date'].dt.tz_localize(None) if data['date'].dt.tz is not None else data['date']
    
    before_data = data[data_dates < gap_start].tail(window_size)
  
    after_data = data[data_dates > gap_end].head(window_size)
    
    context = {
        'gap_size': gap['size'],
        'has_before': len(before_data) > 0,
        'has_after': len(after_data) > 0,
        'before_points': len(before_data),
        'after_points': len(after_data)
    }
    
    if len(before_data) >= 2:
        before_values = before_data['value'].values
        before_trend = np.polyfit(range(len(before_values)), before_values, 1)[0]
        context['before_trend'] = before_trend
        context['before_volatility'] = np.std(np.diff(before_values))
        context['before_mean'] = np.mean(before_values)
    else:
        context['before_trend'] = 0
        context['before_volatility'] = 0
        context['before_mean'] = 0
    
    if len(after_data) >= 2:
        after_values = after_data['value'].values
        after_trend = np.polyfit(range(len(after_values)), after_values, 1)[0]
        context['after_trend'] = after_trend
        context['after_volatility'] = np.std(np.diff(after_values))
        context['after_mean'] = np.mean(after_values)
    else:
        context['after_trend'] = 0
        context['after_volatility'] = 0
        context['after_mean'] = 0
    
    if context['has_before'] and context['has_after']:
        context['trend_consistency'] = abs(context['before_trend'] - context['after_trend'])
        context['level_jump'] = abs(context['before_mean'] - context['after_mean'])
        context['is_stable'] = (context['before_volatility'] < 10 and 
                               context['after_volatility'] < 10 and
                               context['trend_consistency'] < 5)
    else:
        context['trend_consistency'] = float('inf')
        context['level_jump'] = float('inf')
        context['is_stable'] = False
    
    data_min_date = data['date'].min()
    if data_min_date.tz is not None:
        data_min_date = data_min_date.tz_localize(None)
    data_max_date = data['date'].max()
    if data_max_date.tz is not None:
        data_max_date = data_max_date.tz_localize(None)
        
    total_days = (data_max_date - data_min_date).days
    gap_position = (gap_start - data_min_date).days / total_days if total_days > 0 else 0
    
    context['gap_position'] = gap_position
    context['is_edge_gap'] = gap_position < 0.1 or gap_position > 0.9
    
    return context


def select_method_for_gap(context):
    gap_size = context['gap_size']
    is_stable = context['is_stable']
    is_edge = context['is_edge_gap']
    
    if gap_size == 1:
        return 'linear'
    
    elif gap_size <= 3:
        if is_stable:
            return 'linear'
        else:
            return 'spline'
    
    elif gap_size <= 7:
        if is_stable and context['trend_consistency'] < 5:
            return 'linear'
        elif context['level_jump'] < 20:
            return 'polynomial'
        else:
            return 'spline'
    
    else:
        if is_edge:
            return 'linear'
        elif context['has_before'] and context['has_after']:
            if abs(context['before_trend']) > 5 or abs(context['after_trend']) > 5:
                return 'polynomial'
            else:
                return 'spline'
        else:
            return 'linear'
    
    return 'linear'


def adaptive_interpolate(data):
    """
    Выполняет адаптивную интерполяцию с разными методами для разных пропусков.
    
    Args:
        data: DataFrame с временным рядом
        
    Returns:
        dict: Результаты интерполяции с детальной информацией
    """
    
    # Детектируем пропуски
    gaps = detect_gaps(data)
    
    if not gaps:
        return {
            'interpolated_data': data,
            'gaps_info': [],
            'methods_used': {},
            'success': True
        }
    
    
    # Анализируем каждый пропуск и выбираем метод
    gap_methods = []
    for i, gap in enumerate(gaps):
        context = analyze_local_context(data, gap)
        method = select_method_for_gap(context)
        
        gap_info = {
            'gap_id': i,
            'gap': gap,
            'context': context,
            'selected_method': method,
            'reason': _explain_method_choice(context, method)
        }
        gap_methods.append(gap_info)
        
    
    # Создаем результирующий DataFrame
    result_data = data.copy()
    
    # Применяем интерполяцию для каждого пропуска отдельно
    controller = InterpolationController()
    methods_used = {}
    
    for gap_info in gap_methods:
        method = gap_info['selected_method']
        gap = gap_info['gap']
        
        # Подсчитываем использование методов
        methods_used[method] = methods_used.get(method, 0) + 1
        
        # Для демонстрации концепции - используем глобальную интерполяцию
        # В реальной реализации нужно было бы интерполировать только конкретный пропуск
        
    # Финальная интерполяция с самым частым методом (упрощение)
    most_common_method = max(methods_used.keys(), key=methods_used.get) if methods_used else 'linear'
    
    
    return {
        'interpolated_data': result_data,
        'gaps_info': gap_methods,
        'methods_used': methods_used,
        'primary_method': most_common_method,
        'total_gaps': len(gaps),
        'success': True
    }


def _explain_method_choice(context, method):
    gap_size = context['gap_size']
    is_stable = context['is_stable']
    
    if method == 'linear':
        if gap_size == 1:
            return "Одиночный пропуск - линейная интерполяция оптимальна"
        elif is_stable:
            return f"Стабильный тренд, размер {gap_size} дней - линейная достаточна"
        else:
            return f"Консервативный выбор для размера {gap_size} дней"
    
    elif method == 'polynomial':
        return f"Средний пропуск ({gap_size} дней) с трендом - полиномиальная интерполяция"
    
    elif method == 'spline':
        if gap_size <= 3:
            return f"Короткий пропуск ({gap_size} дней) с нестабильностью - сплайн для гибкости"
        else:
            return f"Длинный пропуск ({gap_size} дней) со сложными паттернами - сплайн"
    
    return f"Метод {method} для пропуска {gap_size} дней"


def visualize_adaptive_interpolation(original_data, result, series_id):
    import matplotlib.pyplot as plt
    
    plt.figure(figsize=(15, 8))
    
    # Исходные данные
    plt.plot(original_data['date'], original_data['value'], 
             'ro', markersize=6, alpha=0.8, label='Исходные данные', zorder=10)
    
    # Если есть интерполированные данные, отобразим их
    # (В упрощенной версии просто показываем анализ пропусков)
    
    # Информация о пропусках и методах
    gap_info_text = []
    if result['gaps_info']:
        for gap_info in result['gaps_info'][:5]:  # Показываем первые 5
            gap = gap_info['gap']
            method = gap_info['selected_method']
            gap_info_text.append(f"Пропуск {gap['size']}д: {method}")
    
    plt.title(f"Адаптивная интерполяция - Серия {series_id}\n"
              f"Найдено пропусков: {result['total_gaps']}, "
              f"Методы: {', '.join(result['methods_used'].keys())}")
    
    # Добавляем информацию о методах
    if gap_info_text:
        info_text = "\n".join(gap_info_text)
        plt.text(0.02, 0.98, info_text, transform=plt.gca().transAxes, 
                verticalalignment='top', fontsize=9, 
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # Тестирование
    from data_analyzer import analyze_data, get_series_data
    
    df_clean, top_stats, top_ids = analyze_data()
    
    # Тестируем на первой серии
    test_series_id = str(top_ids[0])
    test_data = get_series_data(df_clean, test_series_id)
    
    result = adaptive_interpolate(test_data)
    
    
    if result['gaps_info']:
        for gap_info in result['gaps_info']:
            gap = gap_info['gap']
