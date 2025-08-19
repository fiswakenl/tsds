import setup_env
import matplotlib.pyplot as plt
from interpolation_controller import interpolate, compare_all_methods
from data_analyzer import analyze_data, get_series_data
from auto_interpolation import auto_select_method
from adaptive_interpolation import adaptive_interpolate, visualize_adaptive_interpolation
from pathlib import Path

plt.rcParams["figure.figsize"] = (12, 6)

df_clean, top_stats, top_ids = analyze_data()


# %% Быстрая интерполяция с графиком
def quick_plot(series_id, method="linear"):
    # Загружаем исходные данные через data_analyzer
    original_data = get_series_data(df_clean, series_id)

    if original_data.empty:
        return None

    # Автоматический выбор метода если method='auto'
    if method == 'auto':
        selection_result = auto_select_method(original_data)
        method = selection_result['selected_method']
        print(f"Автоматически выбран метод: {method} ({selection_result['confidence']} уверенность)")

    # Выполняем интерполяцию (создаем временный CSV для совместимости)
    temp_path = Path("data") / "temp"
    temp_path.mkdir(exist_ok=True)
    temp_file = temp_path / f"series_{series_id}.csv"
    original_data.to_csv(temp_file, index=False)

    result = interpolate(series_id, method, input_dir="data/temp", save_results=False)

    # График с исходными и интерполированными данными
    plt.figure(figsize=(12, 6))

    # Интерполированные данные (линия) - рисуем сначала
    plt.plot(
        result["date"],
        result["value"],
        "b-",
        alpha=0.8,
        linewidth=2,
        label=f"{method} интерполяция",
    )

    # Исходные данные (точки) - рисуем поверх линии
    plt.plot(
        original_data["date"],
        original_data["value"],
        "ro",
        alpha=0.8,
        markersize=5,
        label="Исходные данные",
        zorder=10,
    )

    plt.title(f"Временной ряд {series_id}")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # Удаляем временный файл
    temp_file.unlink()

    return None


def quick_compare(series_id):
    # Загружаем исходные данные через data_analyzer
    original_data = get_series_data(df_clean, series_id)

    if original_data.empty:
        return None

    # Выполняем сравнение (создаем временный CSV для совместимости)
    temp_path = Path("data") / "temp"
    temp_path.mkdir(exist_ok=True)
    temp_file = temp_path / f"series_{series_id}.csv"
    original_data.to_csv(temp_file, index=False)

    print(f"Сравнение всех методов для серии {series_id}:")
    results = compare_all_methods(series_id, save_results=False, input_dir="data/temp")

    # Удаляем временный файл
    temp_file.unlink()

    # Фильтруем успешные результаты
    successful_results = {
        method: result for method, result in results.items() if result is not None
    }

    if not successful_results:
        return results

    # Создаем графики для каждого успешного метода
    num_methods = len(successful_results)
    colors = ["blue", "green", "orange", "purple", "brown", "pink"]

    # Определяем размеры subplot
    if num_methods == 1:
        rows, cols = 1, 1
    elif num_methods == 2:
        rows, cols = 1, 2
    elif num_methods <= 4:
        rows, cols = 2, 2
    else:
        rows, cols = 2, 3

    fig, axes = plt.subplots(rows, cols, figsize=(15, 10))
    if num_methods == 1:
        axes = [axes]
    elif rows == 1 or cols == 1:
        axes = axes.flatten()
    else:
        axes = axes.flatten()

    for i, (method, result) in enumerate(successful_results.items()):
        ax = axes[i]
        color = colors[i % len(colors)]

        # Интерполированные данные (линия) - рисуем сначала
        ax.plot(
            result["date"],
            result["value"],
            f"{color[0]}-",
            alpha=0.8,
            linewidth=2,
            label=f"{method} интерполяция",
        )

        # Исходные данные (точки) - рисуем поверх линии
        ax.plot(
            original_data["date"],
            original_data["value"],
            "ro",
            alpha=0.8,
            markersize=5,
            label="Исходные данные",
            zorder=10,
        )

        ax.set_title(f"Метод: {method} ({len(result)} точек)")
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis="x", rotation=45)

    # Скрываем лишние subplot'ы
    for i in range(num_methods, len(axes)):
        axes[i].set_visible(False)

    plt.suptitle(f"Сравнение методов интерполяции - Серия {series_id}", fontsize=14)
    plt.tight_layout()
    plt.show()

    return None


def auto_interpolate(series_id):
    # Загружаем исходные данные через data_analyzer
    original_data = get_series_data(df_clean, series_id)
    
    if original_data.empty:
        return None

    # Автоматически выбираем метод
    selection_result = auto_select_method(original_data)
    best_method = selection_result['selected_method']
    
    print(f"\n=== АВТОМАТИЧЕСКИЙ ВЫБОР МЕТОДА ===")
    print(f"Выбранный метод: {best_method}")
    print(f"Уверенность: {selection_result['confidence']}")
    print(f"Обоснование: {selection_result['reason']}")
    
    # Выполняем интерполяцию выбранным методом
    temp_path = Path("data") / "temp"
    temp_path.mkdir(exist_ok=True)
    temp_file = temp_path / f"series_{series_id}.csv"
    original_data.to_csv(temp_file, index=False)
    
    result = interpolate(series_id, best_method, input_dir="data/temp", save_results=False)

    # График с исходными и интерполированными данными
    plt.figure(figsize=(12, 6))

    # Интерполированные данные (линия) - рисуем сначала
    plt.plot(
        result["date"],
        result["value"],
        "g-",
        alpha=0.8,
        linewidth=2,
        label=f"{best_method} интерполяция (авто)",
    )

    # Исходные данные (точки) - рисуем поверх линии
    plt.plot(
        original_data["date"],
        original_data["value"],
        "ro",
        alpha=0.8,
        markersize=5,
        label="Исходные данные",
        zorder=10,
    )

    plt.title(f"Автоматическая интерполяция - Серия {series_id}\nМетод: {best_method} (уверенность: {selection_result['confidence']})")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    
    # Удаляем временный файл
    temp_file.unlink()

    return result, selection_result


def adaptive_plot(series_id):
    # Загружаем исходные данные через data_analyzer
    original_data = get_series_data(df_clean, series_id)
    
    if original_data.empty:
        return None

    print(f"\n=== АДАПТИВНАЯ ИНТЕРПОЛЯЦИЯ ===")
    print(f"Анализ серии {series_id}: каждый пропуск обрабатывается индивидуально")
    
    # Выполняем адаптивную интерполяцию
    result = adaptive_interpolate(original_data)
    
    # Визуализируем результаты
    visualize_adaptive_interpolation(original_data, result, series_id)
    
    return result


print("\nГОТОВ К АНАЛИЗУ!")
for series_id in top_ids[:5]:  # показываем первые 5 из топ-10


adaptive_plot(str(top_ids[0]))

# %%
