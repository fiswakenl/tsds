# %% Импорты
import matplotlib.pyplot as plt
from interpolation_controller import interpolate, compare_all_methods
import subprocess
import sys

plt.rcParams["figure.figsize"] = (12, 6)

# %% Получить топ временные ряды (Polars - супер быстро!)
subprocess.run([sys.executable, "series_extraction.py"], check=True)


# %% Быстрая интерполяция с графиком
def quick_plot(series_id, method="linear"):
    """Интерполяция + график одной командой"""
    # Загружаем исходные данные
    import pandas as pd
    from pathlib import Path

    input_path = Path("data/top_series") / f"series_{series_id}.csv"
    original_data = pd.read_csv(input_path, parse_dates=["date"])

    # Выполняем интерполяцию
    result = interpolate(series_id, method)

    # График с исходными и интерполированными данными
    plt.figure(figsize=(12, 6))

    # Исходные данные (точки)
    plt.plot(
        original_data["date"],
        original_data["value"],
        "ro",
        alpha=0.7,
        markersize=4,
        label="Исходные данные",
    )

    # Интерполированные данные (линия)
    plt.plot(
        result["date"],
        result["value"],
        "b-",
        alpha=0.8,
        linewidth=2,
        label=f"{method} интерполяция",
    )

    plt.title(f"Временной ряд {series_id}")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    return result


# %% Примеры использования
print("ГОТОВ К АНАЛИЗУ!")
print("Примеры команд:")
print("quick_plot('160077920', 'linear')")
print("quick_plot('160077920', 'log')")
print("compare_all_methods('160077920')")
# %%
quick_plot("160077920", "linear")

# %%
