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
    result = interpolate(series_id, method)

    # Простой график
    plt.figure(figsize=(12, 6))
    plt.plot(
        result["date"], result["value"], "b-", alpha=0.8, label=f"{method} интерполяция"
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
