# %% TSDS - Time Series Data Analysis
import os
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

from core import plot_series, compare_all_methods, top_ids

print(f"Доступные серии: {top_ids[:5]}")
print("\nПримеры использования:")
print(f"plot_series('{top_ids[0]}')  # автоматический выбор метода")
print(f"plot_series('{top_ids[0]}', 'linear')  # конкретный метод")
print(f"compare_all_methods('{top_ids[0]}')  # сравнение всех методов")

# %% Быстрая интерполяция с автовыбором метода
plot_series(top_ids[0])

# %% Интерполяция конкретным методом
plot_series(top_ids[0], 'polynomial')

# %% Сравнение всех методов
results = compare_all_methods(top_ids[0])

# %% Анализ других серий
plot_series(top_ids[1])
compare_all_methods(top_ids[1])

# %%