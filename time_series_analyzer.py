import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime

# Загрузка и обработка данных
raw_df = (
    pl.scan_csv("data/raw/collected.csv", has_header=False,
                new_columns=["row", "date", "item_id", "y"])
    .with_columns([
        pl.col("date").str.to_datetime(strict=False),
        pl.col("y").cast(pl.Float64, strict=False),
    ])
    .drop("row").drop_nulls()
    .sort(["item_id", "date"])
    .collect()
)

# Ресемплирование по дням (среднее)
df = (
    raw_df
    .group_by_dynamic("date", every="1d", group_by="item_id")
    .agg(pl.col("y").mean().alias("y"))
    .upsample("date", every="1d", group_by="item_id")
)

# Топ рядов по покрытию
stats = (
    df.group_by("item_id")
    .agg([
        ((pl.col("date").max() - pl.col("date").min()).dt.total_days() + 1).alias("span"),
        pl.col("y").is_not_null().mean().alias("coverage")
    ])
    .filter(pl.col("span") >= 7)
    .sort(["coverage", "span"], descending=True)
    .head(10)
)

print(f"Обработано {len(raw_df):,} записей -> {len(df):,} дней")
print(f"Топ-5 рядов: {[x for x in stats['item_id'].to_list()[:5] if x]}")

# Захардкоженные общие разрывы
gap_periods = [
    ('2024-08-01', '2024-08-11'),  # 11 дней
    ('2024-10-15', '2024-10-17'),  # 3 дня  
    ('2024-10-19', '2024-10-20'),  # 2 дня
    ('2024-10-22', '2024-10-24'),  # 3 дня
    ('2025-01-30', '2025-03-19'),  # 49 дней
]
gap_periods = [(datetime.strptime(s, '%Y-%m-%d'), datetime.strptime(e, '%Y-%m-%d')) 
               for s, e in gap_periods]

# График
fig, ax = plt.subplots(figsize=(15, 8))
colors = plt.cm.tab10(np.linspace(0, 1, 10))
plot_data = df.filter(pl.col("item_id").is_in(stats["item_id"].to_list()))
selected_ids = [x for x in stats["item_id"].to_list()[:5] if x is not None]

for i, item_id in enumerate(selected_ids):
    series_data = plot_data.filter(
        (pl.col("item_id") == item_id) & (pl.col("y").is_not_null())
    ).sort("date")
    
    if len(series_data) == 0:
        continue
        
    dates = series_data["date"].to_list()
    values = series_data["y"].to_list()
    
    # Нормализация
    if len(values) > 0:
        min_val, max_val = min(values), max(values)
        if max_val > min_val:
            values = [(v - min_val) / (max_val - min_val) for v in values]
    
    # Непрерывные сегменты (≤1 день)
    segments = []
    current_dates = [dates[0]]
    current_values = [values[0]]
    
    for j in range(1, len(dates)):
        if (dates[j] - dates[j-1]).days <= 1:
            current_dates.append(dates[j])
            current_values.append(values[j])
        else:
            if len(current_dates) > 1:
                segments.append((current_dates, current_values))
            current_dates = [dates[j]]
            current_values = [values[j]]
    
    if len(current_dates) > 1:
        segments.append((current_dates, current_values))
    
    # Рисование
    for seg_dates, seg_values in segments:
        ax.plot(seg_dates, seg_values, color=colors[i], linewidth=2, alpha=0.8,
               label=f'ID: {item_id}' if seg_dates == segments[0][0] else "")
    ax.scatter(dates, values, color=colors[i], s=8, alpha=0.9, zorder=5)

# Общие разрывы
for start_gap, end_gap in gap_periods:
    ax.axvspan(start_gap, end_gap, alpha=0.1, color='red', zorder=1)

ax.set_title("Топ временные ряды (общие разрывы выделены)", fontsize=14)
ax.set_xlabel("Дата")
ax.set_ylabel("Нормализованное значение (0-1)")
ax.grid(True, alpha=0.3)
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

plt.tight_layout()
plt.show()