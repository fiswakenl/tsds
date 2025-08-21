#!/usr/bin/env python3
import polars as pl

# 1) Загрузка исходных данных
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

print(f"Исходных записей: {len(raw_df):,}")

# 2) Ресемплирование по дням (D) - агрегация внутридневных данных в дневные
df = (
    raw_df
    .group_by_dynamic("date", every="1d", group_by="item_id")
    .agg([
        pl.col("y").mean().alias("y"),         # Среднее за день - более стабильно
        pl.col("y").last().alias("y_last"),    # Последнее значение за день  
        pl.col("y").count().alias("points_per_day")  # Количество точек за день
    ])
    .upsample("date", every="1d", group_by="item_id")
    # Пропуски остаются как null - БЕЗ интерполяции
)

print(f"После ресемплирования: {len(df):,} дней")

# 3) Топ рядов по покрытию
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

print("Top series:", stats.shape)
print(stats["item_id"].to_list())

# 4) График через matplotlib - отображается в окне
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

# Фильтруем данные топ-рядов
plot_data = df.filter(pl.col("item_id").is_in(stats["item_id"].to_list()))

colors = plt.cm.tab10(np.linspace(0, 1, 10))

# Один график для всех рядов
fig, ax = plt.subplots(figsize=(15, 8))

for i, item_id in enumerate(stats["item_id"].to_list()[:5]):
    if item_id is None:
        continue
    
    series_data = plot_data.filter(
        (pl.col("item_id") == item_id) & (pl.col("y").is_not_null())
    ).sort("date")
    
    if len(series_data) == 0:
        continue
        
    dates = series_data["date"].to_list()
    values = series_data["y"].to_list()
    
    # Нормализуем ряд к диапазону 0-1 для лучшего отображения
    if len(values) > 0:
        min_val = min(values)
        max_val = max(values)
        if max_val > min_val:
            values = [(v - min_val) / (max_val - min_val) for v in values]
    
    # Находим непрерывные участки (разрыв <= 1 день)
    segments = []
    current_segment_dates = [dates[0]]
    current_segment_values = [values[0]]
    
    for j in range(1, len(dates)):
        gap_days = (dates[j] - dates[j-1]).days
        if gap_days <= 1:
            current_segment_dates.append(dates[j])
            current_segment_values.append(values[j])
        else:
            # Сохраняем текущий сегмент
            if len(current_segment_dates) > 1:
                segments.append((current_segment_dates, current_segment_values))
            # Красная зона для разрыва (более прозрачная)
            ax.axvspan(dates[j-1], dates[j], alpha=0.05, color='red')
            # Начинаем новый сегмент
            current_segment_dates = [dates[j]]
            current_segment_values = [values[j]]
    
    # Добавляем последний сегмент
    if len(current_segment_dates) > 1:
        segments.append((current_segment_dates, current_segment_values))
    
    # Рисуем линии для каждого непрерывного сегмента
    for seg_dates, seg_values in segments:
        ax.plot(seg_dates, seg_values, color=colors[i], linewidth=2, alpha=0.8,
               label=f'ID: {item_id}' if seg_dates == segments[0][0] else "")
    
    # Рисуем точки (меньший размер)
    ax.scatter(dates, values, color=colors[i], s=8, alpha=0.9, zorder=5)

ax.set_title("Топ временные ряды (среднее по дням, нормализованные)", fontsize=14)
ax.set_xlabel("Дата")
ax.set_ylabel("Нормализованное значение (0-1)")
ax.grid(True, alpha=0.3)
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

plt.tight_layout()
plt.show()
print("График отображен в окне!")