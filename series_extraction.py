import os
import polars as pl

input_file = r'data/raw/collected.csv'
output_dir = 'data/top_series'
select_top_n = 10

# создаём папку для результатов
os.makedirs(output_dir, exist_ok=True)

print("Анализ временных рядов с Polars (супер быстро!)")

# чтение CSV с Polars (мгновенно!)
try:
    df = pl.read_csv(
        input_file,
        has_header=False,
        new_columns=['row_number', 'date', 'id', 'value']
    )
    print(f"Загружено {len(df):,} записей из {input_file}")
except FileNotFoundError:
    raise SystemExit(f"Файл не найден: {input_file}")

# фильтрация и очистка данных
print("Фильтрация данных...")
pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$"

df_clean = (
    df.filter(
        pl.col("date").str.contains(pattern) & 
        pl.col("value").is_not_null()
    )
    .with_columns([
        pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%z"),
        pl.col("value").cast(pl.Float64, strict=False)
    ])
    .drop_nulls(["date", "value"])
    .drop("row_number")
)

print(f"После очистки: {len(df_clean):,} записей")

# расчёт статистики временных рядов (super fast!)
print("Расчёт статистики...")
stats = (
    df_clean
    .with_columns(pl.col("date").dt.date().alias("day"))
    .group_by("id")
    .agg([
        pl.col("day").n_unique().alias("unique_days"),
        pl.col("date").min().alias("min_date"),
        pl.col("date").max().alias("max_date")
    ])
    .with_columns([
        ((pl.col("max_date") - pl.col("min_date")).dt.total_days().cast(pl.Int32) + 1).alias("span_days")
    ])
    .with_columns(
        (pl.col("unique_days") / pl.col("span_days")).alias("completeness")
    )
    .sort(["unique_days", "completeness"], descending=True)
)

# топ ряды
top_stats = stats.head(select_top_n)
top_ids = top_stats["id"].to_list()

print(f"\nТоп-{select_top_n} серий по количеству дней и полноте заполнения:")
print(top_stats.to_pandas())  # для красивого вывода

# сохранение в CSV (совместимость с остальными скриптами)
print("\nСохранение временных рядов...")
for series_id in top_ids:
    # фильтруем по ID и сортируем
    series_data = (
        df_clean
        .filter(pl.col("id") == series_id)
        .sort("date")
        .with_columns(pl.col("date").dt.date().alias("day"))
        .group_by("day", maintain_order=True)  # последняя запись дня
        .tail(1)
        .drop("day")
        .select(["id", "date", "value"])
    )
    
    # конвертируем в pandas для сохранения (совместимость)
    series_pandas = series_data.to_pandas()
    
    out_path = os.path.join(output_dir, f'series_{series_id}.csv')
    series_pandas.to_csv(out_path, index=False)
    
    # статистика
    row = top_stats.filter(pl.col("id") == series_id).row(0)
    ud, comp, sp = row[1], row[4], row[3]  # unique_days, completeness, span_days
    print(f"Сохранён ряд {series_id}: days={ud}, span_days={sp}, completeness={comp:.2f}, points={len(series_pandas)}")

print('\nОбработка завершена с Polars!')