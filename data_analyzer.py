"""
Анализатор временных рядов с использованием Polars.
Быстрая обработка больших объемов данных и выделение топовых серий.
"""

import polars as pl


def analyze_data(input_file='data/raw/collected.csv', select_top_n=10):
    """
    Анализирует временные ряды и возвращает топовые серии.
    
    Args:
        input_file (str): Путь к исходному CSV файлу
        select_top_n (int): Количество топовых серий для выбора
        
    Returns:
        tuple: (df_clean, top_stats, top_ids)
    """
    
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

    print('\nАнализ завершен с Polars!')
    
    return df_clean, top_stats, top_ids


def get_series_data(df_clean, series_id):
    """
    Извлекает данные конкретной серии из очищенного датасета.
    
    Args:
        df_clean: Очищенный Polars DataFrame
        series_id: ID серии (строка или число)
        
    Returns:
        pandas.DataFrame: Данные серии
    """
    return (
        df_clean
        .filter(pl.col("id") == str(series_id))
        .sort("date")
        .with_columns(pl.col("date").dt.date().alias("day"))
        .group_by("day", maintain_order=True)  # последняя запись дня
        .tail(1)
        .drop("day")
        .select(["id", "date", "value"])
        .to_pandas()
    )


if __name__ == "__main__":
    # Демонстрация использования
    df_clean, top_stats, top_ids = analyze_data()
    print(f"\nНайдено {len(top_ids)} топовых серий:")
    for i, series_id in enumerate(top_ids, 1):
        print(f"{i}. Серия {series_id}")