
import polars as pl


def analyze_data(input_file='data/raw/collected.csv', select_top_n=10):
    

    try:
        df = pl.read_csv(
            input_file,
            has_header=False,
            new_columns=['row_number', 'date', 'id', 'value']
        )
    except FileNotFoundError:
        raise SystemExit(f"Файл не найден: {input_file}")

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

    top_stats = stats.head(select_top_n)
    top_ids = top_stats["id"].to_list()


    
    return df_clean, top_stats, top_ids


def get_series_data(df_clean, series_id):
    return (
        df_clean
        .filter(pl.col("id") == str(series_id))
        .sort("date")
        .with_columns(pl.col("date").dt.date().alias("day"))
        .group_by("day", maintain_order=True)
        .tail(1)
        .drop("day")
        .select(["id", "date", "value"])
        .to_pandas()
    )


if __name__ == "__main__":
    df_clean, top_stats, top_ids = analyze_data()
    for i, series_id in enumerate(top_ids, 1):
