import polars as pl
import numpy as np

def interpolate(df):
    df = df.with_columns(pl.col("date").dt.date())
    date_range = pl.date_range(df["date"].min(), df["date"].max(), interval="1d", eager=True).to_series()
    
    full_df = pl.DataFrame({"date": date_range})
    merged = full_df.join(df, on="date", how="left")
    
    values = merged["value"].to_numpy()
    mask = ~np.isnan(values)
    if mask.sum() > 1:
        values[~mask] = np.interp(np.where(~mask)[0], np.where(mask)[0], values[mask])
    
    return merged.with_columns([
        pl.lit(values.astype(int)).alias("value"),
        pl.lit(df["id"][0]).alias("id")
    ]).select(["id", "date", "value"])