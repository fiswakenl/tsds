import polars as pl
import numpy as np

def interpolate(df):
    df = df.with_columns(pl.col("date").dt.date())
    date_range = pl.date_range(df["date"].min(), df["date"].max(), interval="1d", eager=True).to_series()
    
    full_df = pl.DataFrame({"date": date_range})
    merged = full_df.join(df, on="date", how="left")
    
    values = merged["value"].to_numpy()
    mask = ~np.isnan(values)
    
    # Ensure positive values for log
    if np.any(values[mask] <= 0):
        values[mask] = values[mask] + 1
        shift_applied = True
    else:
        shift_applied = False
    
    if mask.sum() > 1:
        log_values = np.log(values[mask])
        x_known = np.where(mask)[0]
        log_interp = np.interp(np.where(~mask)[0], x_known, log_values)
        values[~mask] = np.exp(log_interp)
    
    if shift_applied:
        values = np.clip(values - 1, 0, None)
    
    return merged.with_columns([
        pl.lit(values.astype(int)).alias("value"),
        pl.lit(df["id"][0]).alias("id")
    ]).select(["id", "date", "value"])