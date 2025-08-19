import polars as pl
import numpy as np
from scipy import interpolate as scipy_interp

def interpolate(df, order=3):
    df = df.with_columns(pl.col("date").dt.date())
    date_range = pl.date_range(df["date"].min(), df["date"].max(), interval="1d", eager=True)
    
    full_df = pl.DataFrame({"date": date_range})
    merged = full_df.join(df, on="date", how="left")
    
    values = merged["value"].to_numpy()
    mask = ~np.isnan(values)
    
    if mask.sum() < order + 1:
        # Fallback to linear interpolation
        if mask.sum() > 1:
            values[~mask] = np.interp(np.where(~mask)[0], np.where(mask)[0], values[mask])
    else:
        x_known = np.where(mask)[0]
        y_known = values[mask]
        spline = scipy_interp.UnivariateSpline(x_known, y_known, s=0, k=min(order, len(x_known)-1))
        values[~mask] = spline(np.where(~mask)[0])
        values = np.clip(values, 0, None)
    
    return merged.with_columns([
        pl.lit(values.astype(int)).alias("value"),
        pl.lit(df["id"][0]).alias("id")
    ]).select(["id", "date", "value"])