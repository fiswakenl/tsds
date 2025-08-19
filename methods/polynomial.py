import pandas as pd

def interpolate(df, order=2):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df_indexed = df.set_index('date').reindex(date_range)
    df_indexed['value'] = df_indexed['value'].interpolate(method='polynomial', order=order).clip(lower=0).astype(int)
    df_indexed['id'] = df['id'].iloc[0]
    return df_indexed.reset_index().rename(columns={'index': 'date'})[['id', 'date', 'value']]