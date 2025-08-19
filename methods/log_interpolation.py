
import pandas as pd
import numpy as np


def interpolate(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    server_id = df['id'].iloc[0]
    
    if (df['value'] <= 0).any():
        df['value'] = df['value'] + 1
        shift_applied = True
    else:
        shift_applied = False
    
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df_indexed = df.set_index('date').reindex(date_range)
    
    log_values = np.log(df_indexed['value'])
    log_interpolated = log_values.interpolate(method='linear')
    df_indexed['value'] = np.exp(log_interpolated)
    
    if shift_applied:
        df_indexed['value'] = (df_indexed['value'] - 1).clip(lower=0)
    
    df_indexed['value'] = df_indexed['value'].astype(int)
    df_indexed['id'] = server_id
    result = df_indexed.reset_index().rename(columns={'index': 'date'})[['id', 'date', 'value']]
    
    return result


