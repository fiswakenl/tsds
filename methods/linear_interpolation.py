
import pandas as pd
import numpy as np

def interpolate(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    server_id = df['id'].iloc[0]
    
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df_indexed = df.set_index('date').reindex(date_range)
    
    df_indexed['value'] = df_indexed['value'].interpolate(method='linear')
    
    df_indexed['value'] = df_indexed['value'].astype(int)
    df_indexed['id'] = server_id
    result = df_indexed.reset_index().rename(columns={'index': 'date'})[['id', 'date', 'value']]
    
    return result