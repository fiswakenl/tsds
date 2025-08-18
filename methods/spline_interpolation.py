"""
Сплайн-интерполяция для временных рядов нагрузки серверов.
Подходит для сложных паттернов с множественными изгибами и плавными переходами.
"""

import pandas as pd
import numpy as np


def interpolate(df, order=3):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    server_id = df['id'].iloc[0]
    
    if len(df) < order + 1:
        from .linear_interpolation import interpolate as linear_interpolate
        return linear_interpolate(df)
    
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
    df_indexed = df.set_index('date').reindex(date_range)
    
    df_indexed['value'] = df_indexed['value'].interpolate(method='spline', order=order).clip(lower=0).astype(int)
    df_indexed['id'] = server_id
    result = df_indexed.reset_index().rename(columns={'index': 'date'})[['id', 'date', 'value']]
    
    return result


