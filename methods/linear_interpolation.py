"""Линейная интерполяция для временных рядов."""

from .base_interpolation import prepare_data, finalize_result

def interpolate(df):
    """Применяет линейную интерполяцию к временному ряду."""
    df_indexed, server_id = prepare_data(df)
    
    # Линейная интерполяция
    df_indexed['value'] = df_indexed['value'].interpolate(method='linear')
    
    return finalize_result(df_indexed, server_id, len(df), "Линейная интерполяция")