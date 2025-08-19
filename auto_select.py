import numpy as np
import polars as pl

def analyze_series(data):
    values = data.select("value").drop_nulls().to_numpy().flatten()
    if len(values) < 3:
        return 'linear'
    
    # Проверка на экспоненциальный рост (все значения > 0 и растут)
    if np.all(values > 0) and np.polyfit(np.arange(len(values)), np.log(values), 1)[0] > 0.1:
        return 'log'
    
    # Проверка изменчивости
    volatility = np.std(np.diff(values)) / np.mean(values) if np.mean(values) > 0 else 0
    if volatility > 0.3:
        return 'spline'
    
    # Проверка на нелинейность (простая)
    x = np.arange(len(values))
    linear_fit = np.polyfit(x, values, 1)
    poly_fit = np.polyfit(x, values, 2)
    linear_error = np.sum((values - np.polyval(linear_fit, x)) ** 2)
    poly_error = np.sum((values - np.polyval(poly_fit, x)) ** 2)
    
    if linear_error > 0 and (linear_error - poly_error) / linear_error > 0.2:
        return 'polynomial'
    
    return 'linear'

def select_best_method(data):
    method = analyze_series(data)
    return {'method': method, 'confidence': 'auto'}