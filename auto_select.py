import numpy as np

def analyze_series(data):
    values = data['value'].dropna()
    if len(values) < 3:
        return 'linear'
    
    x = np.arange(len(values))
    
    # Проверка на экспоненциальный рост
    if np.all(values > 0):
        log_values = np.log(values)
        log_fit = np.polyfit(x, log_values, 1)
        log_residuals = np.sum((log_values - (log_fit[0] * x + log_fit[1])) ** 2)
        log_total = np.sum((log_values - np.mean(log_values)) ** 2)
        log_r2 = 1 - (log_residuals / log_total) if log_total != 0 else 0
        
        if log_r2 > 0.8:
            return 'log'
    
    # Проверка на нелинейность
    linear_fit = np.polyfit(x, values, 1)
    poly_fit = np.polyfit(x, values, 2)
    
    linear_pred = linear_fit[0] * x + linear_fit[1]
    poly_pred = poly_fit[0] * x**2 + poly_fit[1] * x + poly_fit[2]
    
    linear_residuals = np.sum((values - linear_pred) ** 2)
    poly_residuals = np.sum((values - poly_pred) ** 2)
    
    total_var = np.sum((values - np.mean(values)) ** 2)
    linear_r2 = 1 - (linear_residuals / total_var) if total_var != 0 else 0
    poly_r2 = 1 - (poly_residuals / total_var) if total_var != 0 else 0
    
    # Проверка изменчивости
    volatility = np.std(np.diff(values)) / np.mean(np.abs(values)) if np.mean(np.abs(values)) > 0 else 0
    
    if poly_r2 - linear_r2 > 0.15:
        return 'polynomial'
    elif volatility > 0.3:
        return 'spline'
    else:
        return 'linear'

def select_best_method(data):
    method = analyze_series(data)
    confidence = 'high' if method in ['log', 'polynomial'] else 'medium'
    
    reasons = {
        'linear': 'Стабильный линейный тренд',
        'polynomial': 'Выраженная нелинейность',
        'spline': 'Высокая изменчивость данных',
        'log': 'Экспоненциальный рост'
    }
    
    return {
        'method': method,
        'confidence': confidence,
        'reason': reasons[method]
    }