from methods import linear, polynomial, spline, log
from data_utils import get_top_series, get_series_data
from auto_select import select_best_method
from visualizer import plot_interpolation, compare_methods

# Загружаем данные один раз
df_clean, top_ids = get_top_series()

METHODS = {
    'linear': linear.interpolate,
    'polynomial': polynomial.interpolate, 
    'spline': spline.interpolate,
    'log': log.interpolate
}

def interpolate_series(series_id, method='auto'):
    original_data = get_series_data(df_clean, series_id)
    
    if method == 'auto':
        selection = select_best_method(original_data)
        method = selection['method']
        print(f"Выбран метод: {method} ({selection['confidence']})")
    
    interpolated_data = METHODS[method](original_data)
    return original_data, interpolated_data, method

def plot_series(series_id, method='auto'):
    original_data, interpolated_data, used_method = interpolate_series(series_id, method)
    plot_interpolation(original_data, interpolated_data, used_method, series_id)

def compare_all_methods(series_id):
    original_data = get_series_data(df_clean, series_id)
    results = {}
    
    for method_name, method_func in METHODS.items():
        try:
            results[method_name] = method_func(original_data)
        except:
            results[method_name] = None
    
    compare_methods(original_data, results, series_id)
    return results