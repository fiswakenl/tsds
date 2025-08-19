
import pandas as pd
import numpy as np
from pathlib import Path
import importlib.util


class InterpolationController:
    
    def __init__(self):
        self.methods_dir = Path(__file__).parent / "methods"
        self.available_methods = self._discover_methods()
    
    def _discover_methods(self):
        methods = {}
        if self.methods_dir.exists():
            for method_file in self.methods_dir.glob("*_interpolation.py"):
                method_name = method_file.stem.replace("_interpolation", "")
                methods[method_name] = method_file
        return methods
    
    def list_methods(self):
        return list(self.available_methods.keys())
    
    def interpolate_series(self, server_id, method="linear", input_dir="data/top_series", output_dir="data/processed", save_results=False):
        
        if method not in self.available_methods:
            available = ", ".join(self.available_methods.keys())
            raise ValueError(f"Метод '{method}' недоступен. Доступные методы: {available}")
        
        input_path = Path(input_dir) / f"series_{server_id}.csv"
        if not input_path.exists():
            raise FileNotFoundError(f"Файл не найден: {input_path}")
        
        df = pd.read_csv(input_path, parse_dates=['date'])
        
        method_file = self.available_methods[method]
        spec = importlib.util.spec_from_file_location(f"{method}_interpolation", method_file)
        method_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(method_module)
        
        result_df = method_module.interpolate(df.copy())
        
        if save_results:
            output_path = Path(output_dir) / f"series_{server_id}_{method}_filled.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result_df.to_csv(output_path, index=False, date_format='%Y-%m-%d')
        
        return result_df
    
    def compare_methods(self, server_id, methods=None, input_dir="data/top_series", save_results=False):
        
        if methods is None:
            methods = self.list_methods()
        
        results = {}
        
        for method in methods:
            try:
                result = self.interpolate_series(server_id, method, input_dir, save_results=save_results)
                results[method] = result
            except Exception as e:
                results[method] = None
        
        return results


def interpolate(server_id, method="linear", save_results=False, **kwargs):
    controller = InterpolationController()
    return controller.interpolate_series(server_id, method, save_results=save_results, **kwargs)


def compare_all_methods(server_id, save_results=False, **kwargs):
    controller = InterpolationController()
    return controller.compare_methods(server_id, save_results=save_results, **kwargs)


if __name__ == "__main__":
    controller = InterpolationController()
    
