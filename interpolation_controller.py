"""
Главный контроллер для выбора и применения методов интерполяции временных рядов.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import importlib.util


class InterpolationController:
    """Контроллер для управления различными методами интерполяции."""
    
    def __init__(self):
        self.methods_dir = Path(__file__).parent / "methods"
        self.available_methods = self._discover_methods()
    
    def _discover_methods(self):
        """Автоматически находит доступные методы интерполяции."""
        methods = {}
        if self.methods_dir.exists():
            for method_file in self.methods_dir.glob("*_interpolation.py"):
                method_name = method_file.stem.replace("_interpolation", "")
                methods[method_name] = method_file
        return methods
    
    def list_methods(self):
        """Возвращает список доступных методов интерполяции."""
        return list(self.available_methods.keys())
    
    def interpolate_series(self, server_id, method="linear", input_dir="data/top_series", output_dir="data/processed", save_results=False):
        """
        Применяет выбранный метод интерполяции к временному ряду.
        
        Args:
            server_id (str): ID сервера
            method (str): Метод интерполяции (linear, polynomial, spline, log)
            input_dir (str): Папка с исходными данными
            output_dir (str): Папка для сохранения результатов
            save_results (bool): Сохранять ли результаты в файл (по умолчанию False)
            
        Returns:
            pandas.DataFrame: Интерполированный временной ряд
        """
        
        # Проверяем доступность метода
        if method not in self.available_methods:
            available = ", ".join(self.available_methods.keys())
            raise ValueError(f"Метод '{method}' недоступен. Доступные методы: {available}")
        
        # Загружаем исходные данные
        input_path = Path(input_dir) / f"series_{server_id}.csv"
        if not input_path.exists():
            raise FileNotFoundError(f"Файл не найден: {input_path}")
        
        df = pd.read_csv(input_path, parse_dates=['date'])
        
        # Динамически загружаем и вызываем метод интерполяции
        method_file = self.available_methods[method]
        spec = importlib.util.spec_from_file_location(f"{method}_interpolation", method_file)
        method_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(method_module)
        
        # Вызываем функцию interpolate из модуля
        result_df = method_module.interpolate(df.copy())
        
        # Сохраняем результат только если save_results=True
        if save_results:
            output_path = Path(output_dir) / f"series_{server_id}_{method}_filled.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            result_df.to_csv(output_path, index=False, date_format='%Y-%m-%d')
            print(f"Результат сохранен: {output_path}")
        
        return result_df
    
    def compare_methods(self, server_id, methods=None, input_dir="data/top_series", save_results=False):
        """
        Сравнивает разные методы интерполяции для одного сервера.
        
        Args:
            server_id (str): ID сервера
            methods (list): Список методов для сравнения. Если None, использует все доступные
            input_dir (str): Папка с исходными данными
            save_results (bool): Сохранять ли результаты в файл (по умолчанию False)
            
        Returns:
            dict: Словарь с результатами интерполяции для каждого метода
        """
        
        if methods is None:
            methods = self.list_methods()
        
        results = {}
        
        for method in methods:
            try:
                result = self.interpolate_series(server_id, method, input_dir, save_results=save_results)
                results[method] = result
                print(f"[OK] Метод '{method}' применен успешно")
            except Exception as e:
                print(f"[ERROR] Ошибка в методе '{method}': {e}")
                results[method] = None
        
        return results


# Функции для удобства использования
def interpolate(server_id, method="linear", save_results=False, **kwargs):
    """Быстрая интерполяция без создания объекта контроллера."""
    controller = InterpolationController()
    return controller.interpolate_series(server_id, method, save_results=save_results, **kwargs)


def compare_all_methods(server_id, save_results=False, **kwargs):
    """Быстрое сравнение всех методов для сервера."""
    controller = InterpolationController()
    return controller.compare_methods(server_id, save_results=save_results, **kwargs)


if __name__ == "__main__":
    # Пример использования
    controller = InterpolationController()
    print("Доступные методы интерполяции:", controller.list_methods())
    
    # Пример интерполяции
    # result = controller.interpolate_series("160077920", "linear")
    # print(f"Интерполировано {len(result)} точек")