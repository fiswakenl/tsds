import matplotlib.pyplot as plt
import math

def plot_interpolation(original_data, interpolated_data, method, series_id):
    plt.figure(figsize=(12, 6))
    # Интерполированные данные - полупрозрачно
    plt.plot(interpolated_data["date"].to_list(), interpolated_data["value"].to_list(), "b-", 
             alpha=0.6, linewidth=2, label=f"{method} интерполяция")
    # Исходные данные - тонкая линия + маленькие точки поверх всего
    plt.plot(original_data["date"].to_list(), original_data["value"].to_list(), "-", 
             color='black', alpha=0.8, linewidth=1, label="Исходные данные", zorder=10)
    plt.plot(original_data["date"].to_list(), original_data["value"].to_list(), "o", 
             color='red', alpha=1.0, markersize=3, zorder=11)
    plt.title(f"Серия {series_id} - {method}")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def compare_methods(original_data, results, series_id):
    methods = [k for k, v in results.items() if v is not None]
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    axes = axes.flatten()
    
    colors = ["blue", "green", "orange", "purple"]
    
    for i, method in enumerate(methods[:4]):
        result = results[method]
        ax = axes[i]
        
        # Интерполированные данные - полупрозрачно
        ax.plot(result["date"].to_list(), result["value"].to_list(), f"{colors[i][0]}-", 
               alpha=0.6, linewidth=2, label=method)
        # Исходные данные - тонкая линия + маленькие точки поверх всего  
        ax.plot(original_data["date"].to_list(), original_data["value"].to_list(), "-", 
               color='black', alpha=0.8, linewidth=1, zorder=10)
        ax.plot(original_data["date"].to_list(), original_data["value"].to_list(), "o", 
               color='red', alpha=1.0, markersize=3, label="Исходные", zorder=11)
        ax.set_title(method)
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    for i in range(len(methods), 4):
        axes[i].set_visible(False)
    
    plt.suptitle(f"Сравнение методов - Серия {series_id}", fontsize=14)
    plt.tight_layout()
    plt.show()

