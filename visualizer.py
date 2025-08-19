import matplotlib.pyplot as plt
import math

def plot_interpolation(original_data, interpolated_data, method, series_id):
    plt.figure(figsize=(12, 6))
    # Исходные данные - тонкая линия
    plt.plot(original_data["date"].to_list(), original_data["value"].to_list(), "-", 
             color='darkblue', alpha=1.0, linewidth=1.5, label="Исходные данные")
    # Интерполированные данные - полупрозрачная пастельная линия
    plt.plot(interpolated_data["date"].to_list(), interpolated_data["value"].to_list(), "-", 
             color='lightcoral', alpha=0.6, linewidth=4, label=f"{method} интерполяция")
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
    
    pastel_colors = ["lightcoral", "lightgreen", "lightsalmon", "plum"]
    
    for i, method in enumerate(methods[:4]):
        result = results[method]
        ax = axes[i]
        
        # Исходные данные - тонкая линия
        ax.plot(original_data["date"].to_list(), original_data["value"].to_list(), "-", 
               color='darkblue', alpha=1.0, linewidth=1.5, label="Исходные")
        # Интерполированные данные - полупрозрачная пастельная линия
        ax.plot(result["date"].to_list(), result["value"].to_list(), "-", 
               color=pastel_colors[i], alpha=0.6, linewidth=4, label=method)
        ax.set_title(method)
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    for i in range(len(methods), 4):
        axes[i].set_visible(False)
    
    plt.suptitle(f"Сравнение методов - Серия {series_id}", fontsize=14)
    plt.tight_layout()
    plt.show()

