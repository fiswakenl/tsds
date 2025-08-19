import matplotlib.pyplot as plt

def plot_interpolation(original_data, interpolated_data, method, series_id):
    plt.figure(figsize=(12, 6))
    
    plt.plot(interpolated_data["date"], interpolated_data["value"], "b-", 
             alpha=0.8, linewidth=2, label=f"{method} интерполяция")
    
    plt.plot(original_data["date"], original_data["value"], "ro", 
             alpha=0.8, markersize=5, label="Исходные данные", zorder=10)
    
    plt.title(f"Серия {series_id} - {method} интерполяция")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def compare_methods(original_data, results, series_id):
    methods = list(results.keys())
    num_methods = len(methods)
    
    if num_methods <= 2:
        rows, cols = 1, num_methods
    elif num_methods <= 4:
        rows, cols = 2, 2
    else:
        rows, cols = 2, 3
    
    fig, axes = plt.subplots(rows, cols, figsize=(15, 10))
    if num_methods == 1:
        axes = [axes]
    elif rows == 1 or cols == 1:
        axes = axes.flatten()
    else:
        axes = axes.flatten()
    
    colors = ["blue", "green", "orange", "purple"]
    
    for i, (method, result) in enumerate(results.items()):
        if result is None:
            continue
            
        ax = axes[i]
        color = colors[i % len(colors)]
        
        ax.plot(result["date"], result["value"], f"{color[0]}-", 
               alpha=0.8, linewidth=2, label=f"{method}")
        
        ax.plot(original_data["date"], original_data["value"], "ro", 
               alpha=0.8, markersize=4, label="Исходные", zorder=10)
        
        ax.set_title(f"{method}")
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis="x", rotation=45)
    
    for i in range(num_methods, len(axes)):
        axes[i].set_visible(False)
    
    plt.suptitle(f"Сравнение методов - Серия {series_id}", fontsize=14)
    plt.tight_layout()
    plt.show()