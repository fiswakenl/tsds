import pandas as pd

# file paths
input_path = r'data/top_series/series_159782958.csv'
output_path = r'data/processed/series_159782958_filled.csv'

# чтение с парсингом даты
df = pd.read_csv(input_path, parse_dates=['date'])

# обрезаем время
df['date'] = df['date'].dt.normalize()

# сохраняем id, он должен оставаться одинаковым
id_value = df['id'].iloc[0]

# сздаём полный диапазон дат между минимальной и максимальной датой
full_idx = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')

# индексируем исходный вataаrame по дате и создаём новый с полным диапазоном дат
df = df.set_index('date').reindex(full_idx)

# линейная интерполяция для пропущенных значений в столбце value
df['value'] = df['value'].interpolate(method='linear')

# преобразуем значения value в int
df['value'] = df['value'].astype(int)

# восстанавливаем столбец id и переносим его в первую позицию
df['id'] = id_value

# сброс индекса и преобразование даты обратно в столбец
df = df.reset_index().rename(columns={'index': 'date'})

# сохраняем результат
df.to_csv(output_path, index=False, date_format='%Y-%m-%d')

print(f"Новый файл сохранён по пути: {output_path}")
