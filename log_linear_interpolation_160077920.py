import numpy as np
import pandas as pd

# пути к файлам
input_path = r'series_160077920.csv'
output_path = r'series_160077920_filled.csv'

# чтение с парсингом даты
df = pd.read_csv(input_path, parse_dates=['date'])

# обрезаем время, оставляем только дату
df['date'] = df['date'].dt.normalize()

# сохраняем id, он должен оставаться одинаковым
id_value = df['id'].iloc[0]

# создаём полный диапазон дат между минимальной и максимальной датой
full_idx = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')

# индексируем исходный dataframe по дате и создаём новый с полным диапазоном дат
df = df.set_index('date').reindex(full_idx)

# лог-линейная интерполяция для пропущенных значений в value
# предполагаем, что все значения value > 0
log_vals = np.log(df['value'])
log_interp = log_vals.interpolate(method='linear')
df['value'] = np.exp(log_interp)

# преобразуем значения 'value' в тип int
df['value'] = df['value'].astype(int)

# восстанавливаем столбец 'id', так как он не изменяется
df['id'] = id_value

# сброс индекса и преобразование даты обратно в столбец
df = df.reset_index().rename(columns={'index': 'date'})

# сохраняем результат в новый csv
df.to_csv(output_path, index=False, date_format='%Y-%m-%d')
print(f"Новый файл сохранён по пути: {output_path}")
