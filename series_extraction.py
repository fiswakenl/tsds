import os

import pandas as pd

input_file = r'D:\collected.csv'  # путь к csv
output_dir = 'filtered_series'  # папка для сохранения результатов
select_top_n = 10  # сколько рядов выбрать и сохранить

# создаём папку для результатов, если её нет
os.makedirs(output_dir, exist_ok=True)

# чтение и предобработка
try:
    df = pd.read_csv(
        input_file,
        header=None,
        names=['row_number', 'date', 'id', 'value'],
        dtype=str,
        parse_dates=False,
        low_memory=False
    )
except FileNotFoundError:
    raise SystemExit(f"Файл не найден: {input_file}")

# фильтрация корректных строк
pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$"
mask_valid = df['date'].str.match(pattern) & df['value'].notna()
df = df[mask_valid].copy()

# преобразование типов
df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce')
df['value'] = pd.to_numeric(df['value'], errors='coerce')

# удаление некорректных строк
df = df.dropna(subset=['date', 'value'])

# удаляем столбец номера строки
df = df.drop(columns=['row_number'])

# расчёт показателей полноты и размера серий
stats = (
    df.assign(day=df['date'].dt.date)
    .groupby('id')
    .agg(unique_days=('day', 'nunique'), min_date=('date', 'min'), max_date=('date', 'max'))
)
stats['span_days'] = (stats['max_date'] - stats['min_date']).dt.days + 1
stats['completeness'] = stats['unique_days'] / stats['span_days']

# сортировка: сначала по уникальным дням, затем по полноте
stats = stats.sort_values(['unique_days', 'completeness'], ascending=[False, False])

# выбор топ-n сериалов
top_ids = stats.head(select_top_n).index.tolist()
print(f"Топ-{select_top_n} серий по количеству дней и полноте заполнения:")
print(stats.loc[top_ids])

# сохранение результатов
for series_id in top_ids:
    group = df[df['id'] == series_id].sort_values('date')

    # оставляем последнюю запись каждого дня
    group = group.groupby(group['date'].dt.date, as_index=False).tail(1)
    out_path = os.path.join(output_dir, f'series_{series_id}.csv')
    group[['id', 'date', 'value']].to_csv(out_path, index=False)
    ud = stats.at[series_id, 'unique_days']
    sp = stats.at[series_id, 'span_days']
    comp = stats.at[series_id, 'completeness']
    print(f"Сохранён ряд {series_id}: days={ud}, span_days={sp}, \
    completeness={comp:.2f}, points_saved={len(group)}")
print('Обработка завершена')
