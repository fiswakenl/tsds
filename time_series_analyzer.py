#!/usr/bin/env python3
"""
Анализ временных рядов с functime поверх polars
"""

import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

def analyze_time_series(input_file='data/raw/collected.csv', top_n=10):
    """Анализ временных рядов с polars оптимизированно"""
    print("Загружаем данные...")
    
    # Загружаем и очищаем одним lazy пайплайном с валидацией
    df = (
        pl.scan_csv(input_file, has_header=False, 
                   new_columns=['row_number', 'date', 'item_id', 'property_value'])
        .filter(
            pl.col('date').str.contains(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$") &
            pl.col('property_value').is_not_null()
        )
        .with_columns([
            pl.col('date').str.to_datetime("%Y-%m-%d %H:%M:%S%z", strict=False),
            pl.col('property_value').cast(pl.Float64, strict=False)
        ])
        .drop_nulls(['date', 'property_value'])
        .drop('row_number')
        .sort('date')
        .collect()
    )
    
    print(f"Обработано записей: {len(df):,}")
    
    # Анализ временных рядов одним пайплайном
    stats = (
        df.group_by('item_id')
        .agg([
            pl.col('date').dt.date().n_unique().alias('unique_days'),
            pl.col('date').min().alias('start_date'),
            pl.col('date').max().alias('end_date'),
            pl.col('property_value').len().alias('total_records'),
            pl.col('property_value').mean().alias('avg_value'),
            pl.col('property_value').std().alias('std_value')
        ])
        .with_columns([
            ((pl.col('end_date') - pl.col('start_date')).dt.total_days() + 1).alias('span_days')
        ])
        .with_columns([
            (pl.col('unique_days') / pl.col('span_days')).alias('completeness')
        ])
        .filter(pl.col('span_days') >= 7)
        .sort(['span_days', 'completeness'], descending=True)
        .head(top_n)
    )
    
    print(f"\nТОП-{top_n} ВРЕМЕННЫХ РЯДОВ:")
    print("=" * 80)
    print(f"{'ID':<12} {'Дни':<6} {'Заполн%':<8} {'Период':<11} {'Записей':<8} {'Среднее':<12}")
    print("-" * 80)
    
    for row in stats.rows():
        item_id, unique_days, start_date, end_date, total_records, avg_value, std_value, span_days, completeness = row
        print(f"{item_id:<12} {unique_days:<6} {completeness*100:>6.1f}% "
              f"{span_days:<11} {total_records:<8} {avg_value:>11.2e}")
    
    return df, stats

def resample_series(df, stats):
    """Ресемплирование с group_by_dynamic и полным диапазоном дат"""
    print(f"\nРесемплирование временных рядов...")
    
    resampled_data = []
    all_gaps = set()
    
    for row in stats.rows():
        item_id = row[0]
        print(f"Обрабатываем ряд {item_id}...")
        
        # Получаем данные и группируем по дням (простой метод)
        series_data = df.filter(pl.col('item_id') == item_id).sort('date')
        
        # Группируем по дням и берем последнее значение
        daily_data = (
            series_data
            .with_columns(pl.col('date').dt.date().alias('day'))
            .group_by('day', maintain_order=True)
            .agg([
                pl.col('property_value').last().alias('value'),
            ])
            .sort('day')
        )
        
        # Создаем полный диапазон дат
        start_date = daily_data['day'].min()
        end_date = daily_data['day'].max()
        full_range = pl.date_range(start_date, end_date, interval='1d', eager=True)
        
        # Находим пропуски
        existing_dates = set(daily_data['day'].to_list())
        missing_dates = [d for d in full_range if d not in existing_dates]
        all_gaps.update(missing_dates)
        
        # Создаем полный DataFrame с пропусками
        resampled = (
            pl.DataFrame({'day': full_range})
            .join(daily_data, on='day', how='left')
            .with_columns([
                pl.lit(item_id).alias('item_id')
            ])
        )
        
        print(f"  Обработано: {len(resampled)} дней")
        resampled_data.append((item_id, resampled))
    
    return resampled_data, sorted(all_gaps)

def create_summary_plot(resampled_data, gap_dates):
    """Создание графика с выделением периодов пропусков"""
    print("\nСоздаем сводный график...")
    
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Рисуем области пропусков используя numpy для группировки
    if gap_dates:
        print(f"Найдено {len(gap_dates)} дней с пропусками")
        
        gap_array = np.array(gap_dates, dtype='datetime64[D]')
        # Находим разрывы между смежными датами
        breaks = np.diff(gap_array) != np.timedelta64(1, 'D')
        break_indices = np.where(breaks)[0] + 1
        
        # Разбиваем на периоды
        periods = np.split(gap_array, break_indices)
        
        for period in periods:
            if len(period) > 0:
                ax.axvspan(period[0], period[-1] + np.timedelta64(1, 'D'), 
                          color='red', alpha=0.2, zorder=0)
    
    # Отображаем временные ряды
    colors = plt.cm.tab10(range(10))
    
    for i, (item_id, data) in enumerate(resampled_data[:10]):
        # Используем polars to_pandas() для matplotlib
        dates = data['day'].to_pandas()
        values = data['value'].to_pandas()
        
        ax.plot(dates, values, 
                color=colors[i], 
                linewidth=1.5, 
                label=f'ID: {item_id}',
                alpha=0.8,
                marker='o',
                markersize=2)
    
    # Настройки графика
    ax.set_title('Топ-10 временных рядов (без заполнения пропусков)', fontsize=16, pad=20)
    ax.set_xlabel('Дата', fontsize=12)
    ax.set_ylabel('Значение', fontsize=12)
    ax.set_yscale('log')
    ax.grid(True, alpha=0.3)
    
    # Форматируем ось X
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Легенды
    legend1 = ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    
    from matplotlib.patches import Patch
    gap_patch = Patch(color='red', alpha=0.2, label='Периоды с пропусками')
    legend2 = ax.legend(handles=[gap_patch], bbox_to_anchor=(1.05, 0.15), loc='upper left', fontsize=10)
    
    # Возвращаем первую легенду чтобы показать обе
    ax.add_artist(legend1)
    
    plt.tight_layout()
    plt.show()

def main():
    """Основная функция"""
    print("=" * 80)
    print("АНАЛИЗ ВРЕМЕННЫХ РЯДОВ С НОВЫМИ ВОЗМОЖНОСТЯМИ POLARS")
    print("=" * 80)
    
    df, stats = analyze_time_series()
    resampled_data, gap_dates = resample_series(df, stats)
    create_summary_plot(resampled_data, gap_dates)
    
    print("\n" + "=" * 80)
    print("АНАЛИЗ ЗАВЕРШЕН")
    print("=" * 80)

if __name__ == "__main__":
    main()