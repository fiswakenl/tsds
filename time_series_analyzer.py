#!/usr/bin/env python3
"""
Быстрый анализ временных рядов с поиском самых длинных и заполненных рядов
и их ресемплированием. Использует только polars для максимальной производительности.
"""

import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import os

def analyze_time_series(input_file='data/raw/collected.csv', top_n=10):
    """
    Анализ временных рядов: поиск самых длинных и заполненных рядов
    """
    print("Загружаем данные...")
    
    # Загружаем данные
    df = pl.read_csv(
        input_file,
        has_header=False,
        new_columns=['row_number', 'date', 'item_id', 'property_value']
    )
    
    print(f"Загружено записей: {len(df):,}")
    
    # Очищаем данные - фильтруем валидные даты и значения
    pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$"
    df_clean = (
        df.filter(
            pl.col("date").str.contains(pattern) & 
            pl.col("property_value").is_not_null()
        )
        .with_columns([
            pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%z"),
            pl.col("property_value").cast(pl.Float64, strict=False),
            pl.col("item_id").cast(pl.Utf8)
        ])
        .drop_nulls(["date", "property_value"])
        .drop("row_number")
    )
    
    print(f"После очистки записей: {len(df_clean):,}")
    
    # Анализируем временные ряды
    print("Анализируем временные ряды...")
    
    stats = (
        df_clean
        .with_columns(pl.col("date").dt.date().alias("day"))
        .group_by("item_id")
        .agg([
            pl.col("day").n_unique().alias("unique_days"),
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
            pl.col("property_value").count().alias("total_records"),
            pl.col("property_value").mean().alias("avg_value"),
            pl.col("property_value").std().alias("std_value")
        ])
        .with_columns([
            ((pl.col("end_date") - pl.col("start_date")).dt.total_days().cast(pl.Int32) + 1).alias("span_days")
        ])
        .with_columns([
            (pl.col("unique_days") / pl.col("span_days")).alias("completeness"),
            pl.col("span_days").alias("duration_days")
        ])
        .filter(pl.col("span_days") >= 7)  # Минимум неделя данных
        .sort(["duration_days", "completeness"], descending=True)
        .head(top_n)
    )
    
    print(f"\nТОП-{top_n} ВРЕМЕННЫХ РЯДОВ:")
    print("=" * 80)
    print(f"{'ID':<12} {'Дни':<6} {'Заполн%':<8} {'Период':<11} {'Записей':<8} {'Среднее':<12}")
    print("-" * 80)
    
    for row in stats.iter_rows():
        item_id, unique_days, start_date, end_date, total_records, avg_value, std_value, span_days, completeness, duration_days = row
        print(f"{item_id:<12} {unique_days:<6} {completeness*100:>6.1f}% "
              f"{span_days:<11} {total_records:<8} {avg_value:>11.2e}")
    
    return df_clean, stats

def resample_series(df_clean, series_stats):
    """
    Ресемплирование временных рядов к дневной периодичности
    """
    print(f"\nРесемплирование временных рядов...")
    
    resampled_data = []
    all_gaps_mask = None  # Маска для отслеживания пропусков
    
    for row in series_stats.iter_rows():
        item_id = row[0]
        print(f"Обрабатываем ряд {item_id}...")
        
        # Получаем данные для конкретного ряда
        series_data = (
            df_clean
            .filter(pl.col("item_id") == item_id)
            .sort("date")
        )
        
        # Ресемплируем к дневной периодичности
        # Группируем по дням и берем последнее значение дня
        daily_data = (
            series_data
            .with_columns(pl.col("date").dt.date().alias("day"))
            .group_by("day", maintain_order=True)
            .agg([
                pl.col("date").last().alias("date"),
                pl.col("property_value").last().alias("value"),  # Последнее значение дня
                pl.col("item_id").first().alias("item_id")
            ])
            .sort("day")
        )
        
        # Создаем полный диапазон дат но НЕ заполняем пропуски
        start_date = daily_data["day"].min()
        end_date = daily_data["day"].max()
        
        # Создаем полный диапазон дат
        date_range = pl.date_range(
            start_date, 
            end_date, 
            interval="1d",
            eager=True
        ).alias("day")
        
        full_range = pl.DataFrame({"day": date_range})
        
        # Объединяем с данными НО НЕ заполняем пропуски - оставляем как null
        resampled = (
            full_range
            .join(daily_data, on="day", how="left")
            .with_columns([
                pl.col("item_id").fill_null(pl.lit(item_id))
            ])
            # НЕ заполняем null в value - пропуски останутся как null
        )
        
        # Создаем маску пропусков для текущего ряда
        gaps_mask = resampled["value"].is_null()
        
        # Объединяем с общей маской пропусков
        if all_gaps_mask is None:
            all_gaps_mask = resampled.select(["day"]).with_columns(
                gaps_mask.alias("has_gap")
            )
        else:
            # Добавляем пропуски текущего ряда к общей маске
            current_gaps = resampled.select(["day"]).with_columns(
                gaps_mask.alias("current_gap")
            )
            all_gaps_mask = (
                all_gaps_mask
                .join(current_gaps, on="day", how="full", suffix="_new")
                .with_columns([
                    (pl.col("has_gap").fill_null(False) | pl.col("current_gap").fill_null(False)).alias("has_gap")
                ])
                .select(["day", "has_gap"])  # Оставляем только нужные колонки
            )
        
        print(f"  Обработано: {len(resampled)} дней")
        
        resampled_data.append((item_id, resampled))
    
    return resampled_data, all_gaps_mask

def create_summary_plot(resampled_data, gaps_mask):
    """
    Создание единого графика всех топ-10 временных рядов с разными цветами
    и выделением периодов с пропусками
    """
    print("\nСоздаем сводный график...")
    
    # Создаем один большой график
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Сначала рисуем области с пропусками
    if gaps_mask is not None:
        gap_dates = gaps_mask.filter(pl.col("has_gap")).select("day").to_pandas()["day"]
        
        # Группируем смежные даты в периоды для более эффективного отображения
        if len(gap_dates) > 0:
            print(f"Найдено {len(gap_dates)} дней с пропусками")
            
            # Простая группировка смежных дат
            from datetime import timedelta
            for gap_date in gap_dates:
                # Рисуем тонкую вертикальную полосу для каждого дня с пропуском
                ax.axvspan(gap_date, gap_date + timedelta(days=1), 
                          color='red', alpha=0.2, zorder=0)
    
    # Цветовая палитра для 10 рядов
    colors = plt.cm.tab10(range(10))
    
    # Отображаем каждый временной ряд
    for i, (item_id, data) in enumerate(resampled_data[:10]):
        # Конвертируем данные для matplotlib
        dates = data["day"].to_pandas()
        values = data["value"].to_pandas()
        
        # Рисуем линию с уникальным цветом
        # matplotlib автоматически разрывает линию в местах NaN/null значений
        ax.plot(dates, values, 
                color=colors[i], 
                linewidth=1.5, 
                label=f'ID: {item_id}',
                alpha=0.8,
                marker='o',  # Добавляем точки чтобы лучше видеть данные
                markersize=2)
    
    # Настройки графика
    ax.set_title('Топ-10 временных рядов (без заполнения пропусков)', fontsize=16, pad=20)
    ax.set_xlabel('Дата', fontsize=12)
    ax.set_ylabel('Значение', fontsize=12)
    ax.grid(True, alpha=0.3)
    
    # Используем логарифмическую шкалу для Y из-за больших различий в значениях
    ax.set_yscale('log')
    
    # Форматируем ось X
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Добавляем легенду
    # Сначала легенда для временных рядов
    legend1 = ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    
    # Добавляем элемент для обозначения пропусков
    from matplotlib.patches import Patch
    gap_patch = Patch(color='red', alpha=0.2, label='Периоды с пропусками')
    legend2 = ax.legend(handles=[gap_patch], bbox_to_anchor=(1.05, 0.15), loc='upper left', fontsize=10)
    
    # Возвращаем первую легенду чтобы показать обе
    ax.add_artist(legend1)
    
    # Улучшаем компоновку
    plt.tight_layout()
    plt.show()

def main():
    """
    Основная функция
    """
    print("=" * 80)
    print("АНАЛИЗ ВРЕМЕННЫХ РЯДОВ С РЕСЕМПЛИРОВАНИЕМ")
    print("=" * 80)
    
    # Анализируем временные ряды
    df_clean, stats = analyze_time_series()
    
    # Ресемплируем топ-10 рядов и получаем маску пропусков
    resampled_data, gaps_mask = resample_series(df_clean, stats)
    
    # Создаем визуализацию с выделением пропусков
    create_summary_plot(resampled_data, gaps_mask)
    
    print("\n" + "=" * 80)
    print("АНАЛИЗ ЗАВЕРШЕН")
    print("=" * 80)

if __name__ == "__main__":
    main()