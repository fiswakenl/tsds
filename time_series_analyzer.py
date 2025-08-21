# %%
import polars as pl
import altair as alt
from datetime import datetime, timedelta

alt.data_transformers.disable_max_rows()

# %%
# Загрузка данных
raw_df = (
    pl.scan_csv("data/raw/collected.csv", has_header=False,
                new_columns=["row", "date", "item_id", "y"])
    .with_columns([
        pl.col("date").str.to_datetime(strict=False),
        pl.col("y").cast(pl.Float64, strict=False),
    ])
    .drop("row").drop_nulls()
    .sort(["item_id", "date"])
    .collect()
)

# Ресемплирование по дням (медиана)
df = (
    raw_df
    .group_by_dynamic("date", every="1d", group_by="item_id")
    .agg(pl.col("y").median().alias("y"))
    .upsample("date", every="1d", group_by="item_id")
)

# %%
# Захардкоженные системные сбои
gap_periods = [
    ('2024-08-01', '2024-08-11'),
    ('2024-10-15', '2024-10-17'), 
    ('2024-10-19', '2024-10-20'),
    ('2024-10-22', '2024-10-24'),
    ('2025-01-30', '2025-03-19'),
]
gap_periods = [(datetime.strptime(s, '%Y-%m-%d'), datetime.strptime(e, '%Y-%m-%d')) 
               for s, e in gap_periods]

excluded_dates = set()
for start_gap, end_gap in gap_periods:
    current_date = start_gap
    while current_date <= end_gap:
        excluded_dates.add(current_date)
        current_date += timedelta(days=1)

# %%
# Отбор лучших рядов
stats = (
    df.filter(~pl.col("date").is_in(list(excluded_dates)))
    .group_by("item_id")
    .agg([
        pl.col("y").count().alias("n_points"),
        pl.col("y").std().alias("variability")
    ])
    .filter(pl.col("n_points") >= 100)
    .filter(pl.col("variability") > 0.01)
    .sort("n_points", descending=True)
    .head(100)
)

all_series_ids = stats["item_id"].to_list()

# %%
# Подготовка данных для графика
def prepare_data(series_ids, batch_size=10):
    all_data = []
    total_groups = (len(series_ids) + batch_size - 1) // batch_size
    
    for group_idx in range(total_groups):
        start_idx = group_idx * batch_size
        end_idx = min(start_idx + batch_size, len(series_ids))
        group_batch = series_ids[start_idx:end_idx]
        
        for item_id in group_batch:
            series_data = df.filter(
                (pl.col("item_id") == item_id) & (pl.col("y").is_not_null())
            ).sort("date")
            
            for row in series_data.iter_rows(named=True):
                all_data.append({
                    'date': row['date'],
                    'value': row['y'],
                    'item_id': str(item_id),
                    'group_num': group_idx + 1,
                })
    
    return pl.DataFrame(all_data)

chart_data = prepare_data(all_series_ids, batch_size=10)

# %%
# График
max_group = chart_data['group_num'].max()
group_param = alt.param(value=1, bind=alt.binding_range(min=1, max=max_group, step=1, name='Группа: '))
legend_selection = alt.selection_point(fields=['item_id'])

# Фиксированные домены для осей
date_domain = [chart_data['date'].min().replace(tzinfo=None), chart_data['date'].max().replace(tzinfo=None)]
value_domain = [chart_data['value'].min(), chart_data['value'].max()]

base_chart = alt.Chart(chart_data.to_pandas()).add_params(
    group_param, legend_selection
).transform_filter(
    alt.datum.group_num == group_param
)

lines = base_chart.mark_line(point=True, strokeWidth=2).encode(
    x=alt.X('date:T', title='Дата', scale=alt.Scale(domain=date_domain)),
    y=alt.Y('value:Q', title='Значение', scale=alt.Scale(domain=value_domain)),
    color=alt.Color('item_id:N', title='ID ряда'),
    opacity=alt.condition(legend_selection, alt.value(1.0), alt.value(0.2)),
    tooltip=['item_id:N', 'date:T', 'value:Q']
)

final_chart = lines.properties(
    width=1200, height=600,
    title="Временные ряды (клик по легенде чтобы скрыть/показать)"
).resolve_scale(
    x='shared', y='independent'
)

final_chart

# %%