{{
    config(
        materialized='table'
    )
}}

{#
  fct_cost_forecast
  Projects warehouse-level daily cost 30 days into the future.
  Methodology: rolling average + linear trend slope + day-of-week seasonality.
  Confidence bands widen with forecast horizon (1 stddev * sqrt(days_ahead)).
#}

with actuals as (

    select
        usage_date,
        warehouse_name,
        total_cost                              as daily_cost_usd,
        dayofweek(usage_date)                   as day_of_week
    from {{ ref('fct_daily_costs') }}
    where usage_date >= dateadd('day', -{{ var('forecast_lookback_days', 60) }}, current_date())

),

-- Per-warehouse baseline statistics
baseline as (

    select
        warehouse_name,
        avg(daily_cost_usd)                     as avg_daily_cost,
        stddev(daily_cost_usd)                  as stddev_daily_cost,
        count(*)                                as observation_days,

        avg(case when usage_date >= dateadd('day', -7, current_date())
                 then daily_cost_usd end)       as last_7d_avg,
        avg(case when usage_date between dateadd('day', -14, current_date())
                                    and dateadd('day', -8, current_date())
                 then daily_cost_usd end)       as prev_7d_avg,

        -- Trend slope: change in average cost per day over the recent 2-week window
        (avg(case when usage_date >= dateadd('day', -7, current_date())
                  then daily_cost_usd end)
         - avg(case when usage_date between dateadd('day', -14, current_date())
                                     and dateadd('day', -8, current_date())
                    then daily_cost_usd end)
        ) / 7.0                                 as trend_slope_per_day

    from actuals
    group by warehouse_name
    having count(*) >= 7

),

-- Day-of-week seasonality factors per warehouse
dow_factors as (

    select
        a.warehouse_name,
        a.day_of_week,
        avg(a.daily_cost_usd)
            / nullif(b.avg_daily_cost, 0)       as dow_factor
    from actuals a
    inner join baseline b on a.warehouse_name = b.warehouse_name
    group by a.warehouse_name, a.day_of_week, b.avg_daily_cost

),

-- Generate 30-day forecast spine
forecast_spine as (

    select
        dateadd('day', row_number() over (order by seq4()), current_date()) as forecast_date,
        row_number() over (order by seq4())                                 as days_ahead
    from table(generator(rowcount => 30))

),

-- Combine baseline x spine x DOW seasonality
forecasts as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'b.warehouse_name',
            's.forecast_date',
            'current_date()'
        ]) }}                                           as forecast_id,

        current_date()                                  as forecast_run_date,
        s.forecast_date,
        s.days_ahead,
        b.warehouse_name,

        -- Point estimate: trend-adjusted average x day-of-week seasonality
        greatest(0,
            (b.avg_daily_cost + (s.days_ahead * coalesce(b.trend_slope_per_day, 0)))
            * coalesce(d.dow_factor, 1.0)
        )                                               as forecasted_cost_usd,

        -- Confidence bands (1 stddev * sqrt of horizon)
        greatest(0,
            (b.avg_daily_cost + (s.days_ahead * coalesce(b.trend_slope_per_day, 0)))
            * coalesce(d.dow_factor, 1.0)
            - coalesce(b.stddev_daily_cost, 0) * sqrt(s.days_ahead)
        )                                               as confidence_band_low,

        greatest(0,
            (b.avg_daily_cost + (s.days_ahead * coalesce(b.trend_slope_per_day, 0)))
            * coalesce(d.dow_factor, 1.0)
            + coalesce(b.stddev_daily_cost, 0) * sqrt(s.days_ahead)
        )
                                                        as confidence_band_high,

        'rolling_avg_linear_trend'                      as forecast_method,
        {{ var('forecast_lookback_days', 60) }}         as lookback_days,
        current_date()                                  as as_of_date

    from forecast_spine s
    cross join baseline b
    left join dow_factors d
        on b.warehouse_name = d.warehouse_name
        and dayofweek(s.forecast_date) = d.day_of_week

)

select * from forecasts
