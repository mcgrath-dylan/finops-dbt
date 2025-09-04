{{
    config(
        materialized='table'
    )
}}

with compute_costs as (
    select
        usage_date,
        warehouse_name,

        -- Rollups from authoritative hourly costs
        sum(total_credits_used)         as compute_credits,
        sum(compute_cost_usd)           as compute_cost,
        sum(cloud_services_cost_usd)    as cloud_services_cost,
        sum(total_cost_usd)             as total_cost,

        -- Existing signals
        sum(idle_cost_usd)              as idle_cost,
        sum(queries_executed)           as total_queries,
        avg(unique_users)               as avg_concurrent_users
    from {{ ref('int_hourly_compute_costs') }}
    group by 1, 2
),

daily_summary as (
    select
        -- Contract-friendly explicit types
        usage_date::date                                   as usage_date,
        warehouse_name::varchar                            as warehouse_name,

        compute_credits::number(38,3)                      as compute_credits,
        compute_cost::number(38,2)                         as compute_cost,
        cloud_services_cost::number(38,2)                  as cloud_services_cost,
        total_cost::number(38,2)                           as total_cost,

        idle_cost::number(38,2)                            as idle_cost,
        (compute_cost - idle_cost)::number(38,2)           as productive_cost,

        total_queries::number(38,0)                        as total_queries,
        avg_concurrent_users::number(38,2)                 as avg_concurrent_users,

        case when total_queries > 0
             then (compute_cost / nullif(total_queries,0))::number(38,4)
        end                                                as cost_per_query,

        case when compute_cost > 0
             then (100 * (1 - (idle_cost / nullif(compute_cost,0))))::number(5,2)
             else 100::number(5,2)
        end                                                as efficiency_score,

        -- Use TOTAL for running/MA signals (holistic)
        sum(total_cost) over (
            partition by date_trunc('month', usage_date), warehouse_name
            order by usage_date
        )::number(38,2)                                    as month_to_date_cost,

        avg(total_cost) over (
            partition by warehouse_name
            order by usage_date
            rows between 6 preceding and current row
        )::number(38,2)                                    as cost_7day_avg,

        (total_cost - lag(total_cost, 1) over (
            partition by warehouse_name order by usage_date
        ))::number(38,2)                                   as day_over_day_change,

        (total_cost - lag(total_cost, 7) over (
            partition by warehouse_name order by usage_date
        ))::number(38,2)                                   as week_over_week_change

    from compute_costs
)

select
    *,
    {{ dbt_utils.generate_surrogate_key(['usage_date', 'warehouse_name']) }} as daily_cost_key,
    cast(current_timestamp() as timestamp_ntz) as _loaded_at
from daily_summary
