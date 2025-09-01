{{
    config(
        materialized='table'
    )
}}

with compute_costs as (
    select
        usage_date,
        warehouse_name,
        sum(total_credits_used) as compute_credits,
        sum(total_cost_usd) as compute_cost,
        sum(idle_cost_usd) as idle_cost,
        sum(queries_executed) as total_queries,
        avg(unique_users) as avg_concurrent_users
    from {{ ref('int_hourly_compute_costs') }}
    group by 1, 2
),

-- Add storage costs when implementing storage staging
-- Add clustering costs when implementing clustering staging

daily_summary as (
    select
        usage_date,
        warehouse_name,
        compute_credits,
        compute_cost,
        idle_cost,
        compute_cost - idle_cost as productive_cost,
        total_queries,
        avg_concurrent_users,
        
        -- Cost per query (when queries exist)
        case 
            when total_queries > 0 then compute_cost / total_queries
            else null
        end as cost_per_query,
        
        -- Efficiency score (0-100)
        case 
            when compute_cost > 0 then 100 * (1 - (idle_cost / compute_cost))
            else 100
        end as efficiency_score,
        
        -- Running totals for the month
        sum(compute_cost) over (
            partition by date_trunc('month', usage_date), warehouse_name 
            order by usage_date
        ) as month_to_date_cost,
        
        -- 7-day moving average
        avg(compute_cost) over (
            partition by warehouse_name 
            order by usage_date 
            rows between 6 preceding and current row
        ) as cost_7day_avg,
        
        -- Day-over-day change
        compute_cost - lag(compute_cost, 1) over (
            partition by warehouse_name 
            order by usage_date
        ) as day_over_day_change,
        
        -- Week-over-week change
        compute_cost - lag(compute_cost, 7) over (
            partition by warehouse_name 
            order by usage_date
        ) as week_over_week_change
        
    from compute_costs
)

select 
    *,
    {{ dbt_utils.generate_surrogate_key(['usage_date', 'warehouse_name']) }} as daily_cost_key,
    current_timestamp() as _loaded_at
from daily_summary