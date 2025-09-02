{{ config(materialized='table') }}

-- 7/30-day rolling totals by department
with daily as (
    select
        usage_date,
        department,
        total_cost_usd,
        compute_cost_usd,
        idle_cost_usd
    from {{ ref('cost_by_department') }}
)
select
    usage_date,
    department,
    total_cost_usd,
    compute_cost_usd,
    idle_cost_usd,
    sum(total_cost_usd)   over (partition by department order by usage_date rows between 6 preceding  and current row) as total_cost_7d_usd,
    sum(total_cost_usd)   over (partition by department order by usage_date rows between 29 preceding and current row) as total_cost_30d_usd,
    sum(compute_cost_usd) over (partition by department order by usage_date rows between 6 preceding  and current row) as compute_cost_7d_usd,
    sum(idle_cost_usd)    over (partition by department order by usage_date rows between 6 preceding  and current row) as idle_cost_7d_usd
from daily
