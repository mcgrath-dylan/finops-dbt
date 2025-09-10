{{ config(materialized='table') }}

select
    usage_date,
    sum(compute_cost) as compute_cost_usd,
    sum(idle_cost)    as idle_cost_usd,
    sum(compute_cost + idle_cost) as total_cost_usd
from {{ ref('fct_daily_costs') }}
group by 1
order by 1
