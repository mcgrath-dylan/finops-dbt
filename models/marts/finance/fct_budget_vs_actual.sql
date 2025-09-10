{{ config(materialized='table') }}

select
    b.date as usage_date,
    b.budget_usd,
    coalesce(f.total_cost, 0) as actual_cost_usd
from {{ ref('budget_daily') }} b
left join (
    select usage_date, sum(compute_cost + idle_cost) as total_cost
    from {{ ref('fct_daily_costs') }}
    group by 1
) f
  on f.usage_date = b.date
order by 1
