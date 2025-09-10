{{ config(materialized='table') }}

with base as (
    select
        f.usage_date,
        coalesce(nullif(trim(m.department), ''), 'Unassigned') as department,
        f.warehouse_name,
        f.compute_cost as compute_cost_usd,
        f.idle_cost     as idle_cost_usd,
        (f.compute_cost + f.idle_cost) as total_cost_usd
    from {{ ref('fct_daily_costs') }} f
    left join {{ ref('department_mapping') }} m
      on upper(f.warehouse_name) = upper(m.warehouse_name)
)
select
    usage_date,
    department,
    sum(compute_cost_usd) as compute_cost_usd,
    sum(idle_cost_usd)    as idle_cost_usd,
    sum(total_cost_usd)   as total_cost_usd
from base
group by 1, 2
