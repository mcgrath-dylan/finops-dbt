{{ config(materialized='table') }}

with daily_actuals as (
  select
    f.usage_date,
    coalesce(nullif(trim(m.department), ''), 'Unassigned') as department,
    sum(f.compute_cost + f.idle_cost) as actual_cost_usd
  from {{ ref('fct_daily_costs') }} f
  left join {{ ref('department_mapping') }} m
    on upper(f.warehouse_name) = upper(m.warehouse_name)
  group by 1,2
),
budget as (
  select
    cast(b.date as date) as usage_date,
    trim(b.department)    as department,
    b.budget_usd
  from {{ ref('budget_daily') }} b
)
select
  coalesce(a.usage_date, b.usage_date) as usage_date,
  coalesce(a.department, b.department) as department,
  coalesce(a.actual_cost_usd, 0)       as actual_cost_usd,
  coalesce(b.budget_usd, 0)            as budget_usd
from daily_actuals a
full outer join budget b
  on a.usage_date = b.usage_date
 and a.department = b.department
order by 1,2
