{{ config(materialized='table', enabled=var('enable_budget', true)) }}

-- Compare daily budgets vs actual spend per department
with actuals as (
    select
        usage_date,
        department,
        total_cost_usd as actual_usd
    from {{ ref('fct_cost_by_department') }}
),
budgets as (
    select
        cast(date as date) as usage_date,
        department,
        budget_usd
    from {{ ref('budget_daily') }}
)
select
    a.usage_date,
    a.department,
    a.actual_usd,
    b.budget_usd,
    (a.actual_usd - coalesce(b.budget_usd, 0)) as variance_usd
from actuals a
left join budgets b
  on a.department = b.department
 and a.usage_date = b.usage_date
