

select
    usage_date,
    sum(compute_cost) as compute_cost_usd,
    sum(idle_cost)    as idle_cost_usd,
    sum(compute_cost + idle_cost) as total_cost_usd
from DM_AE_FINOPS_DB.DEMO.fct_daily_costs
group by 1
order by 1