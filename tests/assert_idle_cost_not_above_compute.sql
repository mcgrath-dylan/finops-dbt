select
    usage_date,
    warehouse_name,
    compute_cost,
    idle_cost
from {{ ref('fct_daily_costs') }}
where idle_cost > compute_cost
