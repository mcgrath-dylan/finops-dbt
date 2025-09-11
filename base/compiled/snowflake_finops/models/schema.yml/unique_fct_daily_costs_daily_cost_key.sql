
    
    

select
    daily_cost_key as unique_field,
    count(*) as n_records

from DM_AE_FINOPS_DB.DEMO.fct_daily_costs
where daily_cost_key is not null
group by daily_cost_key
having count(*) > 1


