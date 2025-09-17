
    
    

select
    cost_hour_key as unique_field,
    count(*) as n_records

from DM_AE_FINOPS_DB.STG.int_hourly_compute_costs
where cost_hour_key is not null
group by cost_hour_key
having count(*) > 1


