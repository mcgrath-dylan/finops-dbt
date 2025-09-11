
    
    

with child as (
    select warehouse_name as from_field
    from DM_AE_FINOPS_DB.DEMO.fct_daily_costs
    where warehouse_name is not null
),

parent as (
    select warehouse_name as to_field
    from DM_AE_FINOPS_DB.DEMO.stg_warehouse_metering
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


