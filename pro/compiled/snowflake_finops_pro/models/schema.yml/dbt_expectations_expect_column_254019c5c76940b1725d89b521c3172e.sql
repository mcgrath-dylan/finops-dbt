






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and idle_cost_percentage >= 0 and idle_cost_percentage <= 100.001
)
 as expression


    from DM_AE_FINOPS_DB.STG.int_warehouse_optimization
    where
        idle_cost_percentage is not null
    
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







