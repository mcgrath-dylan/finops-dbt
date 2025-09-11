






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and idle_cost >= 0
)
 as expression


    from DM_AE_FINOPS_DB.STG.fct_daily_costs
    

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







