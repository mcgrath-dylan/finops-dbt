






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and opportunity_count >= 0
)
 as expression


    from DM_AE_FINOPS_DB.STG.optimization_summary
    

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







