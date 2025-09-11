






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and estimated_monthly_cost_to_review_usd >= 0
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







