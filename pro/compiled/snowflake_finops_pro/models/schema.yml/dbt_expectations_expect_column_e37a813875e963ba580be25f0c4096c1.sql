






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and estimated_query_cost_usd >= 0
)
 as expression


    from DM_AE_FINOPS_DB.STG.int_query_cost_attribution
    where
        estimated_query_cost_usd is not null
    
    

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







