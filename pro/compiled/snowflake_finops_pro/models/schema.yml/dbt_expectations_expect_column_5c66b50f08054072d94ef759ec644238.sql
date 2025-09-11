






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and credits_used >= 0
)
 as expression


    from DM_AE_FINOPS_DB.STG.stg_automatic_clustering_history
    

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







