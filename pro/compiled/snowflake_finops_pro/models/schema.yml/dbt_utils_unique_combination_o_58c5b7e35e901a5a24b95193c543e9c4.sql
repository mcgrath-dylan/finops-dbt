





with validation_errors as (

    select
        opportunity_type
    from DM_AE_FINOPS_DB.STG.optimization_summary
    group by opportunity_type
    having count(*) > 1

)

select *
from validation_errors


