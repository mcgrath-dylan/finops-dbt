





with validation_errors as (

    select
        date, department
    from DM_AE_FINOPS_DB.STG.budget_daily
    group by date, department
    having count(*) > 1

)

select *
from validation_errors


