





with validation_errors as (

    select
        usage_date, department
    from DM_AE_FINOPS_DB.STG.fct_budget_vs_actual
    group by usage_date, department
    having count(*) > 1

)

select *
from validation_errors


