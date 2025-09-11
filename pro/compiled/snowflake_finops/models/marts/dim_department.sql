

select
    upper(trim(warehouse_name)) as warehouse_name,
    trim(department) as department
from DM_AE_FINOPS_DB.STG.department_mapping