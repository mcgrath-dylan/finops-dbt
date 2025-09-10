{{ config(materialized='view') }}

select
    upper(trim(warehouse_name)) as warehouse_name,
    trim(department) as department
from {{ ref('department_mapping') }}
