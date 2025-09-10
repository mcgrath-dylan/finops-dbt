{{ config(tags=['pack:base', 'stage:marts']) }}
select
  trim(department)      as department,
  upper(warehouse_name) as warehouse_name
from {{ ref('department_mapping') }}
