{{ config(materialized='view', enabled=var('pro_enabled', false)) }}

select *
from (
  select cast(null as string) as pro_feature
) t
where 1 = 0
