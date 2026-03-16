{{ config(materialized='view', enabled=var('enable_pro_pack', false)) }}

select *
from (
  select cast(null as string) as pro_feature
) t
where 1 = 0
