{{ config(materialized='view') }}

select max(hour_end) as last_end_time
from {{ ref('stg_warehouse_metering') }}
