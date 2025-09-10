{{ config(tags=['guard']) }}

-- Recency guard: max hour_end from staged metering (not raw source).
select max(hour_end) as last_end_time
from {{ ref('stg_warehouse_metering') }}