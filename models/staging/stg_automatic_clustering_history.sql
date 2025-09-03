{{ config(
    enabled=var('enable_pro_pack', false),
    materialized='incremental'
) }}

with src as (
  select
    database_name,
    schema_name,
    table_name,
    start_time,
    end_time,
    credits_used
  from {{ source('account_usage','AUTOMATIC_CLUSTERING_HISTORY') }}
  {% if is_incremental() %}
    where start_time >= dateadd('day', - ( {{ var('WINDOW_DAYS', 30) }} )::int, current_timestamp())
  {% endif %}
)

select
  database_name,
  schema_name,
  table_name,
  start_time,
  end_time,
  credits_used
from src
