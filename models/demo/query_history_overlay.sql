{{
  config(
    materialized='table',
    schema='DEMO',
    alias='QUERY_HISTORY'
  )
}}

with base as (
    select
        QUERY_ID::varchar           as QUERY_ID,
        to_timestamp_ntz(START_TIME) as START_TIME,
        to_timestamp_ntz(END_TIME)   as END_TIME,
        USER_NAME::varchar           as USER_NAME,
        ROLE_NAME::varchar           as ROLE_NAME,
        WAREHOUSE_NAME::varchar      as WAREHOUSE_NAME,
        WAREHOUSE_SIZE::varchar      as WAREHOUSE_SIZE,
        QUERY_TYPE::varchar          as QUERY_TYPE,
        DATABASE_NAME::varchar       as DATABASE_NAME,
        SCHEMA_NAME::varchar         as SCHEMA_NAME,
        EXECUTION_STATUS::varchar    as EXECUTION_STATUS,
        BYTES_SCANNED::number        as BYTES_SCANNED,
        ROWS_PRODUCED::number        as ROWS_PRODUCED,
        TOTAL_ELAPSED_TIME::number   as TOTAL_ELAPSED_TIME,
        EXECUTION_TIME::number       as EXECUTION_TIME
    from {{ ref('query_history_demo_seed') }}
)

select * from base
