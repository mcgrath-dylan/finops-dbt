{{
    config(
        materialized='incremental',
        unique_key='query_id',
        on_schema_change='sync_all_columns',
        cluster_by=['usage_date', 'warehouse_name']
    )
}}

with source as (
    select *
    from {{ source('account_usage', 'QUERY_HISTORY') }}
    where START_TIME >= dateadd('day', -{{ var('query_history_days') }}, current_date())
    {% if is_incremental() %}
      and {{ ntz_hour('END_TIME') }} >= (
          select coalesce(
              dateadd('hour', -1, max(usage_hour_ntz)),
              '1970-01-01'::timestamp_ntz
          )
          from {{ this }}
      )
    {% endif %}
),

filtered as (
    select *
    from source
    where EXECUTION_STATUS = 'SUCCESS'
      and WAREHOUSE_NAME is not null   -- only compute-bearing statements
),

transformed as (
    select
        -- Keys & time
        QUERY_ID                                    as query_id,
        {{ ntz_hour('END_TIME') }}                   as usage_hour_ntz,
        cast({{ ntz_hour('END_TIME') }} as date)     as usage_date,
        START_TIME,
        END_TIME,

        -- Who/where
        USER_NAME       as user_name,
        ROLE_NAME       as role_name,
        WAREHOUSE_NAME  as warehouse_name,
        WAREHOUSE_SIZE  as warehouse_size,

        -- What
        QUERY_TYPE      as query_type,
        DATABASE_NAME   as database_name,
        SCHEMA_NAME     as schema_name,
        EXECUTION_STATUS,
        BYTES_SCANNED,
        ROWS_PRODUCED,
        TOTAL_ELAPSED_TIME as total_elapsed_ms,
        EXECUTION_TIME     as execution_ms,

        -- Convenience fields
        BYTES_SCANNED / 1024 / 1024 / 1024.0 as gb_scanned,
        TOTAL_ELAPSED_TIME / 1000.0          as total_elapsed_seconds,

        case
            when TOTAL_ELAPSED_TIME > 600000 then 'long_running'   -- > 10 min
            when TOTAL_ELAPSED_TIME > 60000  then 'medium_running' -- > 1 min
            else 'fast'
        end as runtime_category,

        cast(current_timestamp() as timestamp_ntz) as _loaded_at
    from filtered
)

select * from transformed
