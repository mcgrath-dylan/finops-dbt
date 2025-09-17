{{
    config(
        materialized='incremental',
        unique_key='query_id',
        on_schema_change='sync_all_columns',
        cluster_by=['usage_date', 'warehouse_name']
    )
}}

{% set query_history_watermark_column = 'usage_hour_ntz' %}
{% set existing_column_names = [] %}
{% if is_incremental() and execute %}
  {% set existing_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) %}
  {% if existing_relation %}
    {% set existing_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% for col in existing_columns %}
      {% do existing_column_names.append(col.name | lower) %}
    {% endfor %}

    {% if 'usage_hour_ntz' not in existing_column_names %}
      {% do run_query('alter table ' ~ existing_relation ~ ' add column usage_hour_ntz timestamp_ntz') %}
      {% do existing_column_names.append('usage_hour_ntz') %}
    {% endif %}
    {% if 'usage_date' not in existing_column_names %}
      {% do run_query('alter table ' ~ existing_relation ~ ' add column usage_date date') %}
      {% do existing_column_names.append('usage_date') %}
    {% endif %}

    {% if 'end_time' in existing_column_names %}
      {% set backfill_sql %}
        update {{ existing_relation }}
        set usage_hour_ntz = coalesce(usage_hour_ntz, {{ ntz_hour('end_time') }}),
            usage_date = coalesce(usage_date, cast({{ ntz_hour('end_time') }} as date))
        where usage_hour_ntz is null
           or usage_date is null
      {% endset %}
      {% do run_query(backfill_sql) %}
    {% endif %}

    {% if 'usage_hour_ntz' in existing_column_names %}
      {% set query_history_watermark_column = 'usage_hour_ntz' %}
    {% elif 'usage_date' in existing_column_names %}
      {% set query_history_watermark_column = 'usage_date' %}
    {% else %}
      {% set query_history_watermark_column = none %}
    {% endif %}
  {% else %}
    {% set query_history_watermark_column = none %}
  {% endif %}
{% endif %}

with source as (
    select *
    from {{ source('account_usage', 'QUERY_HISTORY') }}
    where START_TIME >= dateadd('day', -{{ var('query_history_days') }}, current_date())
    {% if is_incremental() %}
      {% if query_history_watermark_column == 'usage_hour_ntz' %}
        and {{ ntz_hour('END_TIME') }} >= (
            select coalesce(
                dateadd('hour', -1, max(usage_hour_ntz)),
                '1970-01-01'::timestamp_ntz
            )
            from {{ this }}
        )
      {% elif query_history_watermark_column == 'usage_date' %}
        and {{ ntz_hour('END_TIME') }} >= (
            select coalesce(
                dateadd('day', -1, max(usage_date)::timestamp_ntz),
                '1970-01-01'::timestamp_ntz
            )
            from {{ this }}
        )
      {% else %}
        and {{ ntz_hour('END_TIME') }} >= '1970-01-01'::timestamp_ntz
      {% endif %}
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
