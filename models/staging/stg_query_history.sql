{% set query_pre_hooks = [] %}
{% if execute %}
  {% set existing_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) %}
  {% if existing_relation %}
    {% set existing_relation_str = existing_relation.render() %}
    {% set existing_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% set existing_column_names = [] %}
    {% for col in existing_columns %}
      {% do existing_column_names.append(col.name | lower) %}
    {% endfor %}

    {% if 'usage_hour_ntz' not in existing_column_names %}
      {% do query_pre_hooks.append("alter table " ~ existing_relation_str ~ " add column usage_hour_ntz timestamp_ntz") %}
    {% endif %}

    {% do query_pre_hooks.append(
        "update " ~ existing_relation_str ~ " set usage_hour_ntz = date_trunc('hour', end_time::timestamp_ntz) "
        ~ "where usage_hour_ntz is null or usage_hour_ntz is distinct from date_trunc('hour', end_time::timestamp_ntz)"
    ) %}

    {% do query_pre_hooks.append(
        "update " ~ existing_relation_str ~ " set usage_date = cast(date_trunc('hour', end_time::timestamp_ntz) as date) "
        ~ "where usage_date is distinct from cast(date_trunc('hour', end_time::timestamp_ntz) as date)"
    ) %}
  {% endif %}
{% endif %}

{{
    config(
        materialized='incremental',
        unique_key='query_id',
        on_schema_change='sync_all_columns',
        cluster_by=['usage_date', 'warehouse_name'],
        pre_hook=query_pre_hooks
    )
}}

{% set existing_cols = [] %}
{% if execute and is_incremental() %}
  {% set existing_cols = adapter.get_columns_in_relation(this) %}
{% endif %}
{% set existing_col_names = [] %}
{% for col in existing_cols %}
  {% do existing_col_names.append(col.name | lower) %}
{% endfor %}
{% set has_usage_hour_ntz = 'usage_hour_ntz' in existing_col_names %}

with source as (
    select *
    from {{ source('account_usage', 'QUERY_HISTORY') }}
    where START_TIME >= dateadd('day', -{{ var('query_history_days') }}, current_date())
    {% if is_incremental() %}
      and date_trunc('hour', END_TIME::timestamp_ntz) >= (
          select coalesce(
              dateadd(
                  'hour',
                  -1,
                  max(
                      {% if has_usage_hour_ntz %}
                          t.usage_hour_ntz
                      {% else %}
                          date_trunc('hour', t.end_time::timestamp_ntz)
                      {% endif %}
                  )
              ),
              '1970-01-01'::timestamp_ntz
          )
          from {{ this }} as t
      )
    {% endif %}
),

transformed as (
    select
        -- Keys & time
        QUERY_ID                                           as query_id,
        date_trunc('hour', END_TIME::timestamp_ntz)        as usage_hour_ntz,
        cast(date_trunc('hour', END_TIME::timestamp_ntz) as date) as usage_date,
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

        current_timestamp() as _loaded_at
    from source
    where EXECUTION_STATUS = 'SUCCESS'
      and WAREHOUSE_NAME is not null   -- only compute-bearing statements
)

select * from transformed
