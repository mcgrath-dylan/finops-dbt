{{
    config(
        materialized='incremental',
        unique_key='metering_id',
        on_schema_change='sync_all_columns'
    )
}}

{% set metering_watermark_column = 'usage_hour_ntz' %}
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

    {% if 'hour_end' in existing_column_names %}
      {% set backfill_sql %}
        update {{ existing_relation }}
        set usage_hour_ntz = coalesce(usage_hour_ntz, date_trunc('hour', hour_end::timestamp_ntz)),
            usage_date = coalesce(usage_date, cast(date_trunc('day', hour_end::timestamp_ntz) as date))
        where usage_hour_ntz is null
           or usage_date is null
      {% endset %}
      {% do run_query(backfill_sql) %}
    {% endif %}

    {% if 'usage_hour_ntz' in existing_column_names %}
      {% set metering_watermark_column = 'usage_hour_ntz' %}
    {% elif 'hour_end' in existing_column_names %}
      {% set metering_watermark_column = 'hour_end' %}
    {% else %}
      {% set metering_watermark_column = none %}
    {% endif %}
  {% else %}
    {% set metering_watermark_column = none %}
  {% endif %}
{% endif %}

-- Authoritative (ACCOUNT_USAGE) or Demo overlay via macro
with source as (
    select *
    from {{ metering_relation() }}
    where START_TIME >= dateadd('day', -{{ var('metering_history_days') }}, current_date())
    {% if is_incremental() %}
      {% if metering_watermark_column == 'usage_hour_ntz' %}
        and date_trunc('hour', END_TIME::timestamp_ntz) >= (
            select coalesce(
                dateadd('hour', -1, max(usage_hour_ntz)),
                '1970-01-01'::timestamp_ntz
            )
            from {{ this }}
        )
      {% elif metering_watermark_column == 'hour_end' %}
        and date_trunc('hour', END_TIME::timestamp_ntz) >= (
            select coalesce(
                dateadd('hour', -1, max(hour_end::timestamp_ntz)),
                '1970-01-01'::timestamp_ntz
            )
            from {{ this }}
        )
      {% else %}
        and date_trunc('hour', END_TIME::timestamp_ntz) >= '1970-01-01'::timestamp_ntz
      {% endif %}
    {% endif %}
),

-- Normalize and add cost ($ = credits * cost_per_credit)
normalized as (
    select
        -- keys & time (normalize to NTZ hour)
        date_trunc('hour', END_TIME::timestamp_ntz) as usage_hour_ntz,
        date_trunc('hour', START_TIME::timestamp_ntz) as hour_start,
        date_trunc('hour', END_TIME::timestamp_ntz) as hour_end,
        cast(date_trunc('day', END_TIME::timestamp_ntz) as date) as usage_date,

        -- warehouse
        WAREHOUSE_ID,
        WAREHOUSE_NAME,

        -- credits (ACCOUNT_USAGE-compatible column names)
        CREDITS_USED                       as total_credits_used,
        CREDITS_USED_COMPUTE               as credits_used_compute,
        CREDITS_USED_CLOUD_SERVICES        as credits_used_cloud_services,

        -- dollars (authoritative)
        (CREDITS_USED * {{ var('cost_per_credit') }})                as total_cost_usd,
        (CREDITS_USED_COMPUTE * {{ var('cost_per_credit') }})        as compute_cost_usd,
        (CREDITS_USED_CLOUD_SERVICES * {{ var('cost_per_credit') }}) as cloud_services_cost_usd,

        -- stable unique key per warehouse-hour
        concat_ws('|', WAREHOUSE_ID::string, to_char(date_trunc('hour', END_TIME::timestamp_ntz), 'YYYY-MM-DD HH24:MI:SS')) as metering_id,

        -- cast to ntz for stability downstream
        cast(current_timestamp() as timestamp_ntz) as _loaded_at
    from source
)

select
    usage_hour_ntz,
    hour_start,
    hour_end,
    usage_date,
    WAREHOUSE_ID   as warehouse_id,
    WAREHOUSE_NAME as warehouse_name,
    total_credits_used,
    credits_used_compute,
    credits_used_cloud_services,
    compute_cost_usd,
    cloud_services_cost_usd,
    (total_cost_usd - compute_cost_usd) as idle_cost_usd, -- informational; equals cloud_services_cost_usd
    total_cost_usd,
    metering_id,
    _loaded_at
from normalized
