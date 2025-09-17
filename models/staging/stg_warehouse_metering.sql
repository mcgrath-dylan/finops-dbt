{{
    config(
        materialized='incremental',
        unique_key='metering_id',
        on_schema_change='sync_all_columns'
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

-- Authoritative (ACCOUNT_USAGE) or Demo overlay via macro
with source as (
    select *
    from {{ metering_relation() }}
    where START_TIME >= dateadd('day', -{{ var('metering_history_days') }}, current_date())
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
                          t.hour_end::timestamp_ntz
                      {% endif %}
                  )
              ),
              '1970-01-01'::timestamp_ntz
          )
          from {{ this }} as t
      )
    {% endif %}
),

-- Normalize and add cost ($ = credits * cost_per_credit)
normalized as (
    select
        -- keys & time (normalize to NTZ hour)
        date_trunc('hour', END_TIME::timestamp_ntz) as usage_hour_ntz,
        date_trunc('hour', START_TIME::timestamp_ntz) as hour_start,
        date_trunc('hour', END_TIME::timestamp_ntz) as hour_end,
        cast(date_trunc('hour', END_TIME::timestamp_ntz) as date) as usage_date,

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
