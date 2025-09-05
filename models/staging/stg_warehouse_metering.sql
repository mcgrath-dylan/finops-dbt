{{
    config(
        materialized='incremental',
        unique_key='metering_id',
        on_schema_change='sync_all_columns'
    )
}}

-- Authoritative (ACCOUNT_USAGE) or Demo overlay via macro
with source as (
    select *
    from {{ metering_relation() }}
    where START_TIME >= dateadd('day', -{{ var('metering_history_days') }}, current_date())
    {% if is_incremental() %}
      and date(END_TIME) >= (
          select coalesce(max(t.usage_date), '1900-01-01'::date)
          from {{ this }} as t
      )
    {% endif %}
),

-- Normalize and add cost ($ = credits * cost_per_credit)
normalized as (
    select
        -- keys & time
        date_trunc('hour', START_TIME)                    as hour_start,
        date_trunc('hour', END_TIME)                      as hour_end,
        cast(date_trunc('day', START_TIME) as date)       as usage_date,

        -- warehouse
        WAREHOUSE_ID,
        WAREHOUSE_NAME,

        -- credits (ACCOUNT_USAGE-compatible column names)
        CREDITS_USED                       as total_credits_used,
        CREDITS_USED_COMPUTE              as credits_used_compute,
        CREDITS_USED_CLOUD_SERVICES       as credits_used_cloud_services,

        -- dollars (authoritative)
        (CREDITS_USED * {{ var('cost_per_credit') }})                as total_cost_usd,
        (CREDITS_USED_COMPUTE * {{ var('cost_per_credit') }})        as compute_cost_usd,
        (CREDITS_USED_CLOUD_SERVICES * {{ var('cost_per_credit') }}) as cloud_services_cost_usd,

        -- stable unique key per warehouse-hour
        concat_ws('|', WAREHOUSE_ID::string, to_char(END_TIME, 'YYYY-MM-DD HH24:MI:SS')) as metering_id,

        cast(current_timestamp() as timestamp_ntz) as _loaded_at
    from source
)

select
    hour_start,
    hour_end,
    usage_date,
    WAREHOUSE_ID       as warehouse_id,
    WAREHOUSE_NAME     as warehouse_name,
    total_credits_used,
    credits_used_compute,
    credits_used_cloud_services,
    compute_cost_usd,
    cloud_services_cost_usd,
    (total_cost_usd - compute_cost_usd) as idle_cost_usd,
    total_cost_usd,
    metering_id,
    _loaded_at
from normalized
