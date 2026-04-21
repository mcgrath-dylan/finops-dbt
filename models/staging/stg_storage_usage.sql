{{
    config(
        materialized='incremental',
        unique_key='storage_snapshot_id',
        on_schema_change='sync_all_columns'
    )
}}

{#
  stg_storage_usage
  Normalizes ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY into a cost-ready staging layer.
  Grain: one row per database per calendar day.
  Converts bytes to TB and applies a configurable daily storage rate.
  Stage storage is account-level in ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY,
  so per-database stage columns stay at zero until an account-level storage
  summary model exists.
#}

{% set storage_watermark_column = 'usage_date' %}

with source as (

    select
        USAGE_DATE,
        DATABASE_ID,
        DATABASE_NAME,
        cast(AVERAGE_DATABASE_BYTES as number(38, 0)) as AVERAGE_DATABASE_BYTES,
        cast(AVERAGE_FAILSAFE_BYTES as number(38, 0)) as AVERAGE_FAILSAFE_BYTES
    from {{ storage_relation() }}

    {% if is_incremental() %}
    where cast(USAGE_DATE as date) > (
        select max({{ storage_watermark_column }}) from {{ this }}
    )
    {% else %}
    where USAGE_DATE >= dateadd('day', -{{ var('storage_history_days', 365) }}, current_date())
    {% endif %}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'coalesce(DATABASE_ID::varchar, DATABASE_NAME)',
            'USAGE_DATE'
        ]) }}                                               as storage_snapshot_id,

        cast(USAGE_DATE as date)                            as usage_date,
        DATABASE_ID                                         as database_id,
        DATABASE_NAME                                       as database_name,

        -- Raw bytes (kept for auditability)
        AVERAGE_DATABASE_BYTES                              as avg_database_bytes,
        AVERAGE_FAILSAFE_BYTES                              as avg_failsafe_bytes,
        0::number
                                                            as avg_stage_bytes,

        -- Terabyte conversions (1 TB = 1024^4 = 1099511627776 bytes)
        AVERAGE_DATABASE_BYTES  / 1099511627776.0           as avg_database_tb,
        AVERAGE_FAILSAFE_BYTES  / 1099511627776.0           as avg_failsafe_tb,
        0::number
            / 1099511627776.0                               as avg_stage_tb,

        (AVERAGE_DATABASE_BYTES
         + AVERAGE_FAILSAFE_BYTES
         + 0::number
        ) / 1099511627776.0                                 as total_storage_tb,

        -- Cost estimates (prorated daily from monthly rate)
        -- Default on-demand: $23/TB/month is about $0.756/TB/day (23 / 30.44)
        {% set daily_rate %}
            ({{ var('storage_cost_per_tb_per_month', 23.0) }} / 30.44)
        {% endset %}

        (AVERAGE_DATABASE_BYTES / 1099511627776.0) * {{ daily_rate }}
                                                            as estimated_active_cost_usd,
        (AVERAGE_FAILSAFE_BYTES / 1099511627776.0) * {{ daily_rate }}
                                                            as estimated_failsafe_cost_usd,
        (0::number / 1099511627776.0) * {{ daily_rate }}
                                                            as estimated_stage_cost_usd,

        ((AVERAGE_DATABASE_BYTES
          + AVERAGE_FAILSAFE_BYTES
          + 0::number
        ) / 1099511627776.0) * {{ daily_rate }}
                                                            as estimated_storage_cost_usd

    from source

)

select * from renamed
