{{
  config(
    materialized='table',
    schema='DEMO',
    alias='WAREHOUSE_METERING_HISTORY',
    enabled=var('DEMO_MODE', false),
    tags=['mode:demo']
  )
}}

with base as (
  select
    to_timestamp_ntz(START_TIME) as START_TIME,
    to_timestamp_ntz(END_TIME)   as END_TIME,
    WAREHOUSE_NAME,
    TOTAL_CREDITS_USED,
    TOTAL_COST_USD
  from {{ ref('metering_demo_seed') }}
),

-- derive a stable synthetic warehouse_id and split credits
aug as (
  select
    START_TIME,
    END_TIME,
    WAREHOUSE_NAME,

    -- deterministic id per warehouse name
    dense_rank() over (order by WAREHOUSE_NAME)           as WAREHOUSE_ID,

    -- split credits into compute/cloud-services (95/5)
    TOTAL_CREDITS_USED                                    as CREDITS_USED,
    round(TOTAL_CREDITS_USED * 0.95, 3)                   as CREDITS_USED_COMPUTE,
    round(TOTAL_CREDITS_USED * 0.05, 3)                   as CREDITS_USED_CLOUD_SERVICES,

    -- AU has credit columns; total_cost is re-derived downstream from cost_per_credit
    TOTAL_COST_USD                                        as TOTAL_COST
  from base
)

select * from aug
