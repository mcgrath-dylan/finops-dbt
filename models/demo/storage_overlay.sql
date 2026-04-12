{{
  config(
    materialized='table',
    schema='DEMO',
    alias='STORAGE_USAGE'
  )
}}

{#
  storage_overlay
  DEMO_MODE counterpart to ACCOUNT_USAGE.STORAGE_USAGE.
  Reads from storage_demo_seed and normalizes column names to match
  the live source schema. Used by storage_relation() macro.
#}

with base as (

    select
        to_date(USAGE_DATE)              as USAGE_DATE,
        DATABASE_ID::number              as DATABASE_ID,
        DATABASE_NAME::varchar           as DATABASE_NAME,
        AVERAGE_DATABASE_BYTES::number   as AVERAGE_DATABASE_BYTES,
        AVERAGE_FAILSAFE_BYTES::number   as AVERAGE_FAILSAFE_BYTES,
        AVERAGE_STAGE_BYTES::number      as AVERAGE_STAGE_BYTES
    from {{ ref('storage_demo_seed') }}

)

select * from base
