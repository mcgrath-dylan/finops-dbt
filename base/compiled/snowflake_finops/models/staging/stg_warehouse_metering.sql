

-- Authoritative (ACCOUNT_USAGE) or Demo overlay via macro
with source as (
    select *
    from 
    DM_AE_FINOPS_DB.DEMO.WAREHOUSE_METERING_HISTORY   
  
    where START_TIME >= dateadd('day', -30, current_date())
    
      and date(END_TIME) >= (
          select coalesce(max(t.usage_date), '1900-01-01'::date)
          from DM_AE_FINOPS_DB.DEMO.stg_warehouse_metering as t
      )
    
),

-- Normalize and add cost ($ = credits * cost_per_credit)
normalized as (
    select
        -- keys & time (ACCOUNT_USAGE is LTZ; preserve LTZ for consistency)
        date_trunc('hour', START_TIME)              as hour_start,
        date_trunc('hour', END_TIME)                as hour_end,
        cast(date_trunc('day', START_TIME) as date) as usage_date,

        -- warehouse
        WAREHOUSE_ID,
        WAREHOUSE_NAME,

        -- credits (ACCOUNT_USAGE-compatible column names)
        CREDITS_USED                       as total_credits_used,
        CREDITS_USED_COMPUTE               as credits_used_compute,
        CREDITS_USED_CLOUD_SERVICES        as credits_used_cloud_services,

        -- dollars (authoritative)
        (CREDITS_USED * 3.0)                as total_cost_usd,
        (CREDITS_USED_COMPUTE * 3.0)        as compute_cost_usd,
        (CREDITS_USED_CLOUD_SERVICES * 3.0) as cloud_services_cost_usd,

        -- stable unique key per warehouse-hour
        concat_ws('|', WAREHOUSE_ID::string, to_char(END_TIME, 'YYYY-MM-DD HH24:MI:SS')) as metering_id,

        -- cast to ntz for stability downstream
        cast(current_timestamp() as timestamp_ntz) as _loaded_at
    from source
)

select
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