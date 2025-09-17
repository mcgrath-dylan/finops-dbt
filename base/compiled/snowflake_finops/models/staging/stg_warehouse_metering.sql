




  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    

    
    

    
      
      
    

    
      
    
  


-- Authoritative (ACCOUNT_USAGE) or Demo overlay via macro
with source as (
    select *
    from 
    SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
  
    where START_TIME >= dateadd('day', -30, current_date())
    
      
        and date_trunc('hour', END_TIME::timestamp_ntz) >= (
            select coalesce(
                dateadd('hour', -1, max(usage_hour_ntz)),
                '1970-01-01'::timestamp_ntz
            )
            from DM_AE_FINOPS_DB.STG.stg_warehouse_metering
        )
      
    
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
        (CREDITS_USED * 3.0)                as total_cost_usd,
        (CREDITS_USED_COMPUTE * 3.0)        as compute_cost_usd,
        (CREDITS_USED_CLOUD_SERVICES * 3.0) as cloud_services_cost_usd,

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