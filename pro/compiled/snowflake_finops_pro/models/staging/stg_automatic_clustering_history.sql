

with src as (
    select
        DATABASE_NAME,
        SCHEMA_NAME,
        TABLE_NAME,
        START_TIME,
        CREDITS_USED
    from SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
    where START_TIME >= dateadd('day', -30, current_date())
    
      and date(START_TIME) >= (
          select coalesce(max(date(START_TIME)), '1900-01-01'::date)
          from DM_AE_FINOPS_DB.STG.stg_automatic_clustering_history
      )
    
),

filtered as (
    select
        DATABASE_NAME     as database_name,
        SCHEMA_NAME       as schema_name,
        TABLE_NAME        as table_name,
        START_TIME        as start_time,
        CREDITS_USED      as credits_used,
        coalesce(DATABASE_NAME,'') || '|' ||
        coalesce(SCHEMA_NAME,'')  || '|' ||
        coalesce(TABLE_NAME,'')   || '|' ||
        to_char(START_TIME, 'YYYY-MM-DD HH24:MI:SS') as ac_key,
        cast(current_timestamp() as timestamp_ntz)   as _loaded_at
    from src
    where CREDITS_USED > 0
      and DATABASE_NAME is not null
      and SCHEMA_NAME   is not null
      and TABLE_NAME    is not null
)

select * from filtered