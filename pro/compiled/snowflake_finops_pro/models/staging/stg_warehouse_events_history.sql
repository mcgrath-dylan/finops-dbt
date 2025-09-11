










with src as (
  select
    warehouse_name,

    /* normalize to event_type */
     event_name as event_type
    ,

    /* normalize to event_ts */
     "TIMESTAMP"
     as event_ts

  from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY
  
    where
       "TIMESTAMP"
      
      >= dateadd('day', - ( 30 )::int, current_timestamp())
  
)

select warehouse_name, event_type, event_ts
from src