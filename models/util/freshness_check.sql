-- Selects the most recent END_TIME from metering to support a simple staleness test.
select max(END_TIME) as last_end_time
from {{ source('account_usage','WAREHOUSE_METERING_HISTORY') }}
