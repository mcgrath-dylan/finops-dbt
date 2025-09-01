# Snowflake FinOps (dbt) — Starter

**Purpose:** a lean, correct-by-design starter to analyze Snowflake spend.  
**Truth for $:** `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`.  
**Per-query $:** estimates only (attribution), clearly labeled.

## Quickstart
1. Set env vars (examples):  
   `COST_PER_CREDIT`, `WINDOW_DAYS`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`
2. Install deps & run:
   ```bash
   dbt deps
   dbt build --select +fct_daily_costs
