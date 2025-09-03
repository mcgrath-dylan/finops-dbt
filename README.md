# Snowflake FinOps (dbt) — Starter

![PR CI](https://github.com/mcgrath-dylan/finops-dbt/actions/workflows/ci.yml/badge.svg)
![Nightly](https://github.com/mcgrath-dylan/finops-dbt/actions/workflows/nightly.yml/badge.svg?branch=main)

**Docs & Lineage:** https://mcgrath-dylan.github.io/finops-dbt/

**Purpose:** a lean, correct-by-design starter to analyze Snowflake spend.  
**Truth for $:** `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`.  
**Per-query $:** estimates only (attribution), clearly labeled.

## Quickstart
1. Set env vars (examples):  
   `COST_PER_CREDIT`, `WINDOW_DAYS`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`
2. Install deps & run:
   dbt deps
   dbt seed --full-refresh
   dbt build --select +fct_daily_costs

## Pro Pack (optional)
Disabled by default. Enable locally:
dbt build --vars '{enable_pro_pack: true}'   

Outputs are informational: per-query dollars are **ESTIMATES**; optimization tables list **candidates**, not savings. Authoritative dollars = ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY.