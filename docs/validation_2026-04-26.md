# v3.0.0 Snowflake Validation - 2026-04-26

This file captures the day 3-7 live Snowflake validation pass after the fresh
trial account has ACCOUNT_USAGE baseline data.

## Day 0 MVP Gate

Command:

```bash
dbt build --profiles-dir .ci/profiles --target demo --vars '{"DEMO_MODE": true, "enable_pro_pack": false}'
dbt test --profiles-dir .ci/profiles --target demo --vars '{"DEMO_MODE": true, "enable_pro_pack": false}'
dbt compile --profiles-dir .ci/profiles --target dev
```

Result:

- Status: Passed on 2026-04-12
- dbt version: dbt-core 1.11.7, dbt-snowflake 1.11.3
- Commit: v3.0.0 release-prep commit
- Notes: `dbt debug --target dev`, `dbt compile --target dev`, `dbt build --target demo`, and `dbt test --target demo` completed successfully against the fresh Snowflake account. Demo build completed with PASS=96 WARN=0 ERROR=0 SKIP=0 NO-OP=1 TOTAL=97. Explicit demo test completed with PASS=75 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=75.

## Day 3-7 Live Validation

Prerequisites:

- `scripts/bootstrap_snowflake.sql` run once as ACCOUNTADMIN
- `scripts/generate_demo_workload.sql` run 2-3 times
- ACCOUNT_USAGE latency cleared

Command:

```bash
dbt build --profiles-dir .ci/profiles --target dev --vars '{"DEMO_MODE": false, "enable_pro_pack": false}'
```

Result:

- Status:
- Started at:
- Finished at:
- Log artifact:

## Row Counts

| Model | Row count | Max date/timestamp | Notes |
| --- | ---: | --- | --- |
| `stg_warehouse_metering` |  |  |  |
| `stg_query_history` |  |  |  |
| `stg_storage_usage` |  |  |  |
| `int_hourly_compute_costs` |  |  |  |
| `fct_daily_costs` |  |  |  |
| `fct_daily_storage_costs` |  |  |  |
| `fct_cost_forecast` |  |  |  |
| `fct_total_cost_summary` |  |  |  |
| `fct_top_spenders` |  |  |  |
| `dim_warehouse` |  |  |  |

## Screenshots

| Screen | Path | Notes |
| --- | --- | --- |
| Overview KPIs |  |  |
| Storage costs |  |  |
| Forecast |  |  |
| Top spenders |  |  |

## Follow-Ups

- Re-enable strict CI source freshness after 2026-05-03.
- Replace demo-data screenshots with live screenshots after ACCOUNT_USAGE has a stable baseline.
