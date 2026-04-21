{% docs finops_overview %}
**FinOps for Snowflake Starter (v3.0.0)**

- Authoritative compute spend from `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`
- Storage cost tracking from `ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY`
- Layered dbt models: staging to intermediate to marts
- Marts: daily compute costs, daily storage costs, cost forecast, total cost summary, budget vs actual, top spenders, department showback
- Warehouse dimension with size/config metadata
- 30-day cost forecast with trend detection, day-of-week seasonality, and confidence bands
- Optional Pro pack adds query-level attribution and optimization candidates

{% enddocs %}

{% docs stg_storage_usage %}
Normalizes `ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY` into a cost-ready staging layer.
Converts bytes to terabytes and applies a configurable daily storage rate
(default: $23/TB/month on-demand). Grain: one row per database per calendar day.
{% enddocs %}

{% docs fct_daily_storage_costs %}
Daily storage cost by database with month-to-date running totals and 30-day rolling averages.
Breaks cost into active database storage and failsafe retention. Internal stage
storage is account-level in Snowflake and is not allocated to databases in the
Starter per-database mart.
Contract-enforced schema.
{% enddocs %}

{% docs fct_cost_forecast %}
Projects warehouse-level daily cost 30 days into the future using rolling average,
linear trend slope, and day-of-week seasonality. Confidence bands widen with forecast
horizon. Replaces the inline MTD run-rate calculation.
{% enddocs %}

{% docs fct_total_cost_summary %}
Single source of truth for total Snowflake Starter spend. Unions compute and storage
costs into one cost_category by day fact table. Includes percentage of daily total
and month-to-date running sums per category.
{% enddocs %}

{% docs int_top_spenders %}
Aggregates query activity to user, role, database, warehouse, and day grain.
When the Pro pack is enabled, includes estimated per-user cost from query attribution.
In Starter mode, cost columns are null but volume metrics are still available.
{% enddocs %}

{% docs fct_top_spenders %}
Ranked user leaderboard with daily cost/volume rankings, percentage shares, and
7-day rolling cost. Rolls up int_top_spenders to user by day. Primary warehouse
is the warehouse where the user had the most queries that day.
{% enddocs %}

{% docs dim_warehouse %}
Warehouse metadata derived from metering and query history so it works in fresh
trial accounts where no warehouse configuration Account Usage view is exposed.
One row per metered warehouse with the latest observed size when query history
contains it.
{% enddocs %}
