{% docs exposure_app_overview %}
**Snowflake FinOps dbt Starter — Base Pack**

- **Authoritative compute $**: from `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` (dbt staging → hourly → daily).
- **Facts**: `fct_daily_costs` (daily compute/cloud-services/idle/total $ by warehouse).
- **Business lens**: `fct_cost_by_department`, `fct_cost_trend`, `fct_budget_vs_actual` (budgets optional).
- **Quality & guardrails**: contracts/tests; a simple freshness monitor on metering recency.
- **Optional Pro Pack**: optimization lineage & candidate insights (gated). Docs site includes **Base** and **Pro** views.

Toggle `DEMO_MODE` to include synthetic rows for screenshots only.
{% enddocs %}
