# Snowflake FinOps (dbt) — Starter

![PR CI](https://github.com/mcgrath-dylan/finops-dbt/actions/workflows/ci.yml/badge.svg)
![Nightly](https://github.com/mcgrath-dylan/finops-dbt/actions/workflows/nightly.yml/badge.svg?branch=main)

**Docs & Lineage:** https://mcgrath-dylan.github.io/finops-dbt/

**Purpose:** a lean, correct-by-design starter to analyze Snowflake spend.  
**Truth for $:** `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`.  
**Per-query $:** estimates only (attribution), clearly labeled.

---

## What’s in v0.5
- **Baseline marts (authoritative $):** `fct_daily_costs`, `cost_by_department`, `cost_trend`.
- **Pro Pack (optional, gated):** query-level $ attribution + warehouse optimization candidates.
- **Read-only demo app:** `app/streamlit_app.py` (Streamlit) visualizes the marts; auto-detects Pro models if present.
- **Docs/lineage exposure:** `models/exposures.yml` registers the app in dbt Docs.

---

## Quickstart

**Env vars (examples):**  
`COST_PER_CREDIT`, `WINDOW_DAYS`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`

**Install & build (baseline):**
```bash
dbt deps
dbt seed --full-refresh
dbt build
dbt docs generate
