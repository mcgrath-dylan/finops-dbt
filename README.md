# FinOps for Snowflake + dbt (**Starter v1.0.0**)

![CI](https://github.com/mcgrath-dylan/finops-dbt/actions/workflows/ci.yml/badge.svg)
![Nightly](https://github.com/mcgrath-dylan/finops-dbt/actions/workflows/nightly.yml/badge.svg?branch=main)

A plug & play app to analyze Snowflake spend with dbt, powered by a small Streamlit app for quick visual insights.

- **Authoritative $**: derived from `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`
- **Clear labeling**: per-query/attribution numbers are estimates and displayed as such
- **Pro-ready**: detects an optional private **Pro** package when installed; otherwise shows an upgrade card

> **Docs & Lineage:** published via dbt Docs (exposure listed in `models/exposures.yml`).

---

## What you get

- **Models (Starter)**  
  - `stg_*` staging from ACCOUNT_USAGE  
  - `marts/fct_daily_costs.sql` – authoritative daily spend  
  - `marts/cost_by_department.sql` – department mapping & window totals  
  - `marts/cost_trend.sql` – daily trend for charts  
  - (Optional) `marts/budget_vs_actual.sql` – if budgets are seeded

- **Streamlit App** (`app/streamlit_app.py`)
  - **Hero (2×2)**: Month-to-date Spend, Forecast (month), **Idle Wasted (last N days)**, **Idle Projected (month)**  
    - *Idle Wasted* and *Idle Projected* appears only if the Pro package is installed; otherwise an upgrade card is shown.  
  - **Spend by Department** & **Top Warehouses** with tidy, consistent labels  
  - **Download insights CSV** button

- **Docs/Exposure**  
  - `models/exposures.yml` registers the app in dbt Docs/Lineage

---

## Quickstart

### 1) Requirements
- Python 3.11+
- dbt Core with Snowflake adapter (tested on dbt-core 1.10.x, snowflake-adapter 1.10.x)
- A Snowflake account with access to `ACCOUNT_USAGE` (or use Demo mode)

### 2) Configure Snowflake (profiles.yml)
Create a `profiles.yml` entry named `finops_dbt` (standard dbt location), for example:
```yaml
finops_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <acct>
      user: <user>
      password: <password>
      role: <role>
      database: <database>
      warehouse: <warehouse>
      schema: <schema>
````

### 3) Install & build

```bash
# from repo root
dbt deps
dbt seed --full-refresh        # loads demo/budget/mapping seeds if present
dbt build                      # builds starter models
dbt docs generate              # optional: build docs
```

### 4) Run the app

```bash
# from repo root
streamlit run app/streamlit_app.py
```

Use the left-hand **Controls** to set **Days shown** and **Rows to show**.
Toggle **Demo mode** if you want to explore the app without hitting your Snowflake account.

---

## Pro Pack (optional, licensed)

The **Starter** works on its own. If you license the **Pro** package:

* You’ll receive access to a **private Git repo** containing additional models (e.g., hourly idle model, optimization candidates, fine-grained attribution).
* Add it to `packages.yml` in the starter and run `dbt deps`. The app auto-detects Pro models and enables **Pro Pack insights** and the **Idle (projected, month)** KPI.
* The starter repo does not expose Pro internals. Contact the author for access and pricing.

---

## Configuration & Data

* **Authoritative spend** = Compute + (optionally) Cloud Services; calculated from `WAREHOUSE_METERING_HISTORY`.
* **Department mapping** (optional) = `seeds/department_mapping.csv` (customize to your org).
* **Budgets** (optional) = `seeds/budget_daily.csv` (customize to your org).
* **Demo data** = `seeds/metering_demo_seed.csv` for local exploration without Snowflake.

**Environment knobs in the app**

* **Days shown** (slider) – default 30, adjustable from 7 up to 90
* **Rows to show** – limits table lengths for readability (e.g. number of departments, warehouses visible)
* **Demo mode** – bypasses live queries and uses cached/demo data

---

## CI / CD

* **CI** (`.github/workflows/ci.yml`): basic dbt parse/build checks on PRs
* **Nightly** (`.github/workflows/nightly.yml`): scheduled `dbt build` for drift detection

---

## Troubleshooting

* If you recently added the Pro package, run:

  ```bash
  dbt clean && dbt deps
  ```
* If a model complains about timestamp types in staging: drop/recreate the staging relation or run a full refresh of the affected seed/demo models.
* To trace credentials/profile use:

  ```bash
  dbt debug
  ```

---

## Roadmap (starter)

* Small visual polish passes (spacing, empty-state cards)
* Optional budget deltas chip per department
* Additional export formats (Parquet)

---

## License & Contact

Starter is open for portfolio/demo purposes. Pro is licensed separately.
Questions, licensing, or support: **[mcgrath.fintech@gmail.com](mailto:mcgrath.fintech@gmail.com)**
