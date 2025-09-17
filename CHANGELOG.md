# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

---

## [1.3.0] - 2025-09-17
### Changed
- Normalized warehouse metering and query staging to `usage_hour_ntz` with usage dates derived from NTZ hours for time-consistent joins.
- Applied hour-level incremental watermarks with a 1-hour lookback to staging and `int_hourly_compute_costs` to ensure idempotent reruns and preserve `cost_hour_key` uniqueness.
- Updated CI profile defaults to honor `DBT_TARGET`/`DBT_THREADS` env vars while defaulting to four threads for demo/dev/live targets.
- Added upgrade-safe pre-hooks that backfill NTZ hour, usage_date, warehouse_id, and surrogate keys so existing incremental tables adopt the new schema without manual refreshes.

---

## [v1.2.0] — 2025-09-16
### Added
- Dept-aware budget vs actual mart.
- AU freshness thresholds (warn 48h / error 96h).
- Schema tests for key models.

### Changed
- App now reads Live budget table with CSV fallback and shows Budget (MTD) and % Used (MTD) KPIs.

---

## [v1.1.0] — 2025-09-12
### Added
- **dbt Core 1.10 project** with layered models `stg_` → `int_` → `marts` plus `dim_department`.
- **Authoritative daily spend marts:** `models/marts/finance/fct_daily_costs.sql`, `fct_cost_by_department.sql`, `fct_cost_trend.sql`, `fct_budget_vs_actual.sql`.
- **Demo Mode** via seeds (`seeds/department_mapping.csv`, `seeds/budget_daily.csv`, `seeds/metering_demo_seed.csv`) and a `DEMO` schema overlay.
- **Docs & lineage publishing** using GitHub Actions to `gh-pages` at `/base/` (and `/pro/` when Pro is enabled).
- **Exposure** for the app: `models/exposures.yml` (`finops_streamlit_app`) with owner metadata.
- **Streamlit app** (`app/streamlit_app.py`) with KPIs (MTD spend, monthly forecast), trends, and top departments.
- **Monitor view** (`models/monitors/monitor_freshness_check.sql`) to surface latest metering hour.

### Changed
- N/A
