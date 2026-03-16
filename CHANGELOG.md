# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-03-16
### Added
- Test coverage for all Starter models (32 tests across 7 models).
- PRO API validator workflow (`validate-pro-api.yml`).
- Fast CI seed for offline testing.

### Changed
- Version reset to 2.0.0.
- Bumped dbt-snowflake to ~1.11, snowflake-connector-python to 4.x, Python to 3.13.
- CI workflows: actions bumped to v6; Snowflake-dependent steps gated on secrets.
- Renamed `pro_enabled` to `enable_pro_pack` across all models and configuration.

### Removed
- `fct_cost_trend.sql` (redundant GROUP BY on fct_daily_costs).
- `sources_backlog.yml` (aspirational, unmodeled sources).
- `monitor_freshness_check.sql` (replaced by source freshness config).
- `packages.local.yml` from version control (gitignored for local use).

---

## [1.4.0] - 2025-09-18
### Added
- PR CI uploads a fork-safe dbt docs artifact (manifest/catalog/index) and enforces Account Usage freshness errors (>96h) on live runs.

### Changed
- Streamlit app now gates Pro tiles behind the licensed flag, surfaces Budget/Variance/Freshness KPIs with safe fallbacks, and clarifies diagnostics context.
- README/.env onboarding spells out Starter vs. licensed Pro configuration so the add-on stays gated.

---

## [1.3.0] - 2025-09-17
### Added
- Shared `ntz_hour` macro to normalize hour bucketing to `timestamp_ntz` across staging and intermediate models.

### Changed
- Normalized warehouse metering and query staging to derive `usage_hour_ntz` from NTZ-cast timestamps with usage dates sourced from that hour.
- Applied hour-level incremental watermarks with a one-hour lookback to staging and `int_hourly_compute_costs` to keep reruns idempotent while preserving surrogate key uniqueness.
- Updated CI profile defaults to honor `DBT_TARGET`/`DBT_THREADS` env vars while defaulting to four threads for demo/dev/live targets.

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
