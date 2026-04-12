# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.0.0] - 2026-04-26
### Added
- **Storage costs:** `stg_storage_usage` + `fct_daily_storage_costs` surfaces storage spend from `ACCOUNT_USAGE.STORAGE_USAGE`.
- **Cost forecast:** `fct_cost_forecast` adds a 30-day warehouse-level projection using rolling average, trend, day-of-week seasonality, and confidence bands.
- **Total cost summary:** `fct_total_cost_summary` rolls compute and storage costs into one daily cost category table.
- **Top spenders:** `int_top_spenders` + `fct_top_spenders` adds a ranked user leaderboard by query volume with optional Pro cost estimates.
- **Warehouse dimension:** `dim_warehouse` adds warehouse metadata from metering and query history for fresh-account compatibility.
- New dbt_project vars: `storage_cost_per_tb_per_month`, `forecast_lookback_days`, `storage_history_days`.
- STORAGE_USAGE added to Starter sources.
- Schema tests for all 8 new models.
- Streamlit dashboard: total cost breakdown, forecast chart, storage costs section, and top users leaderboard.
- Forecast KPI now uses the dbt model instead of naive MTD run-rate extrapolation.
- `generate_budget_seed.py` script for refreshing budget seed dates.
- `scripts/bootstrap_snowflake.sql` for idempotent fresh-account setup.
- `scripts/generate_demo_workload.sql` for representative Account Usage activity.
- `docs/validation_2026-04-26.md` scaffold for live Snowflake validation evidence.

### Changed
- Version bumped to 3.0.0.
- Exposure maturity upgraded from `medium` to `high`.
- Budget seed regenerated with current-dated rows.
- `make demo` now uses the committed `.ci/profiles` demo target.
- CI source freshness is warn-only until the trial account has baseline Account Usage data.
- dbt 1.11 deprecation warnings removed from project, source, and generic test configs.

### Removed
- `models/stubs/pro_stub.sql` empty placeholder.

---

## [2.0.0] - 2026-03-16
### Added
- Test coverage for all Starter models.
- PRO API validator workflow (`validate-pro-api.yml`).
- Fast CI seed for offline testing.

### Changed
- Version reset to 2.0.0.
- Bumped dbt-snowflake to ~1.11, snowflake-connector-python to 4.x, Python to 3.13.
- CI workflows: actions bumped to v6; Snowflake-dependent steps gated on secrets.
- Renamed `pro_enabled` to `enable_pro_pack` across all models and configuration.

### Removed
- `fct_cost_trend.sql` redundant rollup.
- `sources_backlog.yml` aspirational, unmodeled sources.
- `monitor_freshness_check.sql` replaced by source freshness config.
- `packages.local.yml` from version control.

---

## [1.4.0] - 2025-09-18
### Added
- PR CI uploads a fork-safe dbt docs artifact and enforces Account Usage freshness errors on live runs.

### Changed
- Streamlit app gates Pro tiles behind the licensed flag, surfaces Budget/Variance/Freshness KPIs with safe fallbacks, and clarifies diagnostics context.
- README/.env onboarding spells out Starter vs licensed Pro configuration.

---

## [1.3.0] - 2025-09-17
### Added
- Shared `ntz_hour` macro to normalize hour bucketing to `timestamp_ntz` across staging and intermediate models.

### Changed
- Normalized warehouse metering and query staging to derive `usage_hour_ntz` from NTZ-cast timestamps.
- Applied hour-level incremental watermarks with a one-hour lookback to staging and `int_hourly_compute_costs`.
- Updated CI profile defaults to honor `DBT_TARGET`/`DBT_THREADS` env vars.

---

## [1.2.0] - 2025-09-16
### Added
- Department-aware budget vs actual mart.
- Account Usage freshness thresholds.
- Schema tests for key models.

---

## [1.1.0] - 2025-09-12
### Added
- dbt Core project with layered staging, intermediate, and marts models.
- Authoritative daily spend marts.
- Demo Mode via seeds and a `DEMO` schema overlay.
- Docs and lineage publishing using GitHub Actions.
- Exposure for the Streamlit app.
- Streamlit app with KPIs, trends, and top departments.
- Monitor view to surface latest metering hour.
