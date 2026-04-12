# Contributing

## Run Locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
dbt deps --profiles-dir .ci/profiles --target demo
dbt parse --profiles-dir .ci/profiles --target demo
dbt build --profiles-dir .ci/profiles --target demo --vars '{"DEMO_MODE": true, "enable_pro_pack": false}'
dbt test --profiles-dir .ci/profiles --target demo --vars '{"DEMO_MODE": true, "enable_pro_pack": false}'
```

`make demo` uses the same committed demo profile and launches Streamlit after a successful build.

## Add A Model

- Put staging models in `models/staging`, intermediate models in `models/intermediate`, and marts in `models/marts`.
- Add model and column metadata in `models/schema.yml`.
- Add at least one uniqueness or grain test, required `not_null` tests, and one data quality assertion where the model produces measures.
- Use `DEMO_MODE` overlays or seeds when live `ACCOUNT_USAGE` data would make tests brittle.

## CI Expectations

Every PR must pass dbt parse. Snowflake-backed compile, build, test, and freshness checks run when repository secrets are configured. Source freshness is warn-only until the fresh trial account has baseline data.

## Pull Requests

Keep changes scoped to one feature or fix. Do not commit `.env`, local profiles, generated `target/` artifacts, or private package overrides. Note any live Snowflake validation gaps in the PR description.
