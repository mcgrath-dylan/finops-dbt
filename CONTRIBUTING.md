# Contributing — Local development and integration notes

This file documents the minimum steps to develop `finops-dbt` locally and to integrate with the private `snowflake-finops-pro` repository.

Local dev (fast loop using a local pro copy)

1. Create a Python virtual environment and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Use the provided `packages.local.yml` to point `finops-dbt` at your local `snowflake-finops-pro` copy:

```powershell
# packages.local.yml exists at repository root and points to a local absolute path
dbt deps --profiles-dir .ci/profiles
```

3. Feature flags

The starter includes a small stub model guarded by the var `pro_enabled`. To enable pro-only models in local runs:

```powershell
dbt build --vars "{\"pro_enabled\": true}" --profiles-dir .ci/profiles
```

CI notes for maintainers

- The `snowflake-finops-pro` repository contains a GitHub Actions workflow that checks out this repo and runs an integration pass on Pro PRs. To enable it you must provide a repository read token as the repo secret `FINOPS_DBT_READ_TOKEN` in the Pro repo.
- Keep `packages.yml` pinned to semver tags for production; use `packages.local.yml` only for local development.

API manifest

`PRO_PUBLIC_API.yml` is generated within `snowflake-finops-pro` and lists models/macros that are considered part of the public contract. API changes should update that file and be reviewed by the PM.
