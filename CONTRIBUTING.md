# Contributing

Local development and integration notes for `finops-dbt`.

## Local dev

1. Create a Python virtual environment and install dependencies:

```bash
py -m venv .venv
source .venv/bin/activate        # Git Bash on Windows
# .venv\Scripts\Activate.ps1     # PowerShell alternative
pip install -r requirements.txt
```

2. For local Pro integration, create a `packages.local.yml` at the repo root pointing to your local `snowflake-finops-pro` checkout (this file is gitignored):

```yaml
packages:
  - local: ../snowflake-finops-pro
```

Then run:

```bash
dbt deps --profiles-dir .ci/profiles
```

3. Feature flags

The starter includes a stub model guarded by the var `enable_pro_pack`. To enable Pro models in local runs:

```bash
dbt build --vars '{"enable_pro_pack": true}' --profiles-dir .ci/profiles
```

## CI notes

- The `snowflake-finops-pro` repository contains a GitHub Actions workflow that checks out this repo and runs an integration pass on Pro PRs. It requires a `FINOPS_DBT_READ_TOKEN` secret in the Pro repo.
- Keep `packages.yml` pinned to semver tags for production.

## API manifest

`PRO_PUBLIC_API.yml` is generated within `snowflake-finops-pro` and lists models/macros considered part of the public contract. API changes should update that file.
