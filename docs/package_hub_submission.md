# dbt Package Hub Submission Checklist

## Pre-flight

- [ ] v3.0.0 tagged and pushed to GitHub (done 2026-04-12)
- [ ] `dbt_project.yml` has `name`, `version`, `require-dbt-version` set
- [ ] README badges render on hub.getdbt.com (check after listing is live)

## Fork and PR

1. Fork `dbt-labs/hubcap`
2. Edit `hub.json` - add under the appropriate section:

   ```json
   {
     "mcgrath-dylan": [
       "finops-dbt"
     ]
   }
   ```

3. Commit: `feat: add mcgrath-dylan/finops-dbt`
4. PR title: `Add mcgrath-dylan/finops-dbt`
5. PR body:

   > Adds [finops-dbt](https://github.com/mcgrath-dylan/finops-dbt), a Snowflake
   > cost analytics dbt package covering compute, storage, forecasting, department
   > showback, budget variance, and top-spender attribution. Apache-2.0.
   >
   > - First release: v3.0.0 (2026-04-12)
   > - dbt-core: >=1.8.0, <2.0.0
   > - Adapter: dbt-snowflake
   > - Docs: https://mcgrath-dylan.github.io/finops-dbt/base/
   > - License: Apache-2.0

## Post-merge

- [ ] Verify listing appears at https://hub.getdbt.com/mcgrath-dylan/finops-dbt/
- [ ] Update finops-dbt README with "Install from dbt Hub" quickstart
- [ ] Post to dbt Slack #package-ecosystem
