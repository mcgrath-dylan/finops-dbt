.PHONY: demo live docs

DBT_FLAGS := --profiles-dir .ci/profiles

## Run demo (seed + build + app)
demo:
	dbt deps $(DBT_FLAGS) --target demo && dbt seed $(DBT_FLAGS) --target demo && dbt build $(DBT_FLAGS) --target demo --vars '{"DEMO_MODE": true, "enable_pro_pack": false}' && streamlit run app/streamlit_app.py

## Build against live Snowflake
live:
	dbt deps $(DBT_FLAGS) --target live && dbt build $(DBT_FLAGS) --target live

## Generate and serve docs locally
docs:
	dbt deps $(DBT_FLAGS) && dbt docs generate $(DBT_FLAGS) && dbt docs serve $(DBT_FLAGS)
