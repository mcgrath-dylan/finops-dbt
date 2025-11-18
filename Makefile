.PHONY: demo live docs

## Run demo (seed + build + app)
demo:
	dbt deps && dbt seed && dbt build && streamlit run app/streamlit_app.py

## Build against live Snowflake
live:
	dbt deps && dbt build

## Generate and serve docs locally
docs:
	dbt deps && dbt docs generate && dbt docs serve
