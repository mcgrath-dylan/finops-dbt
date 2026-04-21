---
title: "We cut our Snowflake bill 30% — here's the dbt project that shows you where the money goes"
date: 2026-04-xx
canonical_url:
tags: [snowflake, dbt, finops, data-engineering]
cross_posted_to: []
---

<!-- TODO(dylan): verify the 30% number against whatever the current demo seed projects; if it's lower, update the title -->
<!-- TODO(dylan): rewrite prose for voice; see "Voice constraints" in spec. Strip any AI-template patterns Codex left in. -->

On a mid-market Snowflake account, compute + storage typically hides 25-40% in waste: idle warehouses, over-sized clusters, forgotten tables. Here's the dbt project I built to surface every dollar.

The 30% number here comes from the demo account projection, not a customer case study. I do not have a heroic before-and-after story to sell. What I have is a repeatable dbt project that takes Snowflake's raw usage views and turns them into cost marts a data team can inspect, test, and ship into a dashboard.

## The problem

Snowflake cost analysis gets messy because the bill is split across several grains. Warehouse metering tells you compute and cloud services by hour. Query history tells you who ran work, but not the exact invoice line. Database storage history tells you what is sitting around, while serverless and auto-clustering costs sit in their own corners. Each view is useful. None of them is the model a finance partner or analytics engineering manager wants to read.

Most teams start with one dimension because it is the easiest place to begin. A warehouse cost chart is better than nothing, but it misses department ownership. A query leaderboard is useful, but it can turn into blame without budget context. A budget table catches overspend, but it does not explain whether the issue was storage growth, idle compute, or a forecast that drifted.

I built finops-dbt around that gap. The [features table in the README](https://github.com/mcgrath-dylan/finops-dbt#features) shows the coverage: daily compute spend, daily storage spend, department showback, budget variance, forecasts, total cost summary, warehouse metadata, and top spenders by query volume. The Starter project is open source. The Pro add-on exists for deeper query attribution and optimization recommendations, but the base package is enough to make the bill readable.

## What the dashboard shows

<!-- TODO(dylan): swap to real-data screenshots once workload script has populated ACCOUNT_USAGE -->

![Daily KPI dashboard](https://raw.githubusercontent.com/mcgrath-dylan/finops-dbt/main/app/screenshots/hero.png)

The first screen is the one I wanted when I started: month-to-date spend, forecast, idle waste, budget, and variance in one place. The point is not to make a prettier cloud bill. The point is to make the next question obvious.

If the month-to-date number is low but the budget is flat, I know I am looking at a data freshness issue, not a miracle. If the forecast is outrunning the budget by day ten, I can check whether the slope is coming from one warehouse or broad usage growth. If idle waste is material, I can decide whether the next move is autosuspend policy, warehouse sizing, or a conversation with the owning team.

![Spend by department with budget overlay](https://raw.githubusercontent.com/mcgrath-dylan/finops-dbt/main/app/screenshots/spend_by_department.png)

The department view is where the project starts acting like a showback system. It maps warehouses to departments, then rolls spend up by day. That mapping is deliberately simple: a seed file with `warehouse_name` and `department`. I wanted something a team could change in a pull request without waiting on a platform migration.

The budget overlay matters because raw cost rankings are rarely enough. Finance might care that Business Intelligence spent more than Data Science last week, but the more useful question is whether each department is tracking against its own plan. A steady $800/week team can be healthy. A $300/week team can be in trouble if its budget was $120.

![Top departments and warehouses ranked by cost](https://raw.githubusercontent.com/mcgrath-dylan/finops-dbt/main/app/screenshots/top_tables.png)

The top tables are intentionally boring. They rank departments and warehouses over the selected window and show share of spend. I use them as a triage surface. If a forecast moves, I check the warehouse table. If the warehouse table points to a shared transform warehouse, I jump to department showback. If the department view points to one owner, I can inspect query volume and runtime before turning it into a ticket.

That workflow is why this is in dbt instead of only in a Streamlit script. The dashboard is the surface area. The useful part is that every number comes from a model with tests, lineage, and contracts.

I also wanted the dashboard to be honest about missing data. Fresh Snowflake accounts make this awkward because `ACCOUNT_USAGE` does not populate all views at the same speed. A green dbt build can still produce a weak screenshot if the current-month window has budget rows but no metering rows yet. The app now calls that out instead of making a flat zero chart look like a business result.

Storage has the same issue. A small dev account can round to $0 at the default TB/month rate, and internal stage storage is account-level in Snowflake. I would rather show a boring no-data state than allocate stage bytes across databases with a made-up rule. That choice keeps the model explainable when someone clicks from the dashboard into the dbt docs.

## How it works

The staging layer normalizes raw Snowflake views into stable grains. `stg_warehouse_metering` turns `WAREHOUSE_METERING_HISTORY` into hourly warehouse cost. `stg_query_history` filters successful compute-bearing statements. `stg_storage_usage` reads `DATABASE_STORAGE_USAGE_HISTORY` and converts bytes to daily estimated dollars.

The intermediate layer joins activity to metering. `int_hourly_compute_costs` is the bridge between hourly spend and query activity, including the idle-compute signal. `int_top_spenders` rolls query activity to user, role, database, warehouse, and day so the dashboard can rank usage without pretending Starter has exact query-level cost attribution.

The marts are the contract the dashboard reads. `fct_daily_costs` is warehouse-by-day compute cost. `fct_daily_storage_costs` is database-by-day storage cost. `fct_cost_by_department` and `fct_budget_vs_actual` power showback. `fct_cost_forecast` projects the month with rolling average, trend, and day-of-week seasonality.

The full lineage is in the [README architecture diagram](https://github.com/mcgrath-dylan/finops-dbt#architecture), and the generated dbt docs are published at [mcgrath-dylan.github.io/finops-dbt/base/](https://mcgrath-dylan.github.io/finops-dbt/base/).

## Quickstart

The demo path uses seeded Account Usage overlays, so you can validate the model graph before a fresh Snowflake account has enough real usage history.

```bash
git clone https://github.com/mcgrath-dylan/finops-dbt.git
cd finops-dbt
pip install -r requirements.txt
dbt seed --profiles-dir .ci/profiles --target demo
dbt build --profiles-dir .ci/profiles --target demo --vars '{"DEMO_MODE": true, "enable_pro_pack": false}'
```

Then run the dashboard:

```bash
streamlit run app/streamlit_app.py
```

## What's next

This is post #1 in the content plan. Post #2 will go into storage cost, especially the difference between database storage, fail-safe, and internal stage usage. That area looks simple until you try to allocate it cleanly by owner.

Post #3 will cover the forecast model. I kept it in dbt on purpose: rolling averages, trend slope, and day-of-week seasonality are easier to explain and test than a black-box model for this use case.

The Pro add-on exists for teams that need query-level attribution and optimization recommendations. I am not treating that as the main story here. The open-source package is the piece I expect most analytics engineers to inspect first.

## How this was built

<!-- TODO(dylan): draft this paragraph via the interview in docs/distribution_checklist.md §AI-disclosure-draft. Cover: rough timeline from first commit to v3.0.0, which AI tools and in what split, one thing AI accelerated, one modeling/methodology decision that was yours, one Snowflake gotcha AI couldn't have predicted. Matter-of-fact, first person, no apology, no overclaiming. -->

## Footer

Repo: [github.com/mcgrath-dylan/finops-dbt](https://github.com/mcgrath-dylan/finops-dbt)

License: Apache-2.0

Contact: mcgrath.fintech@gmail.com
