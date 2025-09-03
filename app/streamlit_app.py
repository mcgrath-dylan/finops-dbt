import os
import pandas as pd
import streamlit as st
from contextlib import contextmanager

# --- Connection helpers (read-only) ---
def _env(name, default=None, required=False):
    val = os.getenv(name, default)
    if required and not val:
        st.error(f"Missing required environment variable: {name}")
        st.stop()
    return val

@contextmanager
def snowflake_conn():
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=_env("SNOWFLAKE_ACCOUNT", required=True),
        user=_env("SNOWFLAKE_USER", required=True),
        password=_env("SNOWFLAKE_PASSWORD"),  # not needed if using external auth
        authenticator=_env("SNOWFLAKE_AUTHENTICATOR", "snowflake"),  # e.g., 'externalbrowser'
        role=_env("SNOWFLAKE_ROLE"),
        warehouse=_env("SNOWFLAKE_WAREHOUSE", required=True),
    )
    try:
        db = _env("SNOWFLAKE_DATABASE", required=True)
        schema = _env("SNOWFLAKE_SCHEMA", required=True)
        conn.cursor().execute(f'USE DATABASE "{db}"')
        conn.cursor().execute(f'USE SCHEMA "{schema}"')
        yield conn
    finally:
        conn.close()

def df(q, params=None):
    """Run a query and return a DataFrame with lower-cased column names."""
    with snowflake_conn() as c:
        cur = c.cursor()
        cur.execute(q, params or {})
        rows = cur.fetchall()
        cols = [d[0].lower() for d in cur.description]  # normalize to lower-case
        return pd.DataFrame(rows, columns=cols)

# --- UI ---
st.set_page_config(page_title="FinOps Starter – Cost Overview", layout="wide")
st.title("FinOps Starter (Snowflake + dbt) – Read-Only Demo")

st.caption(
    "Authoritative $ from WAREHOUSE_METERING_HISTORY. Query-level $ are ESTIMATES. "
    "Optimization outputs list candidates, not savings."
)

# Controls
days = st.slider("Window (days)", min_value=7, max_value=60, value=30, step=1)
topn = st.slider("Top N (departments/warehouses)", 5, 25, 10, 1)

# --- Cards: Totals ---
colA, colB, colC = st.columns(3)
totals_sql = """
select 
  sum(compute_cost) as total_compute_usd,
  sum(idle_cost)    as total_idle_usd,
  sum(compute_cost + idle_cost) as total_usd
from fct_daily_costs
where usage_date >= dateadd(day, -%(days)s, current_date())
"""
totals = df(totals_sql, {"days": days}).fillna(0)

if totals.empty:
    st.warning(
        "No rows found in fct_daily_costs for the selected window/schema. "
        "Rebuild dbt or verify SNOWFLAKE_* env vars."
    )
else:
    colA.metric("Compute $ (window)", f"${totals.total_compute_usd.iloc[0]:,.0f}")
    colB.metric("Idle $ (window)",    f"${totals.total_idle_usd.iloc[0]:,.0f}")
    colC.metric("Total $ (window)",   f"${totals.total_usd.iloc[0]:,.0f}")

st.divider()

# --- Trend by department ---
st.subheader("Trend by department (daily)")
trend_sql = """
select department, usage_date, total_cost_usd
from cost_by_department
where usage_date >= dateadd(day, -%(days)s, current_date())
order by usage_date
"""
trend = df(trend_sql, {"days": days})
if trend.empty:
    st.info("No department data found. Load seeds and rebuild marts.")
else:
    pivot = trend.pivot_table(
        index="usage_date",
        columns="department",
        values="total_cost_usd",
        aggfunc="sum"
    ).fillna(0)
    st.line_chart(pivot)

st.divider()

# --- Top N departments (window sum) ---
st.subheader("Top departments by cost")
top_dept_sql = """
select department, sum(total_cost_usd) as window_cost_usd
from cost_by_department
where usage_date >= dateadd(day, -%(days)s, current_date())
group by department
order by window_cost_usd desc
limit %(topn)s
"""
st.dataframe(df(top_dept_sql, {"days": days, "topn": int(topn)}))

# --- Top N warehouses (window sum) ---
st.subheader("Top warehouses by cost")
top_wh_sql = """
select warehouse_name, sum(compute_cost + idle_cost) as window_cost_usd
from fct_daily_costs
where usage_date >= dateadd(day, -%(days)s, current_date())
group by warehouse_name
order by window_cost_usd desc
limit %(topn)s
"""
st.dataframe(df(top_wh_sql, {"days": days, "topn": int(topn)}))

st.divider()

# --- Pro Pack candidates (if enabled) ---
st.subheader("Pro Pack candidates (if enabled)")

# Robust existence probe: returns at most one row if table *or* view exists
pro_exists_sql = """
select 1 as has_model
from information_schema.tables
where table_schema = current_schema()
  and table_name = 'INT_WAREHOUSE_OPTIMIZATION'
union all
select 1 as has_model
from information_schema.views
where table_schema = current_schema()
  and table_name = 'INT_WAREHOUSE_OPTIMIZATION'
limit 1
"""
exists_df = df(pro_exists_sql)

if not exists_df.empty:
    safe_topn = int(topn)
    pro_sql = f"""
    select warehouse_name, 
           round(avg_daily_cost,2) as avg_daily_cost,
           idle_cost_percentage, 
           recommended_auto_suspend_seconds,
           estimated_monthly_idle_cost_usd,
           optimization_priority
    from int_warehouse_optimization
    order by optimization_priority desc, estimated_monthly_idle_cost_usd desc
    limit {safe_topn}
    """
    try:
        st.dataframe(df(pro_sql))
    except Exception:
        st.info(
            "Pro Pack appears present but not readable in this schema/role, or columns differ. "
            "Rebuild with Pro enabled in this schema, or switch role/schema."
        )
else:
    st.caption("Pro Pack is disabled or models not found in this schema.")
