# app/streamlit_app.py
import os
import calendar
import datetime as dt
from typing import Optional, Dict, List

# Load .env so flags like ENABLE_PRO_PACK are available to the app
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import numpy as np
import pandas as pd
import streamlit as st

try:
    import plotly.express as px
    PLOTLY = True
except Exception:
    PLOTLY = False

try:
    import snowflake.connector as sf
except Exception:
    sf = None  # type: ignore

# -------- helpers -----------------------------------------------------------
def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v is not None and v != "" else float(default)
    except Exception:
        return float(default)

DEV_MODE = env_bool("DEV_MODE", False)
CREDIT_PRICE = env_float("CREDIT_PRICE_USD", 3.0)

st.set_page_config(
    page_title="FinOps Starter – Cost Overview",
    layout="wide",
    menu_items={"Get Help": None, "Report a bug": None, "About": None},
)

DOCS_URL = os.getenv("FINOPS_DOCS_URL", "https://mcgrath-dylan.github.io/finops-dbt/")
REPO_URL = os.getenv("FINOPS_REPO_URL", "https://github.com/mcgrath-dylan/finops-dbt")
PRO_DATABASE = (os.getenv("PRO_DATABASE") or "").strip()
PRO_SCHEMA = (os.getenv("PRO_SCHEMA") or "").strip()
PRO_PACK_FLAG = env_bool("ENABLE_PRO_PACK", False)

def fmt_usd(x: Optional[float], decimals: int = 0) -> str:
    if x is None:
        return "—"
    try:
        if np.isnan(x) or np.isinf(x):
            return "—"
    except Exception:
        pass
    return f"${x:,.{decimals}f}" if decimals else f"${x:,.0f}"

def lc(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [str(c).lower() for c in out.columns]
    return out

def to_float(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            try:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
            except Exception:
                try:
                    df[c] = df[c].astype(float)
                except Exception:
                    pass
    return df

def dim_count(d: dt.date) -> int:
    return calendar.monthrange(d.year, d.month)[1]

def clear_all_caches():
    try:
        st.cache_data.clear()
    except Exception:
        pass
    try:
        st.cache_resource.clear()
    except Exception:
        pass
    try:
        for k in list(st.session_state.keys()):
            if str(k).startswith("sf_conn::"):
                conn = st.session_state.get(k)
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass
                try:
                    del st.session_state[k]
                except Exception:
                    pass
    except Exception:
        pass

def humanize_header(key_col: str) -> str:
    return (
        "Warehouse"
        if key_col == "warehouse_name"
        else ("Department" if key_col == "department" else key_col.replace("_", " ").title())
    )

# -------- Snowflake ---------------------------------------------------------
@st.cache_data(show_spinner=False)
def get_conn_params() -> Dict[str, str]:
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
        "user": os.getenv("SNOWFLAKE_USER", ""),
        "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
        "role": os.getenv("SNOWFLAKE_ROLE", ""),
        "database": os.getenv("SNOWFLAKE_DATABASE", ""),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", ""),
    }

def active_schema(demo: bool) -> str:
    return "DEMO" if demo else (get_conn_params().get("schema", "") or "PUBLIC")

def _raw_connect():
    cp = get_conn_params()
    if sf is None:
        return None
    try:
        return sf.connect(
            account=cp["account"],
            user=cp["user"],
            password=cp["password"],
            warehouse=cp["warehouse"],
            role=cp["role"],
            database=cp["database"],
            schema=cp["schema"],
        )
    except Exception:
        return None

def connect():
    cp = get_conn_params()
    key = f"sf_conn::{cp.get('account','')}::{cp.get('user','')}::{cp.get('database','')}::{cp.get('schema','')}"
    conn = st.session_state.get(key)
    try:
        if conn is not None and hasattr(conn, "is_closed") and not conn.is_closed():
            return conn
    except Exception:
        pass
    conn = _raw_connect()
    st.session_state[key] = conn
    return conn

@st.cache_data(show_spinner=False)
def run_query(sql: str, cache_key: Optional[str] = None) -> pd.DataFrame:
    _ = cache_key
    if sf is None:
        return pd.DataFrame()
    conn = connect()
    if conn is None:
        return pd.DataFrame()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description] if cur.description else []
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass

def table_exists(database: str, schema: str, table: str) -> bool:
    if not database or not schema or not table:
        return False
    q = f"""
        select 1
        from {database}.information_schema.tables
        where lower(table_schema) = lower('{schema}')
          and lower(table_name)   = lower('{table}')
        limit 1
    """
    try:
        df = lc(run_query(q, cache_key=f"exists:{database}.{schema}.{table}".lower()))
        return not df.empty
    except Exception:
        return False

# -------- data loads --------------------------------------------------------
@st.cache_data(ttl=60, show_spinner=False)
def load_models(demo: bool, lookback_days: int):
    cp = get_conn_params()
    db = cp["database"]
    sch = active_schema(demo)
    lb = max(lookback_days * 2 + 7, 14)

    fct = lc(
        run_query(
            f"""
        select usage_date, warehouse_name, compute_cost, cloud_services_cost,
               total_cost, idle_cost, _loaded_at
        from {db}.{sch}.fct_daily_costs
        where usage_date >= dateadd(day, -{lb}, current_date())
        order by usage_date
    """,
            cache_key=f"fct:{db}.{sch}:{lb}",
        )
    )
    if "usage_date" in fct.columns:
        fct["usage_date"] = pd.to_datetime(fct["usage_date"]).dt.date
    fct = to_float(fct, ["compute_cost", "cloud_services_cost", "total_cost", "idle_cost"])

    dept = lc(
        run_query(
            f"""
        select department, usage_date, total_cost_usd
        from {db}.{sch}.fct_cost_by_department
        where usage_date >= dateadd(day, -{lb}, current_date())
        order by usage_date
    """,
            cache_key=f"dept:{db}.{sch}:{lb}",
        )
    )
    if "usage_date" in dept.columns:
        dept["usage_date"] = pd.to_datetime(dept["usage_date"]).dt.date
    dept = to_float(dept, ["total_cost_usd"])

    # Fallback dept derivation (if mart empty) using a local mapping seed
    if (dept is None or dept.empty) and (fct is not None and not fct.empty):
        mapping = None
        for p in (
            "seeds/department_mapping.csv",
            os.path.join("app", "department_mapping.csv"),
            "/mnt/data/department_mapping.csv",
            "department_mapping.csv",
        ):
            if os.path.exists(p):
                try:
                    _m = pd.read_csv(p)
                    cols = {c.lower(): c for c in _m.columns}
                    wn = cols.get("warehouse_name")
                    dp = cols.get("department")
                    if wn and dp:
                        mapping = _m[[wn, dp]].copy()
                        mapping.columns = ["warehouse_name", "department"]
                        mapping["warehouse_name"] = mapping["warehouse_name"].astype(str).str.upper()
                        break
                except Exception:
                    pass
        tmp = fct.copy()
        for col in ("compute_cost", "idle_cost"):
            if col not in tmp.columns:
                tmp[col] = 0.0
        tmp["total_cost_usd"] = tmp["compute_cost"].fillna(0) + tmp["idle_cost"].fillna(0)
        if "warehouse_name" in tmp.columns:
            tmp["warehouse_name"] = tmp["warehouse_name"].astype(str).str.upper()
        if mapping is not None:
            tmp = tmp.merge(mapping, on="warehouse_name", how="left")
            tmp["department"] = tmp["department"].fillna("Unassigned")
        else:
            tmp["department"] = "Unassigned"
        if "usage_date" in tmp.columns:
            tmp["usage_date"] = pd.to_datetime(tmp["usage_date"]).dt.date
            dept = (
                tmp.groupby(["department", "usage_date"], as_index=False)["total_cost_usd"]
                .sum()
                .sort_values(["usage_date", "department"])
            )
        else:
            dept = pd.DataFrame(columns=["department", "usage_date", "total_cost_usd"])

    AU_DB = os.getenv("ACCOUNT_USAGE_DATABASE", "SNOWFLAKE")
    AU_SCHEMA = os.getenv("ACCOUNT_USAGE_SCHEMA", "ACCOUNT_USAGE")

    fresh = pd.DataFrame()
    try:
        if not demo:  # only probe warehouse metering in Live
            fresh = lc(
                run_query(
                    f"select max(END_TIME) as last_end_time from {AU_DB}.{AU_SCHEMA}.WAREHOUSE_METERING_HISTORY",
                    cache_key=f"fresh:{AU_DB}.{AU_SCHEMA}",
                )
            )
    except Exception:
        fresh = pd.DataFrame(columns=["last_end_time"])

    return fct, dept, fresh

@st.cache_data(ttl=60, show_spinner=False)
def load_budget(demo: bool) -> pd.DataFrame:
    cp = get_conn_params()
    db = cp.get("database", "")
    sch = active_schema(demo)
    if sf is not None and db and sch:
        try:
            live = lc(
                run_query(
                    f"select date, department, budget_usd from {db}.{sch}.budget_daily",
                    cache_key=f"budget:{db}.{sch}",
                )
            )
            if not live.empty:
                live["date"] = pd.to_datetime(live["date"]).dt.date
                live = to_float(live, ["budget_usd"])
                live["department"] = live["department"].astype(str).str.strip()
                required = {"date", "department", "budget_usd"}
                if required.issubset(set(live.columns)):
                    return live[["date", "department", "budget_usd"]]
        except Exception:
            pass

    # CSV fallback(s)
    for p in (
        "seeds/budget_daily.csv",
        "budget_daily.csv",
        os.path.join("app", "budget_daily.csv"),
        "/mnt/data/budget_daily.csv",
    ):
        if os.path.exists(p):
            try:
                raw = pd.read_csv(p)
                cols = {c.lower(): c for c in raw.columns}
                out = raw[[cols["date"], cols["department"], cols["budget_usd"]]].copy()
                out.columns = ["date", "department", "budget_usd"]
                out["date"] = pd.to_datetime(out["date"]).dt.date
                out["department"] = out["department"].astype(str).str.strip()
                out["budget_usd"] = pd.to_numeric(out["budget_usd"], errors="coerce").fillna(0.0).astype(float)
                return out
            except Exception:
                continue
    return pd.DataFrame(columns=["date", "department", "budget_usd"])

@st.cache_data(ttl=60, show_spinner=False)
def load_budget_vs_actual_latest(demo: bool) -> Optional[dt.date]:
    cp = get_conn_params()
    db = cp.get("database", "")
    sch = active_schema(demo)
    if sf is None or not db or not sch:
        return None
    try:
        latest = lc(
            run_query(
                f"select max(usage_date) as usage_date from {db}.{sch}.fct_budget_vs_actual",
                cache_key=f"bva_latest:{db}.{sch}",
            )
        )
        if not latest.empty and pd.notnull(latest.iloc[0].get("usage_date")):
            return pd.to_datetime(latest.iloc[0]["usage_date"]).date()
    except Exception:
        pass
    return None

@st.cache_data(show_spinner=False)
def load_current_warehouses():
    df = lc(run_query("show warehouses", cache_key="show_warehouses"))
    if df.empty:
        return {}
    name_col = "name" if "name" in df.columns else "NAME"
    auto_col = "auto_suspend" if "auto_suspend" in df.columns else ("AUTO_SUSPEND" if "AUTO_SUSPEND" in df.columns else None)
    if auto_col is None:
        return {}
    size_col = None
    for cand in ["size", "WAREHOUSE_SIZE", "warehouse_size"]:
        if cand in df.columns:
            size_col = cand
            break
    out = {}
    for _, r in df.iterrows():
        try:
            out[str(r[name_col]).upper()] = {
                "auto_suspend": int(r[auto_col]) if pd.notnull(r[auto_col]) else None,
                "size": str(r[size_col]).upper() if size_col and pd.notnull(r[size_col]) else None,
            }
        except Exception:
            out[str(r[name_col]).upper()] = {"auto_suspend": None, "size": None}
    return out

@st.cache_data(show_spinner=False)
def load_pro_hourly_soft(
    demo: bool,
    days: int,
    credit_threshold: float = 0.05,
    pro_db: Optional[str] = None,
    pro_schema: Optional[str] = None,
) -> pd.DataFrame:
    cp = get_conn_params()
    db = pro_db or cp["database"]
    sch = pro_schema or active_schema(demo)
    probe = lc(run_query(f"select * from {db}.{sch}.int_hourly_compute_costs limit 1", cache_key=f"probe_hourly:{db}.{sch}"))
    if probe.empty:
        return pd.DataFrame()
    has_size = "warehouse_size" in probe.columns or "WAREHOUSE_SIZE" in probe.columns
    size_select = "max(warehouse_size) as warehouse_size," if has_size else "null as warehouse_size,"

    q = f"""
        select
            warehouse_name,
            sum(case when is_potentially_idle = true and total_credits_used >= {credit_threshold}
                     then compute_cost_usd else 0 end) as idle_cost_adj,
            sum(total_cost_usd)  as total_cost,
            sum(compute_cost_usd) as compute_cost,
            count(*) as total_hours,
            sum(case when queries_executed > 0 then 1 else 0 end) as active_hours,
            sum(case when queries_executed > 0 then total_credits_used else 0 end) as credits_on_active_hours,
            {size_select}
            count(distinct usage_date) as total_days,
            count(distinct case when queries_executed > 0 then usage_date end) as active_days
        from {db}.{sch}.int_hourly_compute_costs
        where usage_date between dateadd(day, -{days-1}, current_date()) and current_date()
        group by 1
    """
    df = lc(run_query(q, cache_key=f"pro_hourly:{db}.{sch}:{days}:{credit_threshold}"))
    df = to_float(df, ["idle_cost_adj", "total_cost", "compute_cost", "total_hours", "active_hours", "credits_on_active_hours", "total_days", "active_days"])
    return df

# -------- styles ------------------------------------------------------------
st.markdown(
    """
<style>
/* hide Streamlit chrome */
[data-testid="stToolbar"] { visibility: hidden; height: 0; }
[data-testid="stDecoration"] { display: none; }

/* badges and cards */
.pill{display:inline-block;padding:6px 12px;border-radius:999px;font-weight:700;font-size:.85rem}
.pill-demo{background:#2d3748;color:#e2e8f0;border:1px solid #4a5568}
.pill-live{background:#234e52;color:#c6f6d5;border:1px solid #2c7a7b}

.kpi{border:1px solid rgba(250,250,250,.08);background:rgba(255,255,255,.03);border-radius:14px;padding:14px 16px}
.kpi-title{font-size:.85rem;color:#c9d1d9;margin-bottom:4px}
.kpi-value{font-size:1.4rem;font-weight:700}

/* kill any lingering progress/skeleton bars inside KPI cards */
.kpi [data-testid="stProgress"], .kpi div[role="progressbar"] { display:none !important; }
.muted{color:#9aa4af}
.section-help{float:right;opacity:.9}
</style>
""",
    unsafe_allow_html=True,
)

# -------- sidebar -----------------------------------------------------------
with st.sidebar:
    st.subheader("Environment")
    demo_mode = st.toggle("Demo Mode", True)
    if "last_demo" not in st.session_state:
        st.session_state.last_demo = demo_mode
    if st.session_state.last_demo != demo_mode:
        clear_all_caches()
        st.session_state.last_demo = demo_mode

    cp = get_conn_params()
    st.caption(
        f"**Context:** `{cp.get('database','—')}`.`{active_schema(demo_mode)}` • "
        f"**Role:** `{cp.get('role','—')}` • **Warehouse:** `{cp.get('warehouse','—')}`"
    )

    st.subheader("Controls")
    days_shown = st.slider("Days shown", 7, 90, 30, 1)
    rows_to_show = st.slider("Show up to N rows", 3, 15, 8, 1)

    if PRO_PACK_FLAG:
        enable_pro = st.toggle("Pro Insights", True)
    else:
        enable_pro = False
        st.caption("FinOps Pro add-on required; set `ENABLE_PRO_PACK=true` only after installing the licensed package.")

    st.caption("Controls affect tiles, charts, and tables.")
    if st.button("Clear app cache"):
        clear_all_caches()
        st.success("Caches cleared.")

# -------- header ------------------------------------------------------------
left, right = st.columns([2, 1])
with left:
    st.markdown("## FinOps for Snowflake + dbt")
    pill = '<span class="pill pill-demo">DEMO</span>' if demo_mode else '<span class="pill pill-live">LIVE</span>'
    st.markdown(pill, unsafe_allow_html=True)
with right:
    st.markdown(
        f'<div style="text-align:right"><a href="{DOCS_URL}" target="_blank">Docs &amp; lineage</a> &nbsp;•&nbsp; '
        f'<a href="{REPO_URL}" target="_blank">Repo</a></div>',
        unsafe_allow_html=True,
    )

# -------- data & metrics ----------------------------------------------------
fct, dept, fresh = load_models(demo_mode, days_shown)
budget = load_budget(demo_mode)
bva_latest = load_budget_vs_actual_latest(demo_mode)

today = dt.date.today()
first_day = today.replace(day=1)
dim = dim_count(today)
elapsed = (today - first_day).days + 1

mtd_fct = fct[(fct["usage_date"] >= first_day) & (fct["usage_date"] <= today)].copy() if not fct.empty else pd.DataFrame()
if not mtd_fct.empty and "total_cost" not in mtd_fct.columns and {"compute_cost", "idle_cost"} <= set(mtd_fct.columns):
    mtd_fct["total_cost"] = mtd_fct["compute_cost"].fillna(0) + mtd_fct["idle_cost"].fillna(0)
mtd_total = float(mtd_fct.get("total_cost", pd.Series([0.0])).sum()) if not mtd_fct.empty else 0.0
forecast_month = (mtd_total / max(elapsed, 1)) * dim if mtd_total > 0 else 0.0
forecast_month = max(forecast_month, mtd_total)

# Idle wasted (last N days) from Starter
if not fct.empty and "idle_cost" in fct.columns:
    start_win = today - dt.timedelta(days=days_shown - 1)
    idle_wasted_last_n = float(fct[(fct["usage_date"] >= start_win) & (fct["usage_date"] < today)]["idle_cost"].sum())
else:
    idle_wasted_last_n = None

budget_mtd = None
if not budget.empty and "date" in budget.columns:
    month_mask = (budget["date"] >= first_day) & (budget["date"] <= today)
    budget_mtd = float(budget.loc[month_mask, "budget_usd"].sum())

actual_mtd = None
if not dept.empty and "usage_date" in dept.columns:
    dept_mtd = dept[(dept["usage_date"] >= first_day) & (dept["usage_date"] <= today)]
    if not dept_mtd.empty:
        actual_mtd = float(dept_mtd.get("total_cost_usd", pd.Series([0.0])).sum())
if actual_mtd is None:
    actual_mtd = mtd_total

variance_value = None
variance_pct = None
if budget_mtd is not None:
    variance_value = actual_mtd - budget_mtd
    if budget_mtd > 0:
        variance_pct = (variance_value / budget_mtd) * 100.0

freshness_hours = None
if not fct.empty and "usage_date" in fct.columns:
    try:
        latest_usage_date = pd.to_datetime(fct["usage_date"], errors="coerce").dropna().max().date()
        delta = dt.datetime.utcnow() - dt.datetime.combine(latest_usage_date, dt.time())
        freshness_hours = max(delta.total_seconds() / 3600.0, 0.0)
    except Exception:
        pass

# Pro connectivity probe (only once)
pro_db = PRO_DATABASE or get_conn_params().get("database", "")
pro_schema = PRO_SCHEMA or active_schema(demo_mode)
pro_connected = table_exists(pro_db, pro_schema, "INT_HOURLY_COMPUTE_COSTS")

pro_hourly = (
    load_pro_hourly_soft(demo_mode, days_shown, pro_db=pro_db, pro_schema=pro_schema)
    if (PRO_PACK_FLAG and enable_pro and pro_connected)
    else pd.DataFrame()
)
total_idle_est = None
rightsizing_df = pd.DataFrame()
if PRO_PACK_FLAG and enable_pro and not pro_hourly.empty:
    dfp = pro_hourly.copy()
    dfp["idle_share"] = (100.0 * (dfp["idle_cost_adj"] / dfp["total_cost"].replace(0, np.nan))).clip(0, 100).fillna(0.0)
    dfp["idle_month_est"] = (dfp["idle_cost_adj"] / max(days_shown, 1)) * 30.0
    total_idle_est = float(dfp["idle_month_est"].sum())

    dfp["avg_credits_per_active_hour"] = np.where(
        dfp["active_hours"] > 0, dfp["credits_on_active_hours"] / dfp["active_hours"], np.nan
    )
    size_order = ["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE"]

    def next_size_down(sz: Optional[str]) -> Optional[str]:
        if not sz:
            return None
        s = str(sz).replace("-", "").upper()
        if s not in size_order:
            return None
        i = size_order.index(s)
        return "XSMALL" if i <= 1 else size_order[i - 1]

    def rightsize_reco(row) -> Optional[str]:
        size = str(row.get("warehouse_size") or "").upper().replace("-", "")
        avgc = row.get("avg_credits_per_active_hour")
        if not size or pd.isna(avgc):
            return None
        if size in ["MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE"] and float(avgc) < 0.15:
            down = next_size_down(size)
            return f"Right-size to {down.title()}" if down else None
        return None

    dfp["rightsize_suggestion"] = dfp.apply(rightsize_reco, axis=1)
    rightsizing_df = dfp[
        ["warehouse_name", "avg_credits_per_active_hour", "warehouse_size", "rightsize_suggestion"]
    ].copy()

warehouses_flagged: Optional[int] = None
estimated_savings: Optional[float] = None
if PRO_PACK_FLAG and enable_pro and not pro_hourly.empty:
    flagged = 0
    if not rightsizing_df.empty and "rightsize_suggestion" in rightsizing_df.columns:
        suggestions = rightsizing_df["rightsize_suggestion"].fillna("").astype(str).str.strip()
        flagged = int((suggestions != "").sum())
    warehouses_flagged = flagged
    if total_idle_est is not None:
        try:
            estimated_savings = max(float(total_idle_est), 0.0)
        except Exception:
            estimated_savings = None
    elif not pro_hourly.empty and "idle_cost_adj" in pro_hourly.columns:
        try:
            est = (pro_hourly["idle_cost_adj"].fillna(0.0).astype(float).sum() / max(days_shown, 1)) * 30.0
            estimated_savings = max(float(est), 0.0)
        except Exception:
            estimated_savings = None

# -------- stale-data banner (live only) ------------------------------------
if not demo_mode and freshness_hours is not None:
    if freshness_hours > 96:
        st.error(f"Data may be stale (~{freshness_hours:.1f}h since last update). Some metrics could be delayed.")
    elif freshness_hours > 48:
        st.warning(f"Data may be slightly stale (~{freshness_hours:.1f}h).")

# -------- KPI grid ----------------------------------------
row1 = st.columns(3)
row2 = st.columns(2)

def kpi(container, title, value, note=""):
    with container:
        st.markdown('<div class="kpi">', unsafe_allow_html=True)
        st.markdown(f'<div class="kpi-title">{title}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="kpi-value">{value}</div>', unsafe_allow_html=True)
        if note:
            st.caption(note)
        st.markdown("</div>", unsafe_allow_html=True)

kpi(row1[0], "Month-to-date Spend", fmt_usd(mtd_total), f"Through {today.strftime('%b %d')} of a {dim}-day month")
kpi(row1[1], "Forecast (month)", fmt_usd(forecast_month), "MTD run-rate × days in month")
with row1[2]:
    st.markdown('<div class="kpi">', unsafe_allow_html=True)
    st.markdown('<div class="kpi-title">Right-Sizing (est.)</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    flagged_display = "—" if warehouses_flagged is None else f"{warehouses_flagged}"
    est_display = fmt_usd(estimated_savings) if estimated_savings is not None else "—"
    col1.metric("Warehouses flagged", flagged_display)
    col2.metric("Est. monthly savings", est_display)
    credit_display = f"${CREDIT_PRICE:,.2f}"
    if credit_display.endswith(".00"):
        credit_display = credit_display[:-3]
    if warehouses_flagged is None and estimated_savings is None:
        caption = (
            f"Estimates assume CREDIT_PRICE_USD ≈ {credit_display} and stable workload hours. Install Pro for full heuristics."
        )
    else:
        caption = f"Based on Pro hourly heuristics @ {credit_display}/credit (30-day estimate)."
    st.caption(caption)
    st.markdown("</div>", unsafe_allow_html=True)
kpi(row2[0], f"Idle Wasted (last {days_shown} days)",
    fmt_usd(idle_wasted_last_n) if idle_wasted_last_n is not None else "—",
    "Sum of idle cost over the last N days")
variance_value_disp = "—" if variance_value is None else fmt_usd(variance_value)
variance_note = "Variance shown when budget exists"
if variance_value is not None and variance_pct is not None:
    variance_note = f"{variance_pct:+.0f}% vs budget"
elif variance_value is not None:
    variance_note = "vs actual spend"
try:
    if variance_value is not None:
        color = "#e11d48" if variance_value > 0 else "#10b981"  # red/green
        variance_value_disp = f"<span style='color:{color}'>{variance_value_disp}</span>"
except Exception:
    pass
kpi(row2[1], "Variance (MTD)", variance_value_disp, variance_note)

# Pro KPIs (if enabled)
row3 = st.columns(2)
kpi(row3[0], "Budget (MTD)", fmt_usd(budget_mtd), f"Sum through {today.strftime('%b %d')}")
kpi(
    row3[1],
    "Idle (projected, month)",
    fmt_usd(total_idle_est) if total_idle_est is not None else "—",
    (
        "From Pro hourly model (scaled)"
        if total_idle_est is not None
        else ("Pro enabled, connect PRO_DATABASE/PRO_SCHEMA to activate" if (PRO_PACK_FLAG and enable_pro) else "Turn on Pro insights to see this")
    ),
)

st.divider()

# -------- Spend by Department ----------------------------------------------
st.markdown(
    f'### Spend by Department <span class="section-help"><a href="{DOCS_URL}" target="_blank">ⓘ</a></span>',
    unsafe_allow_html=True,
)
deps = sorted(dept["department"].unique()) if ("department" in dept and not dept.empty) else []
sel_key = f"sel_depts_{active_schema(demo_mode)}"
default_sel: List[str] = st.session_state.get(sel_key, deps)
default_sel = [d for d in default_sel if d in deps] or deps
sel = st.multiselect("Departments", options=deps, default=default_sel, key=sel_key, help="Changes the lines below")

if not dept.empty and "usage_date" in dept.columns and deps:
    # last fully completed day
    dcur = dept[(dept["usage_date"] >= today - dt.timedelta(days=days_shown - 1)) & (dept["usage_date"] < today)].copy()
    if sel:
        dcur = dcur[dcur["department"].isin(sel)]
    plot_df = dcur.rename(columns={"usage_date": "date", "total_cost_usd": "usd"}).copy()
    if PLOTLY and not plot_df.empty:
        fig = px.line(plot_df, x="date", y="usd", color="department")
        fig.update_traces(mode="lines+markers", marker=dict(size=4))
        fig.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10), hovermode="x unified", legend_title_text="Department")
        fig.update_yaxes(tickprefix="$", separatethousands=True, title="")
        fig.update_xaxes(title="")
        st.plotly_chart(fig, width="stretch")
    else:
        agg = plot_df.groupby("date", as_index=False)["usd"].sum().set_index("date")
        st.line_chart(agg, height=300)
else:
    st.info("No department data.")

st.divider()

# -------- Top tables --------------------------------------------------------
def compute_budget_delta_window(days: int) -> pd.DataFrame:
    if budget is None or budget.empty:
        return pd.DataFrame(columns=["department", "budget_window_usd"])
    end_date = today - dt.timedelta(days=1)
    start_date = end_date - dt.timedelta(days=days - 1)
    mask = (budget["date"] >= start_date) & (budget["date"] <= end_date)
    bd = budget.loc[mask].groupby("department", as_index=False)["budget_usd"].sum()
    return bd.rename(columns={"budget_usd": "budget_window_usd"})

budget_win = compute_budget_delta_window(days_shown)

def render_top_table(title: str, df: pd.DataFrame, key_col: str, value_col: str, add_budget=False):
    st.markdown(
        f'### {title} <span class="section-help"><a href="{DOCS_URL}" target="_blank">ⓘ</a></span>',
        unsafe_allow_html=True,
    )
    st.caption(f"Spend over the last {days_shown} days")
    if df.empty:
        st.info("No rows in current window.")
        return

    g = df.groupby(key_col, as_index=False)[value_col].sum().rename(columns={key_col: humanize_header(key_col), value_col: "Window_value"})
    total = float(g["Window_value"].sum())
    g = g.sort_values("Window_value", ascending=False).head(rows_to_show).reset_index(drop=True)

    if add_budget and not budget_win.empty and key_col == "department":
        tmp = budget_win.rename(columns={"department": humanize_header(key_col)})
        g = g.merge(tmp, on=humanize_header(key_col), how="left")
        g["vs budget"] = g.apply(
            lambda r: ("—" if pd.isna(r.get("budget_window_usd")) else (f"{((r['Window_value']-r['budget_window_usd'])/r['budget_window_usd']*100):+.0f}%")),
            axis=1,
        )
    else:
        g["vs budget"] = None

    g["Spend (last N days)"] = g["Window_value"].apply(lambda x: fmt_usd(float(x)))
    display_cols = [humanize_header(key_col), "Spend (last N days)"]
    if add_budget and key_col == "department":
        display_cols.append("vs budget")

    if total > 0:
        g["Share"] = (g["Window_value"] / total * 100.0).round(0)
        display_cols.append("Share")
        st.dataframe(
            g[display_cols],
            hide_index=True,
            width="stretch",
            column_config={
                humanize_header(key_col): st.column_config.TextColumn(width="medium"),
                "Spend (last N days)": st.column_config.TextColumn(width="medium"),
                "vs budget": st.column_config.TextColumn(width="small"),
                "Share": st.column_config.NumberColumn(format="%.0f%%", width="small"),
            },
        )
    else:
        st.dataframe(
            g[display_cols],
            hide_index=True,
            width="stretch",
            column_config={
                humanize_header(key_col): st.column_config.TextColumn(width="medium"),
                "Spend (last N days)": st.column_config.TextColumn(width="medium"),
                "vs budget": st.column_config.TextColumn(width="small"),
            },
        )

L, R = st.columns(2)
with L:
    render_top_table(
        "Top Departments",
        dept[(dept["usage_date"] >= today - dt.timedelta(days=days_shown - 1)) & (dept["usage_date"] < today)] if not dept.empty else pd.DataFrame(),
        "department",
        "total_cost_usd",
        add_budget=True,
    )
with R:
    render_top_table(
        "Top Warehouses",
        fct[(fct["usage_date"] >= today - dt.timedelta(days=days_shown - 1)) & (fct["usage_date"] < today)] if not fct.empty else pd.DataFrame(),
        "warehouse_name",
        "total_cost",
        add_budget=False,
    )

# -------- Pro section -------------------------------------------------------
show_for_export = pd.DataFrame()
if PRO_PACK_FLAG:
    st.divider()
    st.markdown(
        f'### Pro Insights <span class="section-help"><a href="{DOCS_URL}" target="_blank">ⓘ</a></span>',
        unsafe_allow_html=True,
    )
    with st.expander("How these numbers are computed?", expanded=False):
        st.markdown(
            f"""
- **Source of truth:** `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` (Compute + Cloud Services).
- **Forecast (month):** prorates month-to-date run rate to a 30-day month.
- **Idle wasted (last {days_shown}):** sum of hourly idle cost over the last **{days_shown}** days.
- **Idle projected (monthly, Pro):** month-to-date hourly idle scaled to 30 days.
- **Notes:** per-warehouse/attribution figures are estimates; authoritative spend comes from metering history.
            """
        )

    if enable_pro:
        if pro_hourly.empty:
            st.info("Pro enabled, but no Pro datasets found. Set PRO_DATABASE/PRO_SCHEMA to activate.")
        else:
            pdf = pro_hourly.copy()
            pdf["Idle $/mo (est.)"] = (pdf["idle_cost_adj"] / max(days_shown, 1) * 30.0).astype(float)
            pdf["Idle share (%)"] = (
                100.0 * (pdf["idle_cost_adj"] / pdf["total_cost"].replace(0, np.nan))
            ).clip(0, 100).fillna(0.0).round(0)
            if not rightsizing_df.empty:
                pdf = pdf.merge(rightsizing_df, on="warehouse_name", how="left")
            show = (
                pdf[["warehouse_name", "Idle $/mo (est.)", "Idle share (%)"]]
                .rename(columns={"warehouse_name": "Warehouse"})
                .sort_values("Idle $/mo (est.)", ascending=False)
                .head(rows_to_show)
            )
            st.dataframe(
                show.assign(**{"Idle $/mo (display)": show["Idle $/mo (est.)"].apply(lambda v: fmt_usd(float(v)))})[
                    ["Warehouse", "Idle $/mo (display)", "Idle share (%)"]
                ],
                hide_index=True,
                width="stretch",
                column_config={
                    "Warehouse": st.column_config.TextColumn(width="medium"),
                    "Idle $/mo (display)": st.column_config.TextColumn(width="medium"),
                    "Idle share (%)": st.column_config.NumberColumn(format="%.0f%%", width="small"),
                },
            )
            show_for_export = (
                pdf.rename(columns={"warehouse_name": "name"})[
                    ["name", "Idle $/mo (est.)", "Idle share (%)", "rightsize_suggestion"]
                ]
                .sort_values("Idle $/mo (est.)", ascending=False)
            )

            current = load_current_warehouses()
            sql_lines, notes = [], []
            for _, r in show.iterrows():
                wh = str(r["Warehouse"]).upper()
                rec = None
                try:
                    rec = rightsizing_df.loc[
                        rightsizing_df["warehouse_name"].str.upper() == wh, "rightsize_suggestion"
                    ].iloc[0]
                except Exception:
                    rec = None
                if rec:
                    notes.append(f"**{wh}** — {rec}. Change size in Snowsight or via Terraform/IaC policy.")
                    continue
                idle_share = float(show.loc[show["Warehouse"] == r["Warehouse"], "Idle share (%)"].iloc[0])
                mins = 5 if idle_share >= 40 else 10
                target_sec = mins * 60
                cur_meta = current.get(wh, {})
                current_sec = cur_meta.get("auto_suspend")
                if current_sec != target_sec:
                    current_disp = (
                        f"{int(current_sec/60)} min"
                        if current_sec and current_sec >= 60
                        else (f"{current_sec} sec" if current_sec else "—")
                    )
                    notes.append(f"**{wh}** — Current: {current_disp} → Target: {mins} min")
                    sql_lines.append(f"ALTER WAREHOUSE {wh} SET AUTO_SUSPEND = {target_sec};")

            if sql_lines or notes:
                show_actions = st.toggle("Show change-set actions", False, help="Only shows changes when target differs from current setting")
                if show_actions:
                    for n in notes:
                        st.write(n)
                    if sql_lines:
                        st.code("\n".join(sql_lines), language="sql")
                        st.download_button(
                            "Download autosuspend SQL",
                            "\n".join(sql_lines).encode("utf-8"),
                            file_name="autosuspend_changes.sql",
                            mime="text/plain",
                        )
    else:
        st.info("Toggle Pro Insights in the sidebar to surface projected idle and opportunity tiles.")

st.divider()

# -------- Insights CSV ------------------------------------------------------
def build_insights_csv() -> pd.DataFrame:
    expected = [
        "scope",
        "name",
        "window_spend_usd",
        "idle_usd_month_est",
        "idle_share_pct",
        "suggested_action",
        "vs_budget_pct",
    ]

    dep = dept[(dept["usage_date"] >= today - dt.timedelta(days=days_shown - 1)) & (dept["usage_date"] < today)] if not dept.empty else pd.DataFrame()
    if not dep.empty:
        depg = (
            dep.groupby("department", as_index=False)["total_cost_usd"]
            .sum()
            .rename(columns={"department": "name", "total_cost_usd": "window_spend_usd"})
        )
        if budget_win.empty:
            depg["vs_budget_pct"] = np.nan
        else:
            depg = depg.merge(budget_win.rename(columns={"department": "name"}), on="name", how="left")
            depg["vs_budget_pct"] = np.where(
                depg["budget_window_usd"] > 0,
                (depg["window_spend_usd"] - depg["budget_window_usd"]) / depg["budget_window_usd"] * 100.0,
                np.nan,
            )
        depg["scope"] = "department"
    else:
        depg = pd.DataFrame(columns=["scope", "name", "window_spend_usd", "vs_budget_pct"])

    wh = fct[(fct["usage_date"] >= today - dt.timedelta(days=days_shown - 1)) & (fct["usage_date"] < today)] if not fct.empty else pd.DataFrame()
    if not wh.empty:
        whg = (
            wh.groupby("warehouse_name", as_index=False)["total_cost"]
            .sum()
            .rename(columns={"warehouse_name": "name", "total_cost": "window_spend_usd"})
        )
        if isinstance(show_for_export, pd.DataFrame) and not show_for_export.empty:
            whg = whg.merge(
                show_for_export.rename(columns={"Idle $/mo (est.)": "idle_usd_month_est", "Idle share (%)": "idle_share_pct"}),
                on="name",
                how="left",
            )
        else:
            whg["idle_usd_month_est"] = np.nan
            whg["idle_share_pct"] = np.nan

        def action_row(r):
            if pd.notnull(r.get("rightsize_suggestion")):
                return r["rightsize_suggestion"]
            if pd.notnull(r.get("idle_share_pct")):
                v = r["idle_share_pct"]
                return "Schedule weekends" if v >= 70 else ("Auto-suspend 5 min" if v >= 40 else ("Auto-suspend 10 min" if v >= 20 else ""))
            return ""

        whg["suggested_action"] = whg.apply(action_row, axis=1)
        whg["scope"] = "warehouse"
        whg["vs_budget_pct"] = np.nan
    else:
        whg = pd.DataFrame(columns=["scope", "name", "window_spend_usd", "idle_usd_month_est", "idle_share_pct", "suggested_action", "vs_budget_pct"])

    for df_ in (depg, whg):
        for col in expected:
            if col not in df_.columns:
                df_[col] = np.nan
    out = pd.concat([depg[expected], whg[expected]], ignore_index=True)
    return out

budget_win = compute_budget_delta_window(days_shown)
insights_df = build_insights_csv()
if not insights_df.empty:
    st.download_button("Download insights CSV", data=insights_df.to_csv(index=False).encode("utf-8"), file_name="finops_insights.csv", mime="text/csv")

st.divider()

# -------- Diagnostics -------------------
diag_rows = []
def diag_entry(name: str, df: Optional[pd.DataFrame], date_col: Optional[str] = None, explicit_date: Optional[dt.date] = None) -> Dict[str, str]:
    status = "✗"
    latest = "—"
    if df is not None and not df.empty and date_col and date_col in df.columns:
        try:
            series = pd.to_datetime(df[date_col], errors="coerce").dropna()
            if not series.empty:
                latest = series.max().date().isoformat()
                status = "✅"
        except Exception:
            pass
    elif explicit_date:
        latest = explicit_date.isoformat()
        status = "✅"
    elif demo_mode:
        latest = "Demo data"
    return {"Table": name, "Status": status, "Latest usage_date": latest}

diag_rows.append(diag_entry("fct_daily_costs", fct, "usage_date"))
diag_rows.append(diag_entry("fct_cost_by_department", dept, "usage_date"))
diag_rows.append(diag_entry("budget_daily", budget, "date"))
diag_rows.append(diag_entry("fct_budget_vs_actual", None, explicit_date=bva_latest))
diag_df = pd.DataFrame(diag_rows)

with st.expander("Diagnostics", expanded=DEV_MODE):
    st.dataframe(
        diag_df,
        hide_index=True,
        width="stretch",
        column_config={
            "Table": st.column_config.TextColumn(width="medium"),
            "Status": st.column_config.TextColumn(width="small"),
            "Latest usage_date": st.column_config.TextColumn(width="medium"),
        },
    )

# -------- Freshness (sidebar microcopy) ------------------------------------
with st.sidebar:
    if demo_mode:
        st.caption("**Freshness:** Not available in demo")
        st.caption("**Last build:** Not available in demo")
    elif not fresh.empty and "last_end_time" in fresh.columns and pd.notnull(fresh.iloc[0]["last_end_time"]):
        try:
            last = pd.to_datetime(fresh.iloc[0]["last_end_time"])
            if last.tzinfo is None:
                last = last.tz_localize("UTC")
            now = pd.Timestamp.utcnow().tz_localize("UTC")
            age = (now - last).total_seconds() / 3600.0
            st.caption(f"**Freshness:** {age:.1f}h ago")
        except Exception:
            st.caption("**Freshness:** —")
    else:
        st.caption("**Freshness:** —")
        st.caption("**Last build:** —")
