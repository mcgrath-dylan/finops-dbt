"""Diagnose the Snowflake connection the Streamlit app uses.

Run from the finops-dbt repo root:
    python scripts/check_snowflake.py

Reads the same env vars as app/streamlit_app.py:get_conn_params and reports:
  1. Which params are set (password masked)
  2. Whether connect() succeeds
  3. Identity (user/role/warehouse/database)
  4. Row counts in the two critical demo marts
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def main() -> int:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")

    keys = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]
    print("== env ==")
    for k in keys:
        v = os.getenv(k, "")
        if k.endswith("PASSWORD"):
            v = f"<set len={len(v)}>" if v else "<empty>"
        print(f"  {k}={v or '<empty>'}")

    try:
        import snowflake.connector as sf
    except Exception as exc:
        print(f"\nFAIL: snowflake.connector import: {type(exc).__name__}: {exc}")
        return 1

    print("\n== connect ==")
    try:
        conn = sf.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE") or None,
            role=os.environ.get("SNOWFLAKE_ROLE") or None,
            database=os.environ.get("SNOWFLAKE_DATABASE") or None,
            schema=os.environ.get("SNOWFLAKE_SCHEMA") or None,
        )
    except Exception as exc:
        print(f"FAIL: {type(exc).__name__}: {exc}")
        return 1
    print("OK")

    cur = conn.cursor()
    cur.execute("select current_user(), current_role(), current_warehouse(), current_database()")
    print("identity:", cur.fetchone())

    db = os.environ.get("SNOWFLAKE_DATABASE") or "FINOPS_DEV"
    print(f"\n== demo marts ({db}.DEMO) ==")
    for table in ("fct_daily_costs", "fct_cost_by_department"):
        try:
            cur.execute(f"select count(*), max(usage_date) from {db}.DEMO.{table}")
            n, latest = cur.fetchone()
            print(f"  {table}: rows={n}  latest_usage_date={latest}")
        except Exception as exc:
            print(f"  {table}: ERROR {type(exc).__name__}: {exc}")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
