# seeds/generate_query_history_seed.py
# Purpose: Generate query_history_demo_seed.csv aligned with demo metering warehouses.

import csv
import datetime as dt
import os
import random
from pathlib import Path

OUT_DIR = Path("seeds")
OUT_DIR.mkdir(exist_ok=True)
METERING_PATH = OUT_DIR / "metering_demo_seed.csv"
OUT_PATH = OUT_DIR / "query_history_demo_seed.csv"

SEED = int(os.getenv("DEMO_SEED", "42"))
rng = random.Random(SEED ^ 0xA11CE)

WAREHOUSE_PROFILES = {
    "COMPUTE_WH": {
        "department": "Data Platform",
        "warehouse_size": "LARGE",
        "users": ["airflow", "dbt_cloud", "elt_bot"],
        "roles": ["TRANSFORMER", "ELT_ROLE"],
        "targets": [("RAW_DB", "STAGE"), ("STAGING_DB", "INGEST"), ("ANALYTICS_DB", "TRANSFORM")],
        "query_types": ["COPY", "MERGE", "INSERT"],
        "queries_per_credit": 0.32,
    },
    "TRANSFORMING": {
        "department": "Data Platform",
        "warehouse_size": "LARGE",
        "users": ["dbt_cloud", "analytics_eng", "scheduler"],
        "roles": ["TRANSFORMER", "ANALYTICS_ENGINEER"],
        "targets": [("ANALYTICS_DB", "MART"), ("APP_DB", "CORE"), ("RAW_DB", "REFINED")],
        "query_types": ["MERGE", "CREATE_TABLE_AS_SELECT", "INSERT"],
        "queries_per_credit": 0.28,
    },
    "ETL_WH": {
        "department": "Data Platform",
        "warehouse_size": "X-LARGE",
        "users": ["elt_bot", "orchestrator", "dbt_cloud"],
        "roles": ["ELT_ROLE", "TRANSFORMER"],
        "targets": [("RAW_DB", "LANDING"), ("STAGING_DB", "STAGE"), ("ANALYTICS_DB", "TRANSFORM")],
        "query_types": ["COPY", "MERGE", "DELETE"],
        "queries_per_credit": 0.26,
    },
    "INTL_WH": {
        "department": "Analytics",
        "warehouse_size": "MEDIUM",
        "users": ["intl_analyst", "growth_analyst", "product_analyst"],
        "roles": ["ANALYST", "ANALYTICS_ROLE"],
        "targets": [("ANALYTICS_DB", "REPORTING"), ("APP_DB", "PUBLIC"), ("RAW_DB", "INTL")],
        "query_types": ["SELECT", "CREATE_TABLE_AS_SELECT"],
        "queries_per_credit": 0.55,
    },
    "ML_WH": {
        "department": "Data Science",
        "warehouse_size": "LARGE",
        "users": ["ml_eng", "data_scientist", "feature_job"],
        "roles": ["ML_ROLE", "SCIENCE_ROLE"],
        "targets": [("ML_DB", "FEATURES"), ("ML_DB", "EXPERIMENTS"), ("RAW_DB", "SANDBOX")],
        "query_types": ["SELECT", "CREATE_TABLE_AS_SELECT", "INSERT"],
        "queries_per_credit": 0.22,
    },
    "BI_WH": {
        "department": "Business Intelligence",
        "warehouse_size": "MEDIUM",
        "users": ["bi_analyst", "tableau_service", "exec_dashboard"],
        "roles": ["BI_ROLE", "ANALYST"],
        "targets": [("ANALYTICS_DB", "REPORTING"), ("APP_DB", "DASHBOARD"), ("ANALYTICS_DB", "FINANCE")],
        "query_types": ["SELECT", "CREATE_TABLE_AS_SELECT"],
        "queries_per_credit": 0.65,
    },
    "REPORTING_WH": {
        "department": "Business Intelligence",
        "warehouse_size": "SMALL",
        "users": ["finance_analyst", "ops_analyst", "exec_dashboard"],
        "roles": ["BI_ROLE", "FINANCE_ROLE"],
        "targets": [("ANALYTICS_DB", "REPORTING"), ("APP_DB", "DASHBOARD"), ("ANALYTICS_DB", "OPS")],
        "query_types": ["SELECT", "INSERT"],
        "queries_per_credit": 0.58,
    },
    "BATCH_WH": {
        "department": "Finance",
        "warehouse_size": "SMALL",
        "users": ["finance_close", "fpna_bot", "controller"],
        "roles": ["FINANCE_ROLE", "BATCH_ROLE"],
        "targets": [("APP_DB", "FINANCE"), ("ANALYTICS_DB", "FINANCE"), ("RAW_DB", "LEDGER")],
        "query_types": ["INSERT", "MERGE", "SELECT"],
        "queries_per_credit": 0.42,
    },
}


def active_probability(department: str, hour: int, weekday: int) -> float:
    weekend = weekday >= 5
    if department == "Data Platform":
        if hour < 6 or hour >= 20:
            return 0.76 if not weekend else 0.66
        if 6 <= hour < 10:
            return 0.92 if not weekend else 0.78
        return 0.86 if not weekend else 0.72
    if department == "Business Intelligence":
        if 8 <= hour < 18:
            return 0.97 if not weekend else 0.78
        if 5 <= hour < 8 or 18 <= hour < 22:
            return 0.88 if not weekend else 0.68
        return 0.58 if not weekend else 0.42
    if department == "Finance":
        if 1 <= hour < 7:
            return 0.94 if not weekend else 0.76
        if 7 <= hour < 18:
            return 0.80 if not weekend else 0.55
        return 0.64 if not weekend else 0.44
    if department == "Data Science":
        if 11 <= hour < 23:
            return 0.82 if not weekend else 0.66
        return 0.58 if not weekend else 0.40
    if department == "Analytics":
        if 7 <= hour < 21:
            return 0.92 if not weekend else 0.72
        return 0.64 if not weekend else 0.44
    return 0.70


rows = []
query_sequence = 0

with open(METERING_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        warehouse_name = (row.get("WAREHOUSE_NAME") or "").strip().upper()
        if not warehouse_name:
            continue
        profile = WAREHOUSE_PROFILES.get(warehouse_name)
        if profile is None:
            continue

        hour_start = dt.datetime.fromisoformat(row["START_TIME"])
        credits_used = float(row["TOTAL_CREDITS_USED"])
        active_prob = active_probability(profile["department"], hour_start.hour, hour_start.weekday())
        if rng.random() > active_prob:
            continue

        target = rng.choice(profile["targets"])
        query_count = max(
            1,
            int(round(credits_used * profile["queries_per_credit"] + rng.uniform(0.2, 2.4))),
        )

        for offset in range(query_count):
            query_sequence += 1
            minute = rng.randint(0, 55)
            second = rng.randint(0, 55)
            duration_seconds = min(1800, max(8, int(rng.uniform(12, 420) + credits_used * rng.uniform(4, 18))))
            start_time = hour_start + dt.timedelta(minutes=minute, seconds=second)
            end_time = min(start_time + dt.timedelta(seconds=duration_seconds), hour_start + dt.timedelta(minutes=59, seconds=59))
            total_elapsed_ms = int((end_time - start_time).total_seconds() * 1000)
            execution_ms = max(1000, total_elapsed_ms - rng.randint(250, 5000))
            rows_produced = max(1, int(credits_used * rng.uniform(500, 6000)))
            bytes_scanned = int(max(1, credits_used * rng.uniform(2.0, 18.0)) * (1024 ** 3))

            rows.append(
                [
                    f"Q{query_sequence:08d}",
                    start_time.isoformat(sep=" "),
                    end_time.isoformat(sep=" "),
                    rng.choice(profile["users"]),
                    rng.choice(profile["roles"]),
                    warehouse_name,
                    profile["warehouse_size"],
                    rng.choice(profile["query_types"]),
                    target[0],
                    target[1],
                    "SUCCESS",
                    bytes_scanned,
                    rows_produced,
                    total_elapsed_ms,
                    execution_ms,
                ]
            )

with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(
        [
            "QUERY_ID",
            "START_TIME",
            "END_TIME",
            "USER_NAME",
            "ROLE_NAME",
            "WAREHOUSE_NAME",
            "WAREHOUSE_SIZE",
            "QUERY_TYPE",
            "DATABASE_NAME",
            "SCHEMA_NAME",
            "EXECUTION_STATUS",
            "BYTES_SCANNED",
            "ROWS_PRODUCED",
            "TOTAL_ELAPSED_TIME",
            "EXECUTION_TIME",
        ]
    )
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows to {OUT_PATH}.")
