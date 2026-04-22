# seeds/generate_budget_seed.py
# Purpose: Generate budget_daily.csv with current-dated rows aligned to demo spend.

import csv
import datetime as dt
import os
from collections import defaultdict
from pathlib import Path

OUT_DIR = Path("seeds")
OUT_DIR.mkdir(exist_ok=True)
OUT_PATH = OUT_DIR / "budget_daily.csv"
METERING_PATH = OUT_DIR / "metering_demo_seed.csv"
MAPPING_PATH = OUT_DIR / "department_mapping.csv"

DAYS_BACK = int(os.getenv("BUDGET_DAYS_BACK", "75"))
DAYS_FORWARD = int(os.getenv("BUDGET_DAYS_FORWARD", "30"))
BUDGET_LOOKBACK_DAYS = int(os.getenv("BUDGET_LOOKBACK_DAYS", "30"))
TARGET_TOTAL_MULTIPLIER = float(os.getenv("TARGET_BUDGET_MULTIPLIER", "1.10"))

DEPARTMENT_VARIANCE_MULTIPLIERS = {
    "Analytics": 1.08,
    "Business Intelligence": 0.98,
    "Data Platform": 1.05,
    "Data Science": 0.85,
    "Finance": 1.15,
}


def load_mapping(path: Path) -> dict[str, str]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {
            row["warehouse_name"].strip().upper(): row["department"].strip()
            for row in reader
            if row.get("warehouse_name") and row.get("department")
        }


def compute_daily_department_actuals() -> dict[str, float]:
    mapping = load_mapping(MAPPING_PATH)
    daily_totals: dict[tuple[str, dt.date], float] = defaultdict(float)
    max_day = None

    with open(METERING_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            start_time = dt.datetime.fromisoformat(row["START_TIME"])
            usage_day = start_time.date()
            max_day = usage_day if max_day is None or usage_day > max_day else max_day
            warehouse_name = row["WAREHOUSE_NAME"].strip().upper()
            department = mapping.get(warehouse_name)
            if department is None:
                continue
            daily_totals[(department, usage_day)] += float(row["TOTAL_COST_USD"])

    if max_day is None:
        raise RuntimeError("metering_demo_seed.csv is empty; regenerate metering before budget")

    min_day = max_day - dt.timedelta(days=BUDGET_LOOKBACK_DAYS - 1)
    department_totals: dict[str, float] = defaultdict(float)
    for (department, usage_day), total in daily_totals.items():
        if usage_day >= min_day:
            department_totals[department] += total

    actual_daily = {
        department: total / BUDGET_LOOKBACK_DAYS
        for department, total in department_totals.items()
        if department in DEPARTMENT_VARIANCE_MULTIPLIERS
    }
    if not actual_daily:
        raise RuntimeError("could not derive department actuals from metering_demo_seed.csv")
    return actual_daily


actual_daily = compute_daily_department_actuals()
weighted_total = sum(
    actual_daily[department] * multiplier
    for department, multiplier in DEPARTMENT_VARIANCE_MULTIPLIERS.items()
)
target_total = sum(actual_daily.values()) * TARGET_TOTAL_MULTIPLIER
scale = target_total / weighted_total if weighted_total else 1.0

department_budgets = {
    department: round(actual_daily[department] * multiplier * scale, 2)
    for department, multiplier in DEPARTMENT_VARIANCE_MULTIPLIERS.items()
}

start_date = dt.date.today() - dt.timedelta(days=DAYS_BACK)
end_date = dt.date.today() + dt.timedelta(days=DAYS_FORWARD)
rows = []

d = start_date
while d <= end_date:
    for department, budget in sorted(department_budgets.items()):
        rows.append([department, d.isoformat(), budget])
    d += dt.timedelta(days=1)

with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["department", "date", "budget_usd"])
    w.writerows(rows)

print(
    f"Wrote {len(rows)} rows to {OUT_PATH} "
    f"({len(department_budgets)} depts x {(end_date - start_date).days + 1} days)"
)
