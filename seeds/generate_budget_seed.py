# seeds/generate_budget_seed.py
# Purpose: Generate budget_daily.csv with current-dated rows matching demo warehouses.
# Usage:   python seeds/generate_budget_seed.py
#          Regenerate whenever dates go stale or budget allocations change.

import csv, os
import datetime as dt
from pathlib import Path

OUT_DIR = Path("seeds"); OUT_DIR.mkdir(exist_ok=True)
OUT_PATH = OUT_DIR / "budget_daily.csv"

DAYS_BACK    = int(os.getenv("BUDGET_DAYS_BACK", "75"))
DAYS_FORWARD = int(os.getenv("BUDGET_DAYS_FORWARD", "30"))

# Department daily budget allocations (USD) — match department_mapping.csv departments
DEPARTMENT_BUDGETS = {
    "Analytics":             120.0,
    "Business Intelligence": 100.0,
    "Data Platform":         200.0,
    "Data Science":           80.0,
    "Finance":                60.0,
}

start_date = dt.date.today() - dt.timedelta(days=DAYS_BACK)
end_date   = dt.date.today() + dt.timedelta(days=DAYS_FORWARD)
rows = []

d = start_date
while d <= end_date:
    for dept, budget in sorted(DEPARTMENT_BUDGETS.items()):
        rows.append([dept, d.isoformat(), budget])
    d += dt.timedelta(days=1)

with open(OUT_PATH, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["department", "date", "budget_usd"])
    w.writerows(rows)

print(f"Wrote {len(rows)} rows to {OUT_PATH} ({len(DEPARTMENT_BUDGETS)} depts x {(end_date - start_date).days + 1} days)")
