# seeds/generate_storage_seed.py
# Purpose: Generate storage_demo_seed.csv with synthetic STORAGE_USAGE data.
# Usage:   python seeds/generate_storage_seed.py
# Pattern: Matches generate_metering_seed.py conventions.

import csv, random, math, os
import datetime as dt
from pathlib import Path

OUT_DIR = Path("seeds"); OUT_DIR.mkdir(exist_ok=True)
OUT_PATH = OUT_DIR / "storage_demo_seed.csv"

DAYS = int(os.getenv("DEMO_DAYS", "75"))
SEED = int(os.getenv("DEMO_SEED", "42"))
random.seed(SEED)

# Databases with realistic storage profiles (bytes)
DATABASES = {
    "ANALYTICS_DB":  {"active_gb": 150, "failsafe_gb": 20, "stage_gb": 5,  "growth_pct": 0.002},
    "STAGING_DB":    {"active_gb": 80,  "failsafe_gb": 10, "stage_gb": 15, "growth_pct": 0.003},
    "RAW_DB":        {"active_gb": 400, "failsafe_gb": 60, "stage_gb": 25, "growth_pct": 0.004},
    "ML_DB":         {"active_gb": 50,  "failsafe_gb": 5,  "stage_gb": 2,  "growth_pct": 0.001},
}

BYTES_PER_GB = 1073741824  # 1024^3

start_date = dt.date.today() - dt.timedelta(days=DAYS)
rows = []

for db_name, profile in DATABASES.items():
    db_id = hash(db_name) % 100000 + 1000

    for d in range(DAYS + 1):
        day = start_date + dt.timedelta(days=d)

        # Apply daily growth with small noise
        growth_factor = (1 + profile["growth_pct"]) ** d
        noise = random.gauss(1.0, 0.01)

        active_bytes = int(profile["active_gb"] * BYTES_PER_GB * growth_factor * noise)
        failsafe_bytes = int(profile["failsafe_gb"] * BYTES_PER_GB * growth_factor * random.gauss(1.0, 0.02))
        stage_bytes = int(profile["stage_gb"] * BYTES_PER_GB * random.gauss(1.0, 0.05))

        rows.append([
            day.isoformat(),
            db_id,
            db_name,
            max(0, active_bytes),
            max(0, failsafe_bytes),
            max(0, stage_bytes),
        ])

with open(OUT_PATH, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["USAGE_DATE", "DATABASE_ID", "DATABASE_NAME",
                "AVERAGE_DATABASE_BYTES", "AVERAGE_FAILSAFE_BYTES", "AVERAGE_STAGE_BYTES"])
    w.writerows(rows)

print(f"Wrote {len(rows)} rows to {OUT_PATH} for {len(DATABASES)} databases over {DAYS + 1} days.")
