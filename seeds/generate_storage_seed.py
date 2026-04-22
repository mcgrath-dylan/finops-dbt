# seeds/generate_storage_seed.py
# Purpose: Generate storage_demo_seed.csv with synthetic STORAGE_USAGE data.

import csv
import datetime as dt
import os
import random
from pathlib import Path

OUT_DIR = Path("seeds")
OUT_DIR.mkdir(exist_ok=True)
OUT_PATH = OUT_DIR / "storage_demo_seed.csv"

DAYS = int(os.getenv("DEMO_DAYS", "75"))
SEED = int(os.getenv("DEMO_SEED", "42"))
random.seed(SEED)

DATABASES = [
    {"name": "RAW_DB", "active_tb": 120.0, "failsafe_tb": 22.0, "stage_tb": 16.0, "growth_pct": 0.0028},
    {"name": "ANALYTICS_DB", "active_tb": 55.0, "failsafe_tb": 10.0, "stage_tb": 6.0, "growth_pct": 0.0018},
    {"name": "STAGING_DB", "active_tb": 40.0, "failsafe_tb": 12.0, "stage_tb": 24.0, "growth_pct": 0.0035},
    {"name": "APP_DB", "active_tb": 30.0, "failsafe_tb": 6.0, "stage_tb": 5.0, "growth_pct": 0.0015},
    {"name": "ML_DB", "active_tb": 18.0, "failsafe_tb": 4.0, "stage_tb": 2.0, "growth_pct": 0.0022},
]

BYTES_PER_TB = 1024 ** 4

start_date = dt.date.today() - dt.timedelta(days=DAYS)
rows = []

for index, profile in enumerate(DATABASES, start=1):
    db_id = 1000 + index

    for d in range(DAYS + 1):
        day = start_date + dt.timedelta(days=d)
        growth_factor = (1 + profile["growth_pct"]) ** d
        active_noise = random.gauss(1.0, 0.008)
        failsafe_noise = random.gauss(1.0, 0.015)
        stage_noise = random.gauss(1.0, 0.03)

        active_bytes = int(profile["active_tb"] * BYTES_PER_TB * growth_factor * active_noise)
        failsafe_bytes = int(profile["failsafe_tb"] * BYTES_PER_TB * growth_factor * failsafe_noise)
        stage_bytes = int(profile["stage_tb"] * BYTES_PER_TB * stage_noise)

        rows.append(
            [
                day.isoformat(),
                db_id,
                profile["name"],
                max(0, active_bytes),
                max(0, failsafe_bytes),
                max(0, stage_bytes),
            ]
        )

with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(
        [
            "USAGE_DATE",
            "DATABASE_ID",
            "DATABASE_NAME",
            "AVERAGE_DATABASE_BYTES",
            "AVERAGE_FAILSAFE_BYTES",
            "AVERAGE_STAGE_BYTES",
        ]
    )
    w.writerows(rows)

print(f"Wrote {len(rows)} rows to {OUT_PATH} for {len(DATABASES)} databases over {DAYS + 1} days.")
