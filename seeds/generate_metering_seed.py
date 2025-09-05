# seeds/generate_metering_seed.py
# Purpose: Generate demo metering with department-distinct shapes while preserving daily totals.

import csv, random, math, os
import datetime as dt
from pathlib import Path
from typing import List, Dict

# ── Config ────────────────────────────────────────────────────────────────────
OUT_DIR = Path("seeds"); OUT_DIR.mkdir(exist_ok=True)
OUT_PATH = OUT_DIR / "metering_demo_seed.csv"

WAREHOUSES = ["COMPUTE_WH","TRANSFORMING","INTL_WH","ML_WH","BI_WH","BATCH_WH"]
WH_TO_DEPT = {
    "BI_WH":        "Business Intelligence",
    "COMPUTE_WH":   "Data Platform",
    "TRANSFORMING": "Data Platform",
    "ML_WH":        "Data Science",
    "INTL_WH":      "Analytics",
    "BATCH_WH":     "Finance",
}

DAYS             = int(os.getenv("DEMO_DAYS", "75"))         # ~2.5 mo
COST_PER_CREDIT  = float(os.getenv("COST_PER_CREDIT", "3.0"))
SEED             = int(os.getenv("DEMO_SEED", "42"))

# Tuners (safe defaults)
WEEKEND_DIP          = float(os.getenv("WEEKEND_DIP", "0.70"))  # general weekend dip
BI_EOM_SPIKE         = float(os.getenv("BI_EOM_SPIKE", "1.60"))
DP_EOM_SPIKE         = float(os.getenv("DP_EOM_SPIKE", "1.10"))
FIN_MID_EOM_SPIKE    = float(os.getenv("FIN_MID_EOM_SPIKE", "1.30"))
DS_BURSTS_PER_MONTH  = int(os.getenv("DS_BURSTS_PER_MONTH", "3"))
DS_BURST_MULT        = float(os.getenv("DS_BURST_MULT", "1.80"))
ANNUAL_DAYLIGHT_NOISE= float(os.getenv("DAILY_NOISE_SD", "0.20"))  # extra day-to-day wiggle

random.seed(SEED)

# ── Helpers ───────────────────────────────────────────────────────────────────
def month_days(d: dt.date) -> int:
    nxt = (d.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return (nxt - dt.timedelta(days=1)).day

def eom_window(d: dt.date, k: int = 4) -> bool:
    return d.day > (month_days(d) - k)

def ds_burst_days_for_month(year: int, month: int, count: int) -> set:
    # deterministic “experiment” spikes for Data Science
    rng = random.Random((year * 100 + month) ^ SEED)
    days = set()
    mlen = month_days(dt.date(year, month, 1))
    while len(days) < min(count, mlen):
        days.add(rng.randint(2, mlen-2))
    return days

def daily_multiplier(dept: str, d: dt.date) -> float:
    wkend = (WEEKEND_DIP if d.weekday() >= 5 else 1.0)

    if dept == "Data Platform":
        # steady weekdays; mild EOM spikes
        base = 1.0 * wkend
        if eom_window(d): base *= DP_EOM_SPIKE
        return base

    if dept == "Business Intelligence":
        # strong EOM reporting crunch; slight Mon bump
        base = (1.05 if d.weekday() == 0 else 1.0) * wkend
        if eom_window(d): base *= BI_EOM_SPIKE
        return base

    if dept == "Finance":
        # mid-month close + EOM
        base = 1.0 * wkend
        if d.day in (14,15,16): base *= FIN_MID_EOM_SPIKE
        if eom_window(d): base *= FIN_MID_EOM_SPIKE
        return base

    if dept == "Data Science":
        # bursty experimentation mid-month
        bursts = ds_burst_days_for_month(d.year, d.month, DS_BURSTS_PER_MONTH)
        base = 0.95 * wkend
        if d.day in bursts: base *= DS_BURST_MULT
        return base

    if dept == "Analytics":
        # consistent but slightly heavier late-week
        base = (1.0 + (0.05 if d.weekday() in (3,4) else 0.0)) * wkend
        return base

    return 1.0 * wkend

def hourly_weights_for(dept: str) -> List[float]:
    # Rough shape by department (24 values). Sum is irrelevant; normalized later.
    if dept == "Finance":
        # Batch windows ~ early morning
        return [0.9,1.0,1.2,1.2,1.1,1.0,0.7,0.6,0.5,0.5,0.6,0.7,
                0.8,0.8,0.7,0.7,0.6,0.6,0.6,0.7,0.8,0.8,0.8,0.9]
    if dept == "Business Intelligence":
        # Business hours peak 9–16
        return [0.4,0.4,0.5,0.5,0.6,0.7,0.9,1.1,1.2,1.3,1.3,1.2,
                1.1,1.0,1.0,1.0,0.9,0.8,0.7,0.6,0.5,0.5,0.4,0.4]
    if dept == "Data Platform":
        # Daytime steady; low overnight
        return [0.4,0.4,0.5,0.5,0.6,0.8,1.0,1.1,1.1,1.1,1.1,1.1,
                1.1,1.1,1.1,1.1,1.0,0.9,0.8,0.7,0.6,0.5,0.5,0.4]
    if dept == "Data Science":
        # Late night experimentation
        return [0.6,0.6,0.7,0.8,0.8,0.8,0.7,0.7,0.6,0.6,0.7,0.8,
                0.9,1.0,1.0,1.0,1.1,1.2,1.3,1.3,1.2,1.1,0.9,0.7]
    if dept == "Analytics":
        # Evening heavier (timezone drift)
        return [0.5,0.5,0.6,0.6,0.6,0.7,0.8,0.9,0.9,1.0,1.0,1.0,
                1.1,1.2,1.2,1.2,1.2,1.1,1.0,0.9,0.9,0.8,0.7,0.6]
    # Default gentle day curve
    return [0.5 if h < 6 else 1.0 if h < 18 else 0.8 for h in range(24)]

def add_noise_and_preserve_total(hour_slices: List[float], total: float, jitter_scale: float) -> List[float]:
    # multiplicative noise then renormalize to exact 'total'
    rng = random.Random(SEED ^ int(total*1000) ^ len(hour_slices))
    noisy = [max(0.0, s * rng.gauss(1.0, jitter_scale)) for s in hour_slices]
    s = sum(noisy) or 1e-9
    rescaled = [x * (total / s) for x in noisy]
    # Fix rounding drift by adjusting the last bucket
    rounded = [max(0.0, round(x, 6)) for x in rescaled]
    drift = total - sum(rounded)
    rounded[-1] = max(0.0, rounded[-1] + drift)
    return rounded

# ── Main generation ───────────────────────────────────────────────────────────
start_date = dt.date.today() - dt.timedelta(days=DAYS)
rows = []

for wh in WAREHOUSES:
    dept = WH_TO_DEPT.get(wh, "Unmapped")
    base_daily = random.uniform(2.0, 7.0)  # daily credits baseline per WH

    hw = hourly_weights_for(dept)
    hw_sum = sum(hw); hw = [w / hw_sum for w in hw]  # normalize to 1.0

    for d in range(DAYS + 1):
        day = start_date + dt.timedelta(days=d)

        # Day-level total credits
        day_mult = daily_multiplier(dept, day)
        day_wiggle = random.gauss(1.0, ANNUAL_DAYLIGHT_NOISE)  # small daily wiggle
        day_total_credits = max(0.2, base_daily * day_mult * day_wiggle)

        # Split across 24 hours, then add noise but preserve day total
        per_hour = [day_total_credits * w for w in hw]
        per_hour = add_noise_and_preserve_total(per_hour, day_total_credits, jitter_scale=0.25)

        for h, credits in enumerate(per_hour):
            start = dt.datetime.combine(day, dt.time(hour=h))
            end   = start + dt.timedelta(hours=1)
            credits = max(0.0, round(credits, 3))
            cost = round(credits * COST_PER_CREDIT, 2)
            rows.append([start.isoformat(sep=" "), end.isoformat(sep=" "), wh, credits, cost])

with open(OUT_PATH, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["START_TIME","END_TIME","WAREHOUSE_NAME","TOTAL_CREDITS_USED","TOTAL_COST_USD"])
    w.writerows(rows)

print(f"Wrote {len(rows)} rows to {OUT_PATH} for {len(WAREHOUSES)} warehouses over {DAYS+1} days.")
