"""
promotions.py — Full 4-year promotion schedule generator.

Generates approximately 6-10 chain-wide promotions per month from
CALENDAR_START (2023-01-01) through CALENDAR_END (2026-12-31).

Rules:
  - No two promos may overlap on the same department.
  - Duration: 3-10 days per promo.
  - Promo type distribution: weighted toward pct_off/bundle for
    regular months; bogo/loss_leader for peak seasons.
  - Seasonal department preferences come from config.SEASONAL_PROMO_MAP.
  - promo_id is auto-incremented (1, 2, 3, ...).
  - All generation is deterministically seeded from config.GLOBAL_SEED.
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta

import numpy as np
import pandas as pd

from knot_shore.config import (
    CALENDAR_END,
    CALENDAR_START,
    DEPARTMENTS,
    GLOBAL_SEED,
    PROMO_DISCOUNT_RANGE,
    PROMO_DURATION_MAX,
    PROMO_DURATION_MIN,
    PROMO_LIFT_RANGE,
    PROMO_TYPES,
    PROMOS_PER_MONTH_MAX,
    PROMOS_PER_MONTH_MIN,
    SEASONAL_PROMO_MAP,
)

# ---------------------------------------------------------------------------
# Name templates by month (1-indexed) — used to generate realistic promo names
# ---------------------------------------------------------------------------

_MONTH_NAMES: dict[int, list[str]] = {
    1:  [
        "New Year New You Sale",
        "January Fresh Start Savings",
        "Winter Wellness Event",
        "Cold Weather Comfort Savings",
        "January Pantry Stock-Up",
    ],
    2:  [
        "Valentine's Sweets Sale",
        "February Love & Savings",
        "Sweet Treats Event",
        "Game Day Grub Sale",
        "February Fresh Picks",
    ],
    3:  [
        "Spring Forward Savings",
        "March Fresh Start Event",
        "St. Patrick's Day Feast",
        "Early Spring BBQ Preview",
        "March Madness Snack Sale",
    ],
    4:  [
        "Easter Feast Sale",
        "Spring Celebration Savings",
        "April Fresh Picks Event",
        "Spring Into Savings",
        "Easter Table Sale",
    ],
    5:  [
        "Memorial Day Grilling Event",
        "May Outdoor Feast Sale",
        "Spring BBQ Kickoff",
        "May Fresh Flavors Event",
        "Mother's Day Brunch Sale",
    ],
    6:  [
        "Summer Kickoff Sale",
        "June Grilling Event",
        "Summer Fresh Picks",
        "Father's Day Feast",
        "June Backyard BBQ Sale",
    ],
    7:  [
        "Summer Grilling Event",
        "July 4th Celebration Sale",
        "Mid-Summer Refresh",
        "July Fresh & Cool Savings",
        "Peak Summer Sale",
    ],
    8:  [
        "Back-to-School Savings",
        "August Fresh Start Sale",
        "Late Summer BBQ Blowout",
        "School Lunch Pack-Up Sale",
        "August Harvest Picks",
    ],
    9:  [
        "Labor Day Feast Sale",
        "Fall Harvest Preview",
        "September Pantry Reset",
        "Back-to-Routine Savings",
        "September Fresh Picks Event",
    ],
    10: [
        "Halloween Treat Sale",
        "October Harvest Festival",
        "Fall Flavors Event",
        "Trick or Treat Savings",
        "October Cozy Comfort Sale",
    ],
    11: [
        "Thanksgiving Feast Sale",
        "November Holiday Kickoff",
        "Holiday Prep Savings",
        "November Harvest Table Event",
        "Pre-Holiday Stock-Up",
    ],
    12: [
        "Holiday Celebration Sale",
        "December Feast & Savings",
        "Christmas Table Event",
        "Holiday Entertaining Sale",
        "Year-End Savings Blowout",
        "Holiday Baking Event",
        "December Pantry Fill-Up",
    ],
}

# Department-flavored name suffixes — appended when department is strongly seasonal
_DEPT_FLAVOR: dict[str, str] = {
    "Produce":                 "Fresh Produce",
    "Meat & Seafood":          "Meat & Seafood",
    "Dairy & Eggs":            "Dairy",
    "Bakery":                  "Bakery",
    "Deli & Prepared":         "Deli",
    "Frozen":                  "Frozen Foods",
    "Grocery (Center Store)":  "Grocery",
    "Beverages":               "Beverages",
    "Snacks & Candy":          "Snacks",
    "Health/Beauty/Household": "Wellness",
}

# Peak seasons for bogo/loss_leader weighting (months where these are preferred)
_PEAK_MONTHS: set[int] = {5, 6, 7, 11, 12}

# Promo type weights: [pct_off, bogo, bundle, loss_leader]
_WEIGHTS_REGULAR: list[float] = [0.45, 0.15, 0.30, 0.10]
_WEIGHTS_PEAK:    list[float] = [0.25, 0.25, 0.20, 0.30]

_ALL_DEPT_NAMES: list[str] = [d["department_name"] for d in DEPARTMENTS]


def _build_dept_weights(month: int) -> tuple[list[str], list[float]]:
    """Return (dept_names, weights) with seasonal preferences boosted."""
    preferred = set(SEASONAL_PROMO_MAP.get(month, []))
    weights = []
    for name in _ALL_DEPT_NAMES:
        weights.append(3.0 if name in preferred else 1.0)
    total = sum(weights)
    normalized = [w / total for w in weights]
    return _ALL_DEPT_NAMES, normalized


def _promo_name(rng: np.random.Generator, month: int, dept_name: str) -> str:
    """Pick a realistic promotion name for the month and department."""
    base_names = _MONTH_NAMES[month]
    idx = int(rng.integers(0, len(base_names)))
    base = base_names[idx]
    flavor = _DEPT_FLAVOR.get(dept_name, "")
    # 40% chance to append department flavor if it's not already in the name
    if flavor and flavor.lower() not in base.lower() and rng.random() < 0.40:
        return f"{base} — {flavor}"
    return base


def generate_promotions(seed: int = GLOBAL_SEED) -> pd.DataFrame:
    """Generate the full 4-year promotion schedule deterministically."""
    rng = np.random.default_rng(seed)

    # Track active promos per department: dept_name -> list of (start, end) tuples
    dept_schedule: dict[str, list[tuple[date, date]]] = {
        d["department_name"]: [] for d in DEPARTMENTS
    }

    rows: list[dict] = []
    promo_id = 1

    # Iterate month by month
    yr = CALENDAR_START.year
    mo = CALENDAR_START.month
    end_yr = CALENDAR_END.year
    end_mo = CALENDAR_END.month

    while (yr, mo) <= (end_yr, end_mo):
        _, days_in_month = calendar.monthrange(yr, mo)
        month_start = date(yr, mo, 1)
        month_end = date(yr, mo, days_in_month)

        # Clamp to calendar bounds
        effective_start = max(month_start, CALENDAR_START)
        effective_end = min(month_end, CALENDAR_END)

        target_count = int(rng.integers(PROMOS_PER_MONTH_MIN, PROMOS_PER_MONTH_MAX + 1))

        dept_names, dept_weights = _build_dept_weights(mo)
        type_weights = _WEIGHTS_PEAK if mo in _PEAK_MONTHS else _WEIGHTS_REGULAR

        placed = 0
        attempts = 0
        max_attempts = target_count * 8

        while placed < target_count and attempts < max_attempts:
            attempts += 1

            # Pick department
            dept_idx = int(rng.choice(len(dept_names), p=dept_weights))
            dept_name = dept_names[dept_idx]

            # Pick duration
            duration = int(rng.integers(PROMO_DURATION_MIN, PROMO_DURATION_MAX + 1))

            # Pick start date: leave room for duration within the month
            latest_start_day = days_in_month - duration + 1
            if latest_start_day < 1:
                continue
            start_day = int(rng.integers(1, latest_start_day + 1))
            promo_start = date(yr, mo, start_day)
            promo_end = promo_start + timedelta(days=duration - 1)

            # Clamp to effective range
            promo_start = max(promo_start, effective_start)
            promo_end = min(promo_end, effective_end)
            if promo_start > promo_end:
                continue

            # Check for overlap with existing promos on this department
            overlap = False
            for existing_start, existing_end in dept_schedule[dept_name]:
                if promo_start <= existing_end and promo_end >= existing_start:
                    overlap = True
                    break

            if overlap:
                continue

            # Pick promo type
            type_idx = int(rng.choice(len(PROMO_TYPES), p=type_weights))
            promo_type = PROMO_TYPES[type_idx]

            # Pick discount and lift within range for this type
            disc_min, disc_max = PROMO_DISCOUNT_RANGE[promo_type]
            lift_min, lift_max = PROMO_LIFT_RANGE[promo_type]
            discount_pct = round(float(rng.uniform(disc_min, disc_max)), 4)
            lift_factor = round(float(rng.uniform(lift_min, lift_max)), 4)

            # Generate name
            name = _promo_name(rng, mo, dept_name)

            # Find department_id
            dept_id = next(
                d["department_id"]
                for d in DEPARTMENTS
                if d["department_name"] == dept_name
            )

            rows.append(
                {
                    "promo_id": promo_id,
                    "department_id": dept_id,
                    "promo_name": name,
                    "promo_type": promo_type,
                    "discount_pct": discount_pct,
                    "start_date": promo_start,
                    "end_date": promo_end,
                    "lift_factor": lift_factor,
                }
            )

            dept_schedule[dept_name].append((promo_start, promo_end))
            promo_id += 1
            placed += 1

        # Advance month
        if mo == 12:
            yr += 1
            mo = 1
        else:
            mo += 1

    df = pd.DataFrame(rows)
    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
    df["end_date"] = pd.to_datetime(df["end_date"]).dt.date
    return df
