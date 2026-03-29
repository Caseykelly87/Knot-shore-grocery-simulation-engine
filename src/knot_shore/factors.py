"""
factors.py — Factor lookup functions for the sales waterfall.

Functions:
  seasonal_factor(seasonal_profile, month) -> float
  dow_factor(store_profile, day_of_week_num) -> float
  snap_factor(store_profile, is_snap_window) -> float
  promo_volume_factor(department_name, date, promos_df) -> tuple[float, float, bool]
    Returns (lift_factor, discount_pct, promo_active)
  yoy_growth_factor(date) -> float
  labor_pct_adjusted(store_profile, year) -> float
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from knot_shore.config import (
    DOW_FACTORS,
    LABOR_PCT,
    LABOR_WAGE_DRIFT,
    SEASONAL_FACTORS,
    SNAP_FACTOR_OFF,
    SNAP_FACTORS,
    YOY_BASE_DATE,
    YOY_GROWTH_RATE,
)


def seasonal_factor(seasonal_profile: str, month: int) -> float:
    """Return the seasonal multiplier for the given profile and month (1-12)."""
    return SEASONAL_FACTORS[seasonal_profile][month - 1]


def dow_factor(store_profile: str, day_of_week_num: int) -> float:
    """Return the day-of-week multiplier for the given store profile.

    day_of_week_num: 1=Monday ... 7=Sunday (isoweekday convention).
    """
    return DOW_FACTORS[store_profile][day_of_week_num - 1]


def snap_factor(store_profile: str, is_snap_window: bool) -> float:
    """Return the SNAP uplift factor for the given store profile."""
    if is_snap_window:
        return SNAP_FACTORS[store_profile]
    return SNAP_FACTOR_OFF


def promo_volume_factor(
    department_name: str,
    target_date: date,
    promos_df: pd.DataFrame,
) -> tuple[float, float, bool]:
    """Return (lift_factor, discount_pct, promo_active) for a department on a given date.

    Searches promos_df for an active promotion matching the department and date range.
    Returns (1.0, 0.0, False) when no promotion is active.
    """
    if promos_df.empty:
        return (1.0, 0.0, False)

    # Find promos for this department where start_date <= target_date <= end_date
    mask = (
        (promos_df["department_id"].isin(
            promos_df.loc[
                promos_df.get("department_name", pd.Series(dtype=str)) == department_name,
                "department_id",
            ]
        ))
        if "department_name" in promos_df.columns
        else pd.Series([True] * len(promos_df))
    )

    # Filter by department name if the column exists, otherwise use department_id lookup
    # The promos_df contains department_id; we need to match by department_name via DEPARTMENTS.
    # To keep factors.py self-contained we accept either a df with department_name or one
    # that has already been pre-filtered by the caller. The canonical approach: filter by
    # start_date and end_date then check department_name if present.
    if "department_name" in promos_df.columns:
        dept_mask = promos_df["department_name"] == department_name
    else:
        # Resolve department_name via department_id using the DEPARTMENTS config
        from knot_shore.config import DEPARTMENTS as _DEPARTMENTS
        dept_id_map = {d["department_name"]: d["department_id"] for d in _DEPARTMENTS}
        dept_id = dept_id_map.get(department_name)
        if dept_id is None:
            return (1.0, 0.0, False)
        dept_mask = promos_df["department_id"] == dept_id

    date_mask = (promos_df["start_date"] <= target_date) & (promos_df["end_date"] >= target_date)
    active = promos_df[dept_mask & date_mask]

    if active.empty:
        return (1.0, 0.0, False)

    # Use the first matching active promotion
    row = active.iloc[0]
    return (float(row["lift_factor"]), float(row["discount_pct"]), True)


def yoy_growth_factor(target_date: date) -> float:
    """Return the year-over-year growth multiplier anchored to YOY_BASE_DATE."""
    days = (target_date - YOY_BASE_DATE).days
    return (1 + YOY_GROWTH_RATE) ** (days / 365.25)


def labor_pct_adjusted(store_profile: str, year: int) -> float:
    """Return the labor cost percentage for the store profile, adjusted for wage drift.

    Compounds LABOR_WAGE_DRIFT annually from 2023 (base year).
    """
    base = LABOR_PCT[store_profile]
    years_elapsed = year - 2023
    return base * (1 + LABOR_WAGE_DRIFT) ** years_elapsed
