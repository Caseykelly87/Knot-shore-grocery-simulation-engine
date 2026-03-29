"""
dimensions.py — Generators for the three dimension tables.

  dim_stores      : 8 stores with profile metadata
  dim_departments : 10 departments with margin and seasonal profile
  dim_calendar    : full date range 2023-01-01 through 2026-12-31

Holiday logic covers:
  New Year's Day, Super Bowl Sunday, Valentine's Day, Easter Sunday,
  Memorial Day, Independence Day, Labor Day, Halloween,
  Thanksgiving, Christmas Eve, Christmas Day, New Year's Eve

Fiscal calendar uses a 4-4-5 retail calendar pattern (52 weeks per year).
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from knot_shore.config import (
    CALENDAR_END,
    CALENDAR_START,
    DEPARTMENTS,
    STORES,
)


# ---------------------------------------------------------------------------
# Holiday helpers
# ---------------------------------------------------------------------------

def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    """Return the nth occurrence of weekday (0=Mon … 6=Sun) in year/month."""
    first = date(year, month, 1)
    # day of week of the 1st
    offset = (weekday - first.weekday()) % 7
    first_occurrence = first + timedelta(days=offset)
    return first_occurrence + timedelta(weeks=n - 1)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of weekday (0=Mon … 6=Sun) in year/month."""
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)


def _easter(year: int) -> date:
    """Meeus/Jones/Butcher algorithm for Easter Sunday."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _holidays_for_year(year: int) -> dict[date, str]:
    """Return {date: holiday_name} for all tracked holidays in a given year."""
    holidays: dict[date, str] = {}

    # New Year's Day
    holidays[date(year, 1, 1)] = "New Year's Day"

    # Super Bowl Sunday: 2nd Sunday of February
    holidays[_nth_weekday_of_month(year, 2, 6, 2)] = "Super Bowl Sunday"

    # Valentine's Day
    holidays[date(year, 2, 14)] = "Valentine's Day"

    # Easter Sunday
    holidays[_easter(year)] = "Easter Sunday"

    # Memorial Day: last Monday of May
    holidays[_last_weekday_of_month(year, 5, 0)] = "Memorial Day"

    # Independence Day
    holidays[date(year, 7, 4)] = "Independence Day"

    # Labor Day: first Monday of September
    holidays[_nth_weekday_of_month(year, 9, 0, 1)] = "Labor Day"

    # Halloween
    holidays[date(year, 10, 31)] = "Halloween"

    # Thanksgiving: 4th Thursday of November
    holidays[_nth_weekday_of_month(year, 11, 3, 4)] = "Thanksgiving"

    # Christmas Eve
    holidays[date(year, 12, 24)] = "Christmas Eve"

    # Christmas Day
    holidays[date(year, 12, 25)] = "Christmas Day"

    # New Year's Eve
    holidays[date(year, 12, 31)] = "New Year's Eve"

    return holidays


# ---------------------------------------------------------------------------
# Fiscal period (4-4-5) helper
# ---------------------------------------------------------------------------

# 4-4-5 pattern: 12 periods, week counts [4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5]
_445_PATTERN: list[int] = [4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5]
# Cumulative week boundaries: period n ends after sum of first n entries
_445_CUMULATIVE: list[int] = []
_running = 0
for _wks in _445_PATTERN:
    _running += _wks
    _445_CUMULATIVE.append(_running)


def _fiscal_period_from_week(fiscal_week: int) -> int:
    """Map fiscal_week (1-52) to fiscal period (1-12) using 4-4-5 pattern."""
    for period, boundary in enumerate(_445_CUMULATIVE, start=1):
        if fiscal_week <= boundary:
            return period
    return 12


# ---------------------------------------------------------------------------
# Public generators
# ---------------------------------------------------------------------------

def generate_dim_stores() -> pd.DataFrame:
    """Generate the dim_stores DataFrame from config.STORES."""
    rows = []
    for s in STORES:
        rows.append(
            {
                "store_id": s["store_id"],
                "store_name": s["store_name"],
                "address": s["address"],
                "city": s["city"],
                "zip": s["zip"],
                "county_fips": s["county_fips"],
                "trade_area_profile": s["trade_area_profile"],
                "sqft": s["sqft"],
                "open_date": s["open_date"],
                "base_daily_revenue": s["base_daily_revenue"],
            }
        )
    return pd.DataFrame(rows)


def generate_dim_departments() -> pd.DataFrame:
    """Generate the dim_departments DataFrame from config.DEPARTMENTS."""
    rows = []
    for d in DEPARTMENTS:
        rows.append(
            {
                "department_id": d["department_id"],
                "department_name": d["department_name"],
                "is_perishable": d["is_perishable"],
                "seasonal_profile": d["seasonal_profile"],
                "base_margin_pct": d["base_margin_pct"],
            }
        )
    return pd.DataFrame(rows)


def generate_dim_calendar() -> pd.DataFrame:
    """Generate the dim_calendar DataFrame covering CALENDAR_START through CALENDAR_END."""
    # Build holiday lookup for every year in range
    years = range(CALENDAR_START.year, CALENDAR_END.year + 1)
    holiday_map: dict[date, str] = {}
    for yr in years:
        holiday_map.update(_holidays_for_year(yr))

    rows = []
    current = CALENDAR_START
    while current <= CALENDAR_END:
        dow_num = current.isoweekday()  # 1=Monday, 7=Sunday
        day_name = current.strftime("%A")
        is_weekend = dow_num >= 6
        is_snap = current.day <= 10

        # ISO calendar week, capped at 52
        iso_week = current.isocalendar()[1]
        fiscal_week = min(iso_week, 52)

        fiscal_period = _fiscal_period_from_week(fiscal_week)

        is_holiday = current in holiday_map
        holiday_name = holiday_map.get(current, None)

        rows.append(
            {
                "date_key": current,
                "day_of_week": day_name,
                "day_of_week_num": dow_num,
                "is_weekend": is_weekend,
                "is_holiday": is_holiday,
                "holiday_name": holiday_name,
                "is_snap_window": is_snap,
                "fiscal_week": fiscal_week,
                "fiscal_period": fiscal_period,
                "month": current.month,
                "quarter": (current.month - 1) // 3 + 1,
                "year": current.year,
            }
        )
        current += timedelta(days=1)

    return pd.DataFrame(rows)
