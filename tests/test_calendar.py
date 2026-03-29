"""
test_calendar.py

Verify dim_calendar correctness:
  - Row count: 1,461 rows (2023-01-01 through 2026-12-31)
  - SNAP windows: day-of-month 1-10 only
  - Holidays: known dates flagged correctly
  - Fiscal periods: 1-12, covering 4-4-5 pattern
  - Weekend flags: Saturday (6) and Sunday (7) only
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from knot_shore.dimensions import generate_dim_calendar


@pytest.fixture(scope="module")
def calendar_df():
    return generate_dim_calendar()


def test_row_count(calendar_df):
    # 2023-01-01 to 2026-12-31 inclusive = 1461 days
    assert len(calendar_df) == 1461, f"Expected 1461 rows, got {len(calendar_df)}"


def test_date_range(calendar_df):
    assert calendar_df["date_key"].min() == date(2023, 1, 1)
    assert calendar_df["date_key"].max() == date(2026, 12, 31)


def test_snap_window_definition(calendar_df):
    """is_snap_window must be True for days 1–10, False otherwise."""
    snap_true = calendar_df[calendar_df["is_snap_window"]]
    snap_false = calendar_df[~calendar_df["is_snap_window"]]

    assert (snap_true["date_key"].apply(lambda d: d.day) <= 10).all(), \
        "SNAP window True for day > 10"
    assert (snap_false["date_key"].apply(lambda d: d.day) > 10).all(), \
        "SNAP window False for day <= 10"


def test_weekend_flag(calendar_df):
    """is_weekend must be True for Saturday/Sunday (dow_num 6/7), False otherwise."""
    weekend_true = calendar_df[calendar_df["is_weekend"]]
    weekend_false = calendar_df[~calendar_df["is_weekend"]]

    assert weekend_true["day_of_week_num"].isin([6, 7]).all(), \
        "is_weekend=True for non-weekend day"
    assert (~weekend_false["day_of_week_num"].isin([6, 7])).all(), \
        "is_weekend=False for weekend day"


def test_day_of_week_num_range(calendar_df):
    assert calendar_df["day_of_week_num"].between(1, 7).all()


def test_known_holidays(calendar_df):
    """Check a sample of known holiday dates."""
    cal = calendar_df.set_index("date_key")

    # New Year's Day 2024
    row = cal.loc[date(2024, 1, 1)]
    assert row["is_holiday"], "2024-01-01 should be a holiday"
    assert row["holiday_name"] == "New Year's Day"

    # Thanksgiving 2024 = 4th Thursday of November = Nov 28
    row = cal.loc[date(2024, 11, 28)]
    assert row["is_holiday"], "2024-11-28 should be Thanksgiving"
    assert "Thanksgiving" in row["holiday_name"]

    # Christmas Day 2025
    row = cal.loc[date(2025, 12, 25)]
    assert row["is_holiday"]
    assert row["holiday_name"] == "Christmas Day"

    # Independence Day 2023
    row = cal.loc[date(2023, 7, 4)]
    assert row["is_holiday"]
    assert row["holiday_name"] == "Independence Day"


def test_non_holidays_not_flagged(calendar_df):
    cal = calendar_df.set_index("date_key")
    # A random mid-week date that is definitely not a holiday
    row = cal.loc[date(2024, 3, 20)]
    assert not row["is_holiday"]
    assert row["holiday_name"] is None or pd.isna(row["holiday_name"])


def test_fiscal_period_range(calendar_df):
    assert calendar_df["fiscal_period"].between(1, 12).all()


def test_fiscal_week_range(calendar_df):
    assert calendar_df["fiscal_week"].between(1, 52).all()


def test_month_range(calendar_df):
    assert calendar_df["month"].between(1, 12).all()


def test_quarter_range(calendar_df):
    assert calendar_df["quarter"].between(1, 4).all()


def test_no_duplicate_dates(calendar_df):
    assert calendar_df["date_key"].nunique() == len(calendar_df), \
        "Duplicate date_key values found in dim_calendar"


def test_dow_name_matches_num(calendar_df):
    """day_of_week string should match day_of_week_num."""
    day_names = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday",
                 5: "Friday", 6: "Saturday", 7: "Sunday"}
    for num, name in day_names.items():
        subset = calendar_df[calendar_df["day_of_week_num"] == num]
        wrong = subset[subset["day_of_week"] != name]
        assert wrong.empty, f"dow_num={num} has wrong day_of_week name(s)"
