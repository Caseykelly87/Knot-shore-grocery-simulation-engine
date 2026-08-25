"""Tests for the backfill date-range resolver."""

from datetime import date

import pytest

from knot_shore.cli import DEFAULT_BACKFILL_DAYS, resolve_backfill_dates


class TestResolveBackfillDates:
    """The resolver returns a contiguous list of dates ascending."""

    def test_default_window_covers_2024_and_2025(self):
        """The canonical window is the two full calendar years 2024 and 2025.

        Anchored on the default end date and the default length, so a
        change to either constant that breaks the two-year window fails
        here rather than silently shipping a different canonical dataset.
        """
        dates = resolve_backfill_dates(
            start_date=None, end_date=None, days=DEFAULT_BACKFILL_DAYS
        )
        assert dates[0] == date(2024, 1, 1)
        assert dates[-1] == date(2025, 12, 31)
        # 366 days of 2024 (leap) + 365 of 2025
        assert len(dates) == 731
        assert sum(1 for d in dates if d.year == 2024) == 366
        assert sum(1 for d in dates if d.year == 2025) == 365

    def test_explicit_end_date_with_default_days(self):
        dates = resolve_backfill_dates(start_date=None, end_date=date(2025, 9, 30), days=184)
        assert dates[-1] == date(2025, 9, 30)
        assert len(dates) == 184
        # First date is 184 days before the end (inclusive range)
        assert dates[0] == date(2025, 3, 31)

    def test_explicit_start_date_with_default_days(self):
        dates = resolve_backfill_dates(start_date=date(2025, 7, 1), end_date=None, days=184)
        assert dates[0] == date(2025, 7, 1)
        assert len(dates) == 184
        assert dates[-1] == date(2025, 12, 31)

    def test_custom_days_with_end_date(self):
        dates = resolve_backfill_dates(start_date=None, end_date=date(2025, 12, 31), days=30)
        assert len(dates) == 30
        assert dates[-1] == date(2025, 12, 31)
        assert dates[0] == date(2025, 12, 2)

    def test_custom_days_with_start_date(self):
        dates = resolve_backfill_dates(start_date=date(2025, 7, 1), end_date=None, days=7)
        assert len(dates) == 7
        assert dates[0] == date(2025, 7, 1)
        assert dates[-1] == date(2025, 7, 7)

    def test_dates_are_contiguous_and_ascending(self):
        dates = resolve_backfill_dates(
            start_date=None, end_date=None, days=DEFAULT_BACKFILL_DAYS
        )
        for i in range(1, len(dates)):
            assert (dates[i] - dates[i - 1]).days == 1

    def test_start_and_end_both_provided_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            resolve_backfill_dates(
                start_date=date(2025, 7, 1),
                end_date=date(2025, 12, 31),
                days=183,
            )

    def test_zero_days_raises(self):
        with pytest.raises(ValueError, match="days must be"):
            resolve_backfill_dates(start_date=None, end_date=None, days=0)

    def test_negative_days_raises(self):
        with pytest.raises(ValueError, match="days must be"):
            resolve_backfill_dates(start_date=None, end_date=None, days=-1)
