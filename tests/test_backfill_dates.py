"""Tests for the backfill date-range resolver."""

import pytest
from datetime import date

from knot_shore.cli import resolve_backfill_dates


class TestResolveBackfillDates:
    """The resolver returns a contiguous list of dates ascending."""

    def test_default_window_is_2025_07_01_to_2025_12_31(self):
        dates = resolve_backfill_dates(start_date=None, end_date=None, days=183)
        assert dates[0] == date(2025, 7, 2)
        assert dates[-1] == date(2025, 12, 31)
        assert len(dates) == 183

    def test_explicit_end_date_with_default_days(self):
        dates = resolve_backfill_dates(start_date=None, end_date=date(2025, 9, 30), days=183)
        assert dates[-1] == date(2025, 9, 30)
        assert len(dates) == 183
        # First date is 182 days before the end (inclusive range)
        assert dates[0] == date(2025, 4, 1)

    def test_explicit_start_date_with_default_days(self):
        dates = resolve_backfill_dates(start_date=date(2025, 7, 1), end_date=None, days=183)
        assert dates[0] == date(2025, 7, 1)
        assert len(dates) == 183
        assert dates[-1] == date(2025, 12, 30)

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
        dates = resolve_backfill_dates(start_date=None, end_date=None, days=183)
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
