"""Tests for resolve_required_dates."""

from datetime import date

import pytest

from knot_shore.date_resolver import resolve_required_dates


def test_ordinary_anchor():
    anchor = date(2025, 6, 15)
    result = resolve_required_dates(anchor)
    expected = sorted([
        date(2025, 6, 9),
        date(2025, 6, 10),
        date(2025, 6, 11),
        date(2025, 6, 12),
        date(2025, 6, 13),
        date(2025, 6, 14),
        date(2025, 6, 15),
        date(2024, 6, 15),
    ])
    assert result == expected


def test_leap_day_anchor():
    anchor = date(2024, 2, 29)
    result = resolve_required_dates(anchor)
    expected = sorted([
        date(2024, 2, 23),
        date(2024, 2, 24),
        date(2024, 2, 25),
        date(2024, 2, 26),
        date(2024, 2, 27),
        date(2024, 2, 28),
        date(2024, 2, 29),
        date(2023, 2, 28),
    ])
    assert result == expected


@pytest.mark.parametrize("anchor", [
    date(2025, 1, 1),
    date(2025, 7, 4),
    date(2025, 12, 31),
    date(2026, 3, 15),
])
def test_always_eight_unique_dates(anchor):
    result = resolve_required_dates(anchor)
    assert len(result) == 8
    assert len(set(result)) == 8


@pytest.mark.parametrize("anchor", [
    date(2025, 1, 1),
    date(2025, 6, 15),
    date(2024, 2, 29),
    date(2026, 3, 15),
])
def test_always_sorted_ascending(anchor):
    result = resolve_required_dates(anchor)
    assert result == sorted(result)
