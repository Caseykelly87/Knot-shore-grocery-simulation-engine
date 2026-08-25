"""Tests for the daily output directory layout.

The layout is the contract the downstream ETL source adapter walks, so
it is pinned here rather than left implicit in the writer.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from knot_shore.output import daily_dir_for


def test_daily_dir_is_year_month_day():
    d = daily_dir_for(Path("/out"), date(2024, 7, 1))
    assert d == Path("/out/daily/2024/07/01")


def test_month_and_day_are_zero_padded():
    d = daily_dir_for(Path("/out"), date(2025, 3, 9))
    assert d.parts[-3:] == ("2025", "03", "09")


def test_directories_sort_chronologically():
    """Lexical sort of the relative paths must match calendar order."""
    dates = [
        date(2024, 1, 1),
        date(2024, 2, 29),
        date(2024, 10, 5),
        date(2025, 1, 1),
        date(2025, 12, 31),
    ]
    root = Path("/out")
    paths = [daily_dir_for(root, d).as_posix() for d in dates]
    assert paths == sorted(paths)
