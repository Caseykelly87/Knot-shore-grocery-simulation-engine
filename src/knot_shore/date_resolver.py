"""
date_resolver.py — Resolves the set of dates to generate for a single run.

A run produces data for the anchor date, the six preceding calendar days,
and the calendar-matched date one year prior — eight dates total (deduplicated,
sorted ascending).
"""

from __future__ import annotations

from datetime import date, timedelta


def resolve_required_dates(anchor: date) -> list[date]:
    """Return the eight dates a run must generate for the given anchor.

    The set is: the seven-day trailing window ending on anchor
    (anchor, anchor-1, …, anchor-6) plus the same calendar date one year
    prior.  When anchor is Feb 29 and the prior year has no Feb 29, the
    prior-year date falls back to Feb 28.

    Returns dates sorted chronologically ascending, deduplicated.
    """
    trailing = [anchor - timedelta(days=i) for i in range(7)]

    prior_year = anchor.year - 1
    try:
        prior = anchor.replace(year=prior_year)
    except ValueError:
        prior = date(prior_year, 2, 28)

    return sorted(set(trailing) | {prior})
