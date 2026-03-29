"""
test_promo_no_overlap.py

Verify that the promotion schedule contains no two overlapping promos
on the same department (§3.3: "No two promos should overlap on the same department").
"""

from __future__ import annotations

import pytest
import pandas as pd

from knot_shore.config import GLOBAL_SEED
from knot_shore.promotions import generate_promotions


@pytest.fixture(scope="module")
def promos():
    return generate_promotions(seed=GLOBAL_SEED)


def test_no_dept_overlap(promos):
    """No department should have two promos whose date ranges intersect."""
    overlaps = []
    for dept_id, group in promos.groupby("department_id"):
        rows = group.sort_values("start_date").reset_index(drop=True)
        for i in range(len(rows) - 1):
            a_end = rows.loc[i, "end_date"]
            b_start = rows.loc[i + 1, "start_date"]
            if b_start <= a_end:
                overlaps.append(
                    f"dept_id={dept_id}: promo {rows.loc[i,'promo_id']} "
                    f"({rows.loc[i,'start_date']}–{a_end}) overlaps "
                    f"promo {rows.loc[i+1,'promo_id']} ({b_start}–{rows.loc[i+1,'end_date']})"
                )
    assert not overlaps, "Department promo overlaps found:\n" + "\n".join(overlaps[:10])


def test_promo_count_reasonable(promos):
    """Should have roughly 6-10 promos per month × 48 months = 288–480 total."""
    assert 200 <= len(promos) <= 600, f"Unexpected promo count: {len(promos)}"


def test_promo_durations_in_range(promos):
    """Each promo must last 3–10 days (inclusive)."""
    durations = (
        pd.to_datetime(promos["end_date"]) - pd.to_datetime(promos["start_date"])
    ).dt.days + 1
    too_short = promos[durations < 3]
    too_long = promos[durations > 10]
    assert too_short.empty, f"Promos shorter than 3 days: {too_short[['promo_id','start_date','end_date']]}"
    assert too_long.empty, f"Promos longer than 10 days: {too_long[['promo_id','start_date','end_date']]}"


def test_discount_pct_within_type_range(promos):
    """discount_pct must be within the defined range for each promo_type (§3.3)."""
    from knot_shore.config import PROMO_DISCOUNT_RANGE

    for promo_type, (low, high) in PROMO_DISCOUNT_RANGE.items():
        subset = promos[promos["promo_type"] == promo_type]
        bad = subset[(subset["discount_pct"] < low - 0.001) | (subset["discount_pct"] > high + 0.001)]
        assert bad.empty, (
            f"{promo_type} discount_pct out of range [{low}, {high}]: "
            f"{bad[['promo_id','discount_pct']].head()}"
        )


def test_lift_factor_within_type_range(promos):
    """lift_factor must be within the defined range for each promo_type (§3.3)."""
    from knot_shore.config import PROMO_LIFT_RANGE

    for promo_type, (low, high) in PROMO_LIFT_RANGE.items():
        subset = promos[promos["promo_type"] == promo_type]
        bad = subset[(subset["lift_factor"] < low - 0.001) | (subset["lift_factor"] > high + 0.001)]
        assert bad.empty, (
            f"{promo_type} lift_factor out of range [{low}, {high}]: "
            f"{bad[['promo_id','lift_factor']].head()}"
        )


def test_all_promo_types_present(promos):
    """All four promo types should appear in the schedule."""
    from knot_shore.config import PROMO_TYPES
    found = set(promos["promo_type"].unique())
    missing = set(PROMO_TYPES) - found
    assert not missing, f"Missing promo types: {missing}"


def test_promo_ids_unique(promos):
    assert promos["promo_id"].nunique() == len(promos), "Duplicate promo_ids found"
