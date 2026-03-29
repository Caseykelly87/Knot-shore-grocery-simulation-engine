"""
test_deterministic_seed.py

Verify that running the generator twice for the same date + seed produces
identical output (§4.9).

Also verifies that different dates produce different output (not identical seeds).
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from knot_shore.config import DEPARTMENTS, GLOBAL_SEED, STORES
from knot_shore.promotions import generate_promotions
from knot_shore.sales_generator import generate_day

_PROMOS = None


def _get_promos():
    global _PROMOS
    if _PROMOS is None:
        _PROMOS = generate_promotions(seed=GLOBAL_SEED)
    return _PROMOS


def _gen(d: date):
    return generate_day(
        target_date=d,
        stores=STORES,
        departments=DEPARTMENTS,
        promos_df=_get_promos(),
        global_seed=GLOBAL_SEED,
    )


def test_same_date_same_output():
    """Running twice with the same date and seed must produce identical DataFrames."""
    d = date(2024, 8, 15)
    dept1, sum1 = _gen(d)
    dept2, sum2 = _gen(d)

    pd.testing.assert_frame_equal(
        dept1.reset_index(drop=True),
        dept2.reset_index(drop=True),
        check_exact=False,
        rtol=1e-6,
    )
    pd.testing.assert_frame_equal(
        sum1.reset_index(drop=True),
        sum2.reset_index(drop=True),
        check_exact=False,
        rtol=1e-6,
    )


def test_different_dates_produce_different_gross_sales():
    """Two different dates must produce different gross_sales values."""
    d1 = date(2024, 5, 1)
    d2 = date(2024, 5, 2)
    dept1, _ = _gen(d1)
    dept2, _ = _gen(d2)

    # Gross sales for the same store+dept should differ across dates
    s1 = dept1[["store_id", "department_id", "gross_sales"]].sort_values(
        ["store_id", "department_id"]
    )
    s2 = dept2[["store_id", "department_id", "gross_sales"]].sort_values(
        ["store_id", "department_id"]
    )
    identical = (s1["gross_sales"].values == s2["gross_sales"].values).all()
    assert not identical, "Different dates produced identical gross_sales — seeding broken"


def test_different_seeds_produce_different_output():
    """Different global seeds must produce different noise values."""
    d = date(2024, 6, 20)
    dept_42, _ = generate_day(
        target_date=d, stores=STORES, departments=DEPARTMENTS,
        promos_df=_get_promos(), global_seed=42,
    )
    dept_99, _ = generate_day(
        target_date=d, stores=STORES, departments=DEPARTMENTS,
        promos_df=_get_promos(), global_seed=99,
    )
    assert not (dept_42["gross_sales"].values == dept_99["gross_sales"].values).all(), \
        "Different seeds produced identical gross_sales"


def test_promo_generation_deterministic():
    """generate_promotions with same seed twice gives identical DataFrame."""
    p1 = generate_promotions(seed=GLOBAL_SEED)
    p2 = generate_promotions(seed=GLOBAL_SEED)
    pd.testing.assert_frame_equal(p1, p2)
