"""
test_realism_seeding.py

Verify that Stage 2 re-derivation noise respects global_seed.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import knot_shore.realism as realism
from knot_shore.config import DEPARTMENTS, STORES
from knot_shore.promotions import generate_promotions
from knot_shore.sales_generator import generate_day

_DATE = date(2024, 8, 1)


@pytest.fixture(autouse=True)
def _reset_realism_cache():
    # The realism layer caches resolved-source state in module globals.
    # Other test files (notably the fixture-fallback suite) can leave that
    # cache set to "none", which makes adjust() early-return and short-
    # circuit the seeding path these tests are meant to exercise. The
    # bundled-fixture path is the intended source when no DB is configured.
    realism.clear_cache()
    yield
    realism.clear_cache()


@pytest.fixture(scope="module")
def _base_frames():
    promos = generate_promotions(seed=42)
    dept_df, sum_df = generate_day(
        target_date=_DATE,
        stores=STORES,
        departments=DEPARTMENTS,
        promos_df=promos,
        global_seed=42,
    )
    return dept_df, sum_df


def test_stage2_rederivation_noise_respects_global_seed(_base_frames):
    """adjust() with seeds 42 and 99 must differ on at least one transactions value."""
    dept_df, sum_df = _base_frames
    engine = MagicMock()

    with patch("knot_shore.realism._get_engine", return_value=engine), \
         patch("knot_shore.realism._sales_volume_multiplier", return_value=1.05), \
         patch("knot_shore.realism._margin_adjustment", return_value=0.0), \
         patch("knot_shore.realism._labor_cost_multiplier", return_value=1.0):
        a_dept, _ = realism.adjust(dept_df.copy(), sum_df.copy(), _DATE, global_seed=42)
        b_dept, _ = realism.adjust(dept_df.copy(), sum_df.copy(), _DATE, global_seed=99)

    assert not (a_dept["transactions"].values == b_dept["transactions"].values).all(), \
        "Stage 2 transactions identical across seeds 42 and 99 — global_seed is not threaded"


def test_stage2_same_seed_reproducible(_base_frames):
    """Two adjust() calls with the same seed must produce identical DataFrames."""
    dept_df, sum_df = _base_frames
    engine = MagicMock()

    with patch("knot_shore.realism._get_engine", return_value=engine), \
         patch("knot_shore.realism._sales_volume_multiplier", return_value=1.05), \
         patch("knot_shore.realism._margin_adjustment", return_value=0.0), \
         patch("knot_shore.realism._labor_cost_multiplier", return_value=1.0):
        a_dept, _ = realism.adjust(dept_df.copy(), sum_df.copy(), _DATE, global_seed=42)
        b_dept, _ = realism.adjust(dept_df.copy(), sum_df.copy(), _DATE, global_seed=42)

    pd.testing.assert_frame_equal(a_dept.reset_index(drop=True), b_dept.reset_index(drop=True))
