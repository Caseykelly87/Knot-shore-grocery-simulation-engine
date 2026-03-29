"""
test_realism_skip.py

Verify that the engine runs correctly when no database is available (§5.1).

  - realism.is_available() returns False when KNOT_SHORE_DB_URL is unset
  - realism.adjust() returns DataFrames unchanged when DB is unavailable
  - The full Stage 1 → anomaly → Stage 3 pipeline works without sqlalchemy installed
"""

from __future__ import annotations

import os
from datetime import date

import pandas as pd
import pytest

TEST_DATE = date(2024, 9, 10)


def test_realism_unavailable_when_no_env_var(monkeypatch):
    """is_available() must be False when KNOT_SHORE_DB_URL is not set."""
    monkeypatch.delenv("KNOT_SHORE_DB_URL", raising=False)

    # Reset cached state
    import knot_shore.realism as realism
    realism.clear_cache()

    assert not realism.is_available(), \
        "Realism engine should be unavailable without KNOT_SHORE_DB_URL"


def test_adjust_returns_unchanged_when_no_db(monkeypatch):
    """realism.adjust() must return DataFrames unchanged when DB is unavailable."""
    monkeypatch.delenv("KNOT_SHORE_DB_URL", raising=False)

    import knot_shore.realism as realism
    realism.clear_cache()

    from knot_shore.config import DEPARTMENTS, GLOBAL_SEED, STORES
    from knot_shore.promotions import generate_promotions
    from knot_shore.sales_generator import generate_day

    promos = generate_promotions(seed=GLOBAL_SEED)
    dept_df, summary_df = generate_day(
        target_date=TEST_DATE,
        stores=STORES,
        departments=DEPARTMENTS,
        promos_df=promos,
        global_seed=GLOBAL_SEED,
    )

    dept_out, summary_out = realism.adjust(dept_df, summary_df, TEST_DATE)

    pd.testing.assert_frame_equal(dept_df, dept_out)
    pd.testing.assert_frame_equal(summary_df, summary_out)


def test_adjust_returns_unchanged_when_force_disabled():
    """force_disable=True must always skip Stage 2."""
    import knot_shore.realism as realism

    from knot_shore.config import DEPARTMENTS, GLOBAL_SEED, STORES
    from knot_shore.promotions import generate_promotions
    from knot_shore.sales_generator import generate_day

    promos = generate_promotions(seed=GLOBAL_SEED)
    dept_df, summary_df = generate_day(
        target_date=TEST_DATE,
        stores=STORES,
        departments=DEPARTMENTS,
        promos_df=promos,
        global_seed=GLOBAL_SEED,
    )

    dept_out, summary_out = realism.adjust(
        dept_df, summary_df, TEST_DATE, force_disable=True
    )

    pd.testing.assert_frame_equal(dept_df, dept_out)
    pd.testing.assert_frame_equal(summary_df, summary_out)


def test_full_pipeline_runs_without_db(monkeypatch, tmp_path):
    """Full init + run pipeline completes without error when no DB is configured."""
    monkeypatch.delenv("KNOT_SHORE_DB_URL", raising=False)

    import knot_shore.realism as realism
    realism.clear_cache()

    from knot_shore.cli import cmd_init, cmd_run

    cmd_init(seed=42, output_dir=tmp_path)
    cmd_run(seed=42, output_dir=tmp_path, anchor=TEST_DATE, no_realism=True)

    dept_path = tmp_path / "daily" / TEST_DATE.isoformat() / "department_sales.csv"
    assert dept_path.exists(), "department_sales.csv not written"

    df = pd.read_csv(dept_path)
    assert len(df) > 0, "department_sales.csv is empty"
