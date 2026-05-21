"""
test_realism_skip.py

Verify the realism layer's skip paths.

The realism layer resolves its data source via a three-tier precedence:
database → bundled fixture → skip. This file covers the two paths that
end in "skipped (DataFrames returned unchanged)":

  - force_disable=True (always skip, regardless of source availability)
  - no database AND no bundled fixture (broken-install state)

The fixture-fallback path (no DB but fixture present) is covered in
test_realism_fixture_fallback.py.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

TEST_DATE = date(2024, 9, 10)


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


def test_is_available_false_when_force_disabled():
    """is_available(force_disable=True) is always False even if a source exists."""
    import knot_shore.realism as realism
    realism.clear_cache()
    assert not realism.is_available(force_disable=True)


def test_adjust_returns_unchanged_when_no_db_and_no_fixture(monkeypatch, tmp_path):
    """No database AND no bundled fixture → Stage 2 is skipped, dfs unchanged.

    Simulates a broken-install state by pointing the fixture path at a
    location that doesn't exist.
    """
    monkeypatch.delenv("KNOT_SHORE_DB_URL", raising=False)

    import knot_shore.realism as realism
    monkeypatch.setattr(realism, "BUNDLED_FIXTURE_PATH", tmp_path / "missing.parquet")
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

    assert not realism.is_available()
    dept_out, summary_out = realism.adjust(dept_df, summary_df, TEST_DATE)

    pd.testing.assert_frame_equal(dept_df, dept_out)
    pd.testing.assert_frame_equal(summary_df, summary_out)


def test_full_pipeline_runs_without_db(monkeypatch, tmp_path):
    """Full init + run pipeline writes correctly-shaped output with --no-realism.

    Verifies not just that the daily files appear, but that they carry the
    expected schema and cover all eight stores and all ten departments.
    Row counts are not pinned exactly because anomaly injection may add or
    drop a row; store and department coverage is the stable invariant.
    """
    monkeypatch.delenv("KNOT_SHORE_DB_URL", raising=False)

    import knot_shore.realism as realism
    realism.clear_cache()

    from knot_shore.cli import cmd_init, cmd_run
    from knot_shore.config import DEPARTMENTS, STORES

    cmd_init(seed=42, output_dir=tmp_path)
    cmd_run(seed=42, output_dir=tmp_path, anchor=TEST_DATE, no_realism=True)

    from knot_shore.output import daily_dir_for
    daily_dir = daily_dir_for(tmp_path, TEST_DATE)

    dept_path = daily_dir / "department_sales.csv"
    summary_path = daily_dir / "store_summary.csv"
    assert dept_path.exists(), "department_sales.csv not written"
    assert summary_path.exists(), "store_summary.csv not written"

    dept = pd.read_csv(dept_path)
    summary = pd.read_csv(summary_path)

    expected_store_ids = {s["store_id"] for s in STORES}
    expected_dept_ids = {d["department_id"] for d in DEPARTMENTS}

    # store_summary: exactly one row per store.
    assert len(summary) == len(expected_store_ids), (
        f"Expected {len(expected_store_ids)} store summary rows, got {len(summary)}"
    )
    assert set(summary["store_id"]) == expected_store_ids
    assert {"store_id", "net_sales_total", "transactions_total"} <= set(summary.columns)

    # department_sales: every store and every department represented.
    assert {"store_id", "department_id", "gross_sales", "net_sales"} <= set(dept.columns)
    assert set(dept["store_id"]) == expected_store_ids, "Not all stores in department_sales"
    assert set(dept["department_id"]) == expected_dept_ids, "Not all departments present"
    assert (dept["net_sales"] >= 0).all(), "Negative net_sales in pipeline output"
