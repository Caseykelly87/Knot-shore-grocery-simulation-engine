"""
test_summary_matches_detail.py

Verify that store_summary totals match the sum of department rows (§3.2).

  gross_sales_total  = SUM(dept gross_sales)   per store+date
  net_sales_total    = SUM(dept net_sales)      per store+date
  transactions_total = SUM(dept transactions)  per store+date

labor_cost is store-level only and has no department breakdown — no test for that.
"""

from __future__ import annotations

import pandas as pd
import pytest

TOLERANCE = 0.05  # $0.05 rounding tolerance on aggregations


def _dept_agg(dept_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate dept_df to store-level totals."""
    return (
        dept_df.groupby(["date_key", "store_id"], as_index=False)
        .agg(
            gross_sales_sum=("gross_sales", "sum"),
            net_sales_sum=("net_sales", "sum"),
            transactions_sum=("transactions", "sum"),
        )
    )


def test_gross_sales_total_matches_dept_sum(dept_df, summary_df):
    agg = _dept_agg(dept_df)
    merged = summary_df.merge(agg, on=["date_key", "store_id"])
    diff = (merged["gross_sales_total"] - merged["gross_sales_sum"]).abs()
    assert diff.max() <= TOLERANCE, (
        f"gross_sales_total ≠ SUM(dept gross_sales) — max diff {diff.max():.4f}"
    )


def test_net_sales_total_matches_dept_sum(dept_df, summary_df):
    agg = _dept_agg(dept_df)
    merged = summary_df.merge(agg, on=["date_key", "store_id"])
    diff = (merged["net_sales_total"] - merged["net_sales_sum"]).abs()
    assert diff.max() <= TOLERANCE, (
        f"net_sales_total ≠ SUM(dept net_sales) — max diff {diff.max():.4f}"
    )


def test_transactions_total_matches_dept_sum(dept_df, summary_df):
    agg = _dept_agg(dept_df)
    merged = summary_df.merge(agg, on=["date_key", "store_id"])
    diff = (merged["transactions_total"] - merged["transactions_sum"]).abs()
    assert (diff == 0).all(), (
        f"transactions_total ≠ SUM(dept transactions) — max diff {diff.max()}"
    )


def test_summary_has_one_row_per_store(summary_df):
    # For a single date there should be exactly 8 rows
    assert len(summary_df) == 8, f"Expected 8 summary rows, got {len(summary_df)}"


def test_dept_has_expected_row_count(dept_df):
    # 8 stores × 10 departments = 80 rows for a clean date
    assert len(dept_df) == 80, f"Expected 80 dept rows, got {len(dept_df)}"


def test_labor_cost_pct_derivation(summary_df):
    mask = summary_df["net_sales_total"] != 0
    df = summary_df[mask]
    expected = (df["labor_cost"] / df["net_sales_total"]).round(4)
    diff = (df["labor_cost_pct"] - expected).abs()
    assert diff.max() <= 0.001, (
        f"labor_cost_pct ≠ labor_cost / net_sales_total (max diff {diff.max():.6f})"
    )
