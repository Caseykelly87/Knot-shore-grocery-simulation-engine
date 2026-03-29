"""
test_anomaly_injection.py

Verify that anomaly injection:
  - Produces an anomaly_log with correct schema (even when empty)
  - Creates the correct data effect for each anomaly type
  - Does not inject more than one anomaly per store per date
  - anomaly_summary() returns correct counts
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from knot_shore.anomalies import (
    TYPE_DUPLICATE,
    TYPE_INTEGRITY,
    TYPE_MARGIN,
    TYPE_MISSING,
    _inject_duplicate_row,
    _inject_integrity_breach,
    _inject_margin_outlier,
    _inject_missing_department,
    anomaly_summary,
    inject,
)
from knot_shore.config import DEPARTMENTS, GLOBAL_SEED, STORES
from knot_shore.promotions import generate_promotions
from knot_shore.sales_generator import generate_day

TEST_DATE = date(2025, 4, 10)
STORE_ID = 1
DEPT_ID = 1  # Produce


@pytest.fixture(scope="module")
def base_dept_df():
    promos = generate_promotions(seed=GLOBAL_SEED)
    dept_df, _ = generate_day(
        target_date=TEST_DATE,
        stores=STORES,
        departments=DEPARTMENTS,
        promos_df=promos,
        global_seed=GLOBAL_SEED,
    )
    return dept_df


@pytest.fixture(scope="module")
def base_summary_df():
    promos = generate_promotions(seed=GLOBAL_SEED)
    _, summary_df = generate_day(
        target_date=TEST_DATE,
        stores=STORES,
        departments=DEPARTMENTS,
        promos_df=promos,
        global_seed=GLOBAL_SEED,
    )
    return summary_df


# ---------------------------------------------------------------------------
# Schema test
# ---------------------------------------------------------------------------

def test_anomaly_log_columns(base_dept_df, base_summary_df):
    """anomaly_log always has the required columns, even with 0 data rows."""
    _, _, log = inject(
        base_dept_df.copy(), base_summary_df.copy(), TEST_DATE, global_seed=99999
    )
    expected_cols = {"date_key", "store_id", "department_id", "anomaly_type", "description"}
    assert expected_cols <= set(log.columns), \
        f"Missing columns: {expected_cols - set(log.columns)}"


# ---------------------------------------------------------------------------
# Individual injector behaviour
# ---------------------------------------------------------------------------

def test_integrity_breach_breaks_net_sales(base_dept_df, base_summary_df):
    """_inject_integrity_breach adds an offset to net_sales, breaking the invariant."""
    rng = np.random.default_rng(0)
    mod_dept, description = _inject_integrity_breach(
        base_dept_df.copy(), STORE_ID, DEPT_ID, rng
    )

    mask = (mod_dept["store_id"] == STORE_ID) & (mod_dept["department_id"] == DEPT_ID)
    row = mod_dept[mask].iloc[0]
    # net_sales should now differ from gross_sales − discount_amount
    expected_net = round(row["gross_sales"] - row["discount_amount"], 2)
    assert abs(row["net_sales"] - expected_net) > 0.01, \
        "Integrity breach did not change net_sales"
    assert TYPE_INTEGRITY in description or "net_sales" in description


def test_missing_department_removes_row(base_dept_df, base_summary_df):
    """_inject_missing_department removes the target dept row."""
    original_count = len(base_dept_df)
    mod_dept, mod_summary, description = _inject_missing_department(
        base_dept_df.copy(), base_summary_df.copy(), STORE_ID, DEPT_ID
    )
    assert len(mod_dept) == original_count - 1, \
        "Expected one fewer row after missing-department injection"

    # The missing dept should not appear
    mask = (mod_dept["store_id"] == STORE_ID) & (mod_dept["department_id"] == DEPT_ID)
    assert not mask.any(), "Removed department still present in dept_df"

    # Summary totals should reflect only remaining depts
    remaining = mod_dept[mod_dept["store_id"] == STORE_ID]
    summary_row = mod_summary[mod_summary["store_id"] == STORE_ID].iloc[0]
    assert abs(summary_row["net_sales_total"] - remaining["net_sales"].sum()) <= 0.05


def test_margin_outlier_produces_unusual_margin(base_dept_df, base_summary_df):
    """_inject_margin_outlier produces gross_margin_pct outside normal range."""
    rng = np.random.default_rng(0)
    # Patch rng.random to take the negative-margin path
    rng_fixed = np.random.default_rng(0)

    mod_dept, description = _inject_margin_outlier(
        base_dept_df.copy(), STORE_ID, DEPT_ID, rng_fixed
    )

    mask = (mod_dept["store_id"] == STORE_ID) & (mod_dept["department_id"] == DEPT_ID)
    margin = float(mod_dept[mask]["gross_margin_pct"].iloc[0])

    # Should be outside the normal [0.05, 0.85] range
    assert margin < 0 or margin > 0.85, \
        f"Margin outlier produced margin={margin:.4f}, expected outside [0, 0.85]"


def test_duplicate_row_adds_row(base_dept_df, base_summary_df):
    """_inject_duplicate_row adds one extra row."""
    original_count = len(base_dept_df)
    mod_dept, description = _inject_duplicate_row(
        base_dept_df.copy(), STORE_ID, DEPT_ID
    )
    assert len(mod_dept) == original_count + 1, \
        "Duplicate row injection did not add a row"

    # The target dept should appear twice for that store
    mask = (mod_dept["store_id"] == STORE_ID) & (mod_dept["department_id"] == DEPT_ID)
    assert mask.sum() == 2, f"Expected 2 rows for store={STORE_ID} dept={DEPT_ID}, got {mask.sum()}"


# ---------------------------------------------------------------------------
# inject() function integration
# ---------------------------------------------------------------------------

def test_inject_runs_without_error(base_dept_df, base_summary_df):
    """inject() should complete without exceptions for any seed."""
    for seed in [0, 1, 42, 12345]:
        dept_out, summary_out, log = inject(
            base_dept_df.copy(), base_summary_df.copy(), TEST_DATE, global_seed=seed
        )
        assert isinstance(dept_out, pd.DataFrame)
        assert isinstance(log, pd.DataFrame)


def test_inject_at_most_one_anomaly_per_store(base_dept_df, base_summary_df):
    """Each store should have at most 1 anomaly entry in the log."""
    # Run across many seeds to get varied anomaly patterns
    for seed in range(50):
        _, _, log = inject(
            base_dept_df.copy(), base_summary_df.copy(), TEST_DATE, global_seed=seed
        )
        if not log.empty:
            counts = log.groupby("store_id").size()
            assert (counts <= 1).all(), \
                f"Seed {seed}: multiple anomalies for one store: {counts[counts > 1]}"


# ---------------------------------------------------------------------------
# anomaly_summary()
# ---------------------------------------------------------------------------

def test_anomaly_summary_counts():
    log = pd.DataFrame([
        {"date_key": TEST_DATE, "store_id": 1, "department_id": 1,
         "anomaly_type": TYPE_INTEGRITY, "description": "x"},
        {"date_key": TEST_DATE, "store_id": 2, "department_id": 2,
         "anomaly_type": TYPE_MISSING, "description": "y"},
    ])
    s = anomaly_summary(log)
    assert s["total_injected"] == 2
    assert s["by_type"]["integrity_breach"] == 1
    assert s["by_type"]["missing_department"] == 1
    assert s["by_type"]["margin_outlier"] == 0
    assert s["by_type"]["duplicate_row"] == 0


def test_anomaly_summary_empty():
    log = pd.DataFrame(
        columns=["date_key", "store_id", "department_id", "anomaly_type", "description"]
    )
    s = anomaly_summary(log)
    assert s["total_injected"] == 0
    assert all(v == 0 for v in s["by_type"].values())
