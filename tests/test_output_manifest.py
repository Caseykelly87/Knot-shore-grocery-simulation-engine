"""
test_output_manifest.py

Regression tests for manifest.json integrity across cmd_run invocations.
"""

from __future__ import annotations

import json
import logging
import shutil
from collections import Counter
from datetime import date

import pandas as pd
import pytest

from knot_shore.cli import cmd_backfill, cmd_init, cmd_run


@pytest.fixture(autouse=True)
def _silence_logging():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def test_anomaly_summary_matches_disk_after_regeneration(tmp_path):
    """Manifest anomaly_summary must equal the sum of on-disk anomaly_log.csv files.

    Steps:
      1. init + run to populate date folders.
      2. Delete one folder that produced at least one anomaly.
      3. Run again with the same anchor and seed (regenerates the deleted folder).
      4. Assert manifest totals equal what is reconstructed from disk.
    """
    anchor = date(2024, 6, 15)

    cmd_init(seed=42, output_dir=tmp_path)
    cmd_run(seed=42, output_dir=tmp_path, anchor=anchor, no_realism=True)

    # Find a date folder that has at least one anomaly and delete it
    deleted = False
    for p in sorted(tmp_path.rglob("anomaly_log.csv")):
        if len(pd.read_csv(p)) > 0:
            shutil.rmtree(p.parent)
            deleted = True
            break

    assert deleted, "No anomaly-bearing folder found — increase the date range or seed"

    cmd_run(seed=42, output_dir=tmp_path, anchor=anchor, no_realism=True)

    manifest = json.loads((tmp_path / "manifest.json").read_text())

    on_disk_total = 0
    on_disk_by_type: Counter = Counter()
    for p in tmp_path.rglob("anomaly_log.csv"):
        df = pd.read_csv(p)
        on_disk_total += len(df)
        if len(df):
            on_disk_by_type.update(df["anomaly_type"])

    assert manifest["anomaly_summary"]["total_injected"] == on_disk_total, (
        f"Manifest total_injected={manifest['anomaly_summary']['total_injected']} "
        f"but on-disk count={on_disk_total}"
    )
    for k in ("integrity_breach", "missing_department", "margin_outlier", "duplicate_row"):
        assert manifest["anomaly_summary"]["by_type"][k] == on_disk_by_type.get(k, 0), (
            f"Manifest by_type[{k}]={manifest['anomaly_summary']['by_type'][k]} "
            f"but on-disk count={on_disk_by_type.get(k, 0)}"
        )


def test_incremental_manifest_two_window_backfills(tmp_path):
    """Two non-overlapping backfills accumulate cumulative_row_counts correctly.

    Business-correctness: the incremental refactor reads counters from the
    previous manifest and adds only newly-written dates. If it were broken
    (e.g., reset counts each call, or rescanned the wrong subset), totals
    after the second backfill would diverge from the independent disk sum.
    """
    cmd_init(seed=42, output_dir=tmp_path)

    cmd_backfill(
        seed=42, output_dir=tmp_path,
        start_date=date(2024, 1, 1), end_date=None, days=7,
        no_realism=True,
    )
    manifest_1 = json.loads((tmp_path / "manifest.json").read_text())
    counts_1 = dict(manifest_1["cumulative_row_counts"])

    cmd_backfill(
        seed=42, output_dir=tmp_path,
        start_date=date(2024, 2, 1), end_date=None, days=5,
        no_realism=True,
    )
    manifest_2 = json.loads((tmp_path / "manifest.json").read_text())
    counts_2 = manifest_2["cumulative_row_counts"]

    # Independent on-disk reconstruction
    expected_dept = sum(len(pd.read_csv(p)) for p in tmp_path.rglob("department_sales.csv"))
    expected_summary = sum(len(pd.read_csv(p)) for p in tmp_path.rglob("store_summary.csv"))

    assert counts_2["department_sales"] == expected_dept, (
        f"Manifest department_sales={counts_2['department_sales']} but disk={expected_dept}"
    )
    assert counts_2["store_summary"] == expected_summary, (
        f"Manifest store_summary={counts_2['store_summary']} but disk={expected_summary}"
    )

    assert counts_2["department_sales"] > counts_1["department_sales"]
    assert counts_2["store_summary"] > counts_1["store_summary"]

    expected_anomaly_total = 0
    for p in tmp_path.rglob("anomaly_log.csv"):
        expected_anomaly_total += len(pd.read_csv(p))
    assert manifest_2["anomaly_summary"]["total_injected"] == expected_anomaly_total

    assert manifest_2["total_dates_generated"] == 12


def test_rebuild_manifest_recovers_from_drift(tmp_path):
    """--rebuild-manifest path reconciles counts after a date-folder is removed.

    Structural: after deletion the incremental path keeps stale counts
    because the deleted date is still in dates_generated; --rebuild
    re-scans everything in dates_generated and the totals collapse to
    what disk actually holds.
    """
    cmd_init(seed=42, output_dir=tmp_path)
    cmd_backfill(
        seed=42, output_dir=tmp_path,
        start_date=date(2024, 3, 1), end_date=None, days=5,
        no_realism=True,
    )
    manifest_before = json.loads((tmp_path / "manifest.json").read_text())
    dept_before = manifest_before["cumulative_row_counts"]["department_sales"]

    daily_path = next(tmp_path.rglob("department_sales.csv")).parent
    shutil.rmtree(daily_path)

    cmd_run(
        seed=42, output_dir=tmp_path, anchor=date(2024, 3, 10),
        no_realism=True, rebuild_manifest=True,
    )

    manifest_after = json.loads((tmp_path / "manifest.json").read_text())
    expected_dept = sum(
        len(pd.read_csv(p)) for p in tmp_path.rglob("department_sales.csv")
    )

    assert manifest_after["cumulative_row_counts"]["department_sales"] == expected_dept
    assert manifest_after["cumulative_row_counts"]["department_sales"] != dept_before
