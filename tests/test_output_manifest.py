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

from knot_shore.cli import cmd_init, cmd_run


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
