"""
test_reports.py

Baseline structural coverage for reports.py. Asserts on required section
headers, note-branch selection, and file fan-out — not on specific dollar
values or seed-dependent numeric output.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from knot_shore.config import DEPARTMENTS, STORES
from knot_shore.promotions import generate_promotions
from knot_shore.reports import _ANOMALY_NOTES, generate_all_reports, generate_store_report
from knot_shore.sales_generator import generate_day

TEST_DATE = date(2025, 6, 10)

_REQUIRED_SECTIONS = [
    "KNOT SHORE GROCERY",
    "DEPARTMENT PERFORMANCE",
    "STORE TOTAL",
    "Labor Cost:",
    "ACTIVE PROMOTIONS",
    "NOTES",
]


@pytest.fixture(scope="module")
def report_frames():
    promos = generate_promotions(seed=42)
    dept_df, summary_df = generate_day(
        target_date=TEST_DATE,
        stores=STORES,
        departments=DEPARTMENTS,
        promos_df=promos,
        global_seed=42,
    )
    return dept_df, summary_df, promos


def test_generate_store_report_sections_and_store_total(report_frames):
    """The report carries every required section and reports this store's
    identity and store-total net sales correctly for the input data."""
    dept_df, summary_df, promos_df = report_frames
    store = STORES[0]
    txt = generate_store_report(
        store, dept_df, summary_df, promos_df, TEST_DATE, anomaly_log_df=pd.DataFrame()
    )
    for section in _REQUIRED_SECTIONS:
        assert section in txt, f"Required section '{section}' missing from report"

    # The header identifies this store by zero-padded id.
    assert f"(#{store['store_id']:03d})" in txt, "Report header missing store id"

    # The STORE TOTAL line reports the sum of this store's department
    # net_sales, formatted the way the report formats currency ($X,XXX).
    store_dept = dept_df[dept_df["store_id"] == store["store_id"]]
    expected_total = f"${store_dept['net_sales'].sum():,.0f}"
    store_total_line = next(
        line for line in txt.splitlines() if line.startswith("STORE TOTAL")
    )
    assert expected_total in store_total_line, (
        f"STORE TOTAL line {store_total_line!r} missing net sales {expected_total}"
    )


def test_routine_note_used_when_no_anomaly(report_frames):
    dept_df, summary_df, promos_df = report_frames
    txt = generate_store_report(
        STORES[0], dept_df, summary_df, promos_df, TEST_DATE, anomaly_log_df=pd.DataFrame()
    )
    assert not any(n in txt for n in _ANOMALY_NOTES), (
        "Anomaly note appeared in a report with no anomalies"
    )


def test_anomaly_note_used_when_store_has_anomaly(report_frames):
    dept_df, summary_df, promos_df = report_frames
    log = pd.DataFrame([{
        "date_key": TEST_DATE,
        "store_id": STORES[0]["store_id"],
        "department_id": 1,
        "anomaly_type": "integrity_breach",
        "description": "x",
    }])
    txt = generate_store_report(
        STORES[0], dept_df, summary_df, promos_df, TEST_DATE, anomaly_log_df=log
    )
    assert any(n in txt for n in _ANOMALY_NOTES), (
        "No anomaly note in report when store has an anomaly entry"
    )


def test_generate_all_reports_writes_one_correct_file_per_store(tmp_path, report_frames):
    """generate_all_reports writes exactly one report per store, and each
    file is the report for the store its filename names."""
    dept_df, summary_df, promos_df = report_frames
    generate_all_reports(
        TEST_DATE, dept_df, summary_df, promos_df, tmp_path, anomaly_log_df=pd.DataFrame()
    )
    report_dir = tmp_path / "reports" / TEST_DATE.isoformat()
    out = sorted(report_dir.glob("store_*_report.txt"))
    assert len(out) == 8, f"Expected 8 report files, got {len(out)}"

    for store in STORES:
        path = report_dir / f"store_{store['store_id']:03d}_report.txt"
        assert path.exists(), f"Missing report file for store {store['store_id']}"
        txt = path.read_text(encoding="utf-8")
        assert f"(#{store['store_id']:03d})" in txt, (
            f"{path.name} does not identify store {store['store_id']}"
        )
