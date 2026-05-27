"""
output.py — Stage 3: Write DataFrames to CSV files and update manifest.json.

This is the ONLY module that touches the filesystem for data output.

Directory layout for daily data:
  output/daily/{MM}/{DD}/{YYYY}/
    department_sales.csv
    store_summary.csv
    anomaly_log.csv

This groups all years' data for the same calendar date together, so
daily/06/15/ contains subdirectories side by side — useful for browsing
year-over-year comparisons.

Responsibilities:
  - Write dimension tables (run once via init)
  - Write promotion schedule (run once via init)
  - Write daily date folder per date; skip if folder already exists (no overwrite)
  - Update manifest.json (accumulates run history)

CSV encoding: UTF-8, comma-delimited.  No Parquet.
Helper columns (base_margin_pct, avg_ticket_base, items_per_transaction,
discount_pct) are stripped from dept_df before writing — they are internal
Stage 1 scaffolding not part of the output schema.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import structlog

from knot_shore.config import GENERATOR_VERSION, GLOBAL_SEED

logger = structlog.get_logger(__name__)

# Columns to strip from dept_df before writing to CSV (Stage 1 helper cols)
_DEPT_HELPER_COLS = {"base_margin_pct", "avg_ticket_base", "items_per_transaction", "discount_pct"}

# Canonical output schema for department_sales.csv
_DEPT_OUTPUT_COLS = [
    "date_key", "store_id", "department_id",
    "gross_sales", "discount_amount", "net_sales",
    "cogs", "gross_margin", "gross_margin_pct",
    "transactions", "units_sold", "avg_ticket",
    "discount_rate", "promo_flag",
]

# Anomaly type labels tracked in manifest.json under anomaly_summary.by_type
_ANOMALY_TYPES: tuple[str, ...] = (
    "integrity_breach",
    "missing_department",
    "margin_outlier",
    "duplicate_row",
)


def daily_dir_for(output_dir: Path, target_date: date) -> Path:
    """Return the output directory for a specific date.

    Layout: output_dir/daily/{MM}/{DD}/{YYYY}/

    This groups all years' data for the same calendar date (MM/DD) together,
    making year-over-year comparison browsing natural.
    """
    return (
        output_dir
        / "daily"
        / f"{target_date.month:02d}"
        / f"{target_date.day:02d}"
        / str(target_date.year)
    )


def _strip_helpers(dept_df: pd.DataFrame) -> pd.DataFrame:
    """Remove internal helper columns before writing department_sales CSV."""
    drop_cols = [c for c in _DEPT_HELPER_COLS if c in dept_df.columns]
    return dept_df.drop(columns=drop_cols)


# ---------------------------------------------------------------------------
# Dimension and promotion writers (called once by init)
# ---------------------------------------------------------------------------

def write_dimensions(
    stores_df: pd.DataFrame,
    departments_df: pd.DataFrame,
    calendar_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    """Write the three dimension CSVs under output_dir/dimensions/.

    Skips any file that already exists.
    """
    dim_dir = output_dir / "dimensions"
    dim_dir.mkdir(parents=True, exist_ok=True)

    _write_if_new(stores_df, dim_dir / "dim_stores.csv", "dim_stores")
    _write_if_new(departments_df, dim_dir / "dim_departments.csv", "dim_departments")
    _write_if_new(calendar_df, dim_dir / "dim_calendar.csv", "dim_calendar")


def write_promotions(promos_df: pd.DataFrame, output_dir: Path) -> None:
    """Write promotions.csv under output_dir/promotions/.

    Skips if the file already exists.
    """
    promo_dir = output_dir / "promotions"
    promo_dir.mkdir(parents=True, exist_ok=True)
    _write_if_new(promos_df, promo_dir / "promotions.csv", "promotions")


def _write_if_new(df: pd.DataFrame, path: Path, label: str) -> None:
    if path.exists():
        logger.info("Skipping %s — file already exists at %s", label, path)
        return
    df.to_csv(path, index=False, encoding="utf-8")
    logger.info("Wrote %s (%d rows) → %s", label, len(df), path)


# ---------------------------------------------------------------------------
# Daily output writer
# ---------------------------------------------------------------------------

def write_daily(
    target_date: date,
    dept_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    anomaly_log_df: pd.DataFrame,
    output_dir: Path,
) -> bool:
    """Write daily CSVs to output_dir/daily/{MM}/{DD}/{YYYY}/.

    Returns True if files were written, False if the folder already existed
    (skipped — no overwrite).
    """
    daily_dir = daily_dir_for(output_dir, target_date)
    date_str = target_date.isoformat()

    if daily_dir.exists():
        logger.warning(
            "Daily folder already exists for %s — skipping (no overwrite).", date_str
        )
        return False

    daily_dir.mkdir(parents=True, exist_ok=True)

    # Strip Stage 1 helper columns before output
    dept_out = _strip_helpers(dept_df)
    # Enforce canonical column order
    present = [c for c in _DEPT_OUTPUT_COLS if c in dept_out.columns]
    dept_out = dept_out[present]

    dept_out.to_csv(daily_dir / "department_sales.csv", index=False, encoding="utf-8")
    summary_df.to_csv(daily_dir / "store_summary.csv", index=False, encoding="utf-8")

    # anomaly_log always written; headers-only when no anomalies
    anomaly_cols = ["date_key", "store_id", "department_id", "anomaly_type", "description"]
    if anomaly_log_df.empty:
        empty = pd.DataFrame(columns=anomaly_cols)
        empty.to_csv(daily_dir / "anomaly_log.csv", index=False, encoding="utf-8")
    else:
        anomaly_out = anomaly_log_df.reindex(columns=anomaly_cols)
        anomaly_out.to_csv(daily_dir / "anomaly_log.csv", index=False, encoding="utf-8")

    logger.debug(
        "wrote_daily_output",
        date=date_str,
        dept_rows=len(dept_out),
        summary_rows=len(summary_df),
        anomaly_rows=len(anomaly_log_df),
    )
    return True


# ---------------------------------------------------------------------------
# Manifest updater
# ---------------------------------------------------------------------------

def update_manifest(
    output_dir: Path,
    run_dates: list[date],
    realism_active: bool,
    global_seed: int = GLOBAL_SEED,
    command: str = "run",
    rebuild: bool = False,
) -> None:
    """Read, update, and write manifest.json.

    Row counts (``cumulative_row_counts``, ``anomaly_summary``) are updated
    incrementally: only dates that this invocation adds to
    ``dates_generated`` have their CSVs scanned, and the counts are added
    to whatever the previous manifest reported. Pass ``rebuild=True`` to
    force a full re-scan of every date in ``dates_generated`` — useful
    when the manifest has drifted from on-disk state (e.g., a date folder
    was deleted manually without regeneration).

    Field semantics:
      ``dates_generated``         — cumulative union of every date the
                                    engine has produced output for, across
                                    all invocations. Source of truth for
                                    "what data is on disk".
      ``last_invocation_dates``   — the date list passed to this single
                                    invocation. Scoped to the most recent
                                    `run` or `backfill` call only; not a
                                    rolling history.
      ``last_command``            — which command produced
                                    `last_invocation_dates`
                                    ('init', 'run', 'reports', 'backfill').

    The distinction matters: after a 184-day backfill that ran on top of
    earlier `run` output, `dates_generated` contains everything, but
    `last_invocation_dates` only reflects the 184 backfilled dates.
    """
    manifest_path = output_dir / "manifest.json"

    if manifest_path.exists():
        try:
            with open(manifest_path, encoding="utf-8") as fh:
                manifest = json.load(fh)
        except (json.JSONDecodeError, OSError):
            logger.warning("Could not read existing manifest.json — starting fresh.")
            manifest = _empty_manifest(global_seed)
    else:
        manifest = _empty_manifest(global_seed)

    existing_dates: set[str] = set(manifest.get("dates_generated", []))
    new_date_strs = [d.isoformat() for d in run_dates]
    newly_added_strs = [ds for ds in new_date_strs if ds not in existing_dates]
    for ds in newly_added_strs:
        manifest["dates_generated"].append(ds)
    existing_dates.update(newly_added_strs)

    manifest["last_run"] = datetime.now(timezone.utc).isoformat()
    manifest["last_invocation_dates"] = new_date_strs
    manifest["last_command"] = command
    manifest["realism_engine"] = realism_active
    manifest["generator_version"] = GENERATOR_VERSION
    manifest["global_seed"] = global_seed
    manifest["total_dates_generated"] = len(manifest["dates_generated"])

    if rebuild:
        # Full reconciliation: re-scan every date in dates_generated and
        # start the counters from zero. Use this when the manifest has
        # drifted from on-disk state.
        dates_to_scan: list[str] = list(manifest["dates_generated"])
        dept_total = 0
        summary_total = 0
        disk_total = 0
        disk_by_type: dict[str, int] = {k: 0 for k in _ANOMALY_TYPES}
    else:
        # Incremental: only scan the dates this invocation is adding, and
        # carry the existing counters forward. Bulk backfills go from
        # O(N) file opens per call (where N = all dates ever generated)
        # to O(k) (where k = newly-added dates this call).
        dates_to_scan = newly_added_strs
        prior_counts = manifest.get("cumulative_row_counts", {})
        dept_total = int(prior_counts.get("department_sales", 0))
        summary_total = int(prior_counts.get("store_summary", 0))
        prior_anomaly = manifest.get("anomaly_summary", {})
        disk_total = int(prior_anomaly.get("total_injected", 0))
        prior_by_type = prior_anomaly.get("by_type", {})
        disk_by_type = {k: int(prior_by_type.get(k, 0)) for k in _ANOMALY_TYPES}

    for ds in dates_to_scan:
        d = date.fromisoformat(ds)
        daily = daily_dir_for(output_dir, d)
        dept_path = daily / "department_sales.csv"
        summary_path = daily / "store_summary.csv"
        anomaly_path = daily / "anomaly_log.csv"

        if dept_path.exists():
            try:
                with open(dept_path, encoding="utf-8") as fh:
                    dept_total += sum(1 for _ in fh) - 1
            except OSError:
                pass
        if summary_path.exists():
            try:
                with open(summary_path, encoding="utf-8") as fh:
                    summary_total += sum(1 for _ in fh) - 1
            except OSError:
                pass
        if anomaly_path.exists():
            try:
                adf = pd.read_csv(anomaly_path, encoding="utf-8")
                disk_total += len(adf)
                if not adf.empty and "anomaly_type" in adf.columns:
                    for k in _ANOMALY_TYPES:
                        disk_by_type[k] += int((adf["anomaly_type"] == k).sum())
            except (pd.errors.EmptyDataError, OSError):
                pass

    manifest["cumulative_row_counts"] = {
        "department_sales": dept_total,
        "store_summary": summary_total,
    }
    manifest["anomaly_summary"] = {
        "total_injected": disk_total,
        "by_type": disk_by_type,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    logger.info("Manifest updated → %s", manifest_path)


def _empty_manifest(global_seed: int) -> dict:
    return {
        "last_run": "",
        "last_invocation_dates": [],
        "last_command": "",
        "realism_engine": False,
        "generator_version": GENERATOR_VERSION,
        "global_seed": global_seed,
        "dates_generated": [],
        "total_dates_generated": 0,
        "cumulative_row_counts": {"department_sales": 0, "store_summary": 0},
        "anomaly_summary": {
            "total_injected": 0,
            "by_type": {
                "integrity_breach": 0,
                "missing_department": 0,
                "margin_outlier": 0,
                "duplicate_row": 0,
            },
        },
    }


# ---------------------------------------------------------------------------
# Loader helpers (used by CLI to check init state)
# ---------------------------------------------------------------------------

def dimensions_exist(output_dir: Path) -> bool:
    """Return True if all three dimension files have been written."""
    dim_dir = output_dir / "dimensions"
    return all(
        (dim_dir / f).exists()
        for f in ("dim_stores.csv", "dim_departments.csv", "dim_calendar.csv")
    )


def promotions_exist(output_dir: Path) -> bool:
    """Return True if the promotions file has been written."""
    return (output_dir / "promotions" / "promotions.csv").exists()


def load_promotions(output_dir: Path) -> pd.DataFrame:
    """Load the promotions CSV and parse date columns."""
    path = output_dir / "promotions" / "promotions.csv"
    df = pd.read_csv(path, encoding="utf-8")
    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
    df["end_date"] = pd.to_datetime(df["end_date"]).dt.date
    return df
