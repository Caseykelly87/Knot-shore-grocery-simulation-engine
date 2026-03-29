"""
cli.py — Entry point: orchestrates Stage 1 → Stage 2 → Anomaly Injection → Stage 3.

Usage
-----
  python -m knot_shore init    --seed 42 --output ./output
  python -m knot_shore run     --seed 42 --output ./output [--date YYYY-MM-DD] [--no-realism]
  python -m knot_shore reports --date YYYY-MM-DD --output ./output

Commands
--------
  init
    Generate dimension tables and the full 4-year promotion schedule.
    Safe to re-run — skips files that already exist.

  run
    Compute the 4 target dates (today + same date for 3 prior years).
    For each date: check skip → Stage 1 → Stage 2 (optional) → anomaly injection → Stage 3.
    Generate store reports for the current-year date only.
    Update manifest.json.

  reports
    (Re-)generate store report files for a specific date.
    Requires daily CSV data to already exist for that date.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Target date helpers
# ---------------------------------------------------------------------------

def _target_dates(anchor: date) -> list[date]:
    """Return the 4 target dates: anchor and same calendar date for 3 prior years."""
    dates = []
    for years_back in range(4):
        yr = anchor.year - years_back
        # Use Feb 28 when anchor is Feb 29 and the prior year is not a leap year
        try:
            d = anchor.replace(year=yr)
        except ValueError:
            d = date(yr, 2, 28)
        dates.append(d)
    # Process oldest → newest so log output reads chronologically
    dates.reverse()
    return dates


# ---------------------------------------------------------------------------
# Command: init
# ---------------------------------------------------------------------------

def cmd_init(seed: int, output_dir: Path) -> None:
    from knot_shore.dimensions import (  # noqa: PLC0415
        generate_dim_calendar,
        generate_dim_departments,
        generate_dim_stores,
    )
    from knot_shore.output import (  # noqa: PLC0415
        dimensions_exist,
        promotions_exist,
        write_dimensions,
        write_promotions,
    )
    from knot_shore.promotions import generate_promotions  # noqa: PLC0415

    if dimensions_exist(output_dir):
        logger.info("Dimension files already exist — skipping dimension generation.")
    else:
        logger.info("Generating dimension tables …")
        stores_df = generate_dim_stores()
        depts_df = generate_dim_departments()
        calendar_df = generate_dim_calendar()
        write_dimensions(stores_df, depts_df, calendar_df, output_dir)

    if promotions_exist(output_dir):
        logger.info("Promotions file already exists — skipping promotion generation.")
    else:
        logger.info("Generating 4-year promotion schedule (seed=%d) …", seed)
        promos_df = generate_promotions(seed=seed)
        write_promotions(promos_df, output_dir)
        logger.info("Promotion schedule: %d promos generated.", len(promos_df))

    logger.info("init complete.")


# ---------------------------------------------------------------------------
# Command: run
# ---------------------------------------------------------------------------

def cmd_run(
    seed: int,
    output_dir: Path,
    anchor: date,
    no_realism: bool,
) -> None:
    from knot_shore import anomalies, realism  # noqa: PLC0415
    from knot_shore.anomalies import anomaly_summary  # noqa: PLC0415
    from knot_shore.config import DEPARTMENTS, STORES  # noqa: PLC0415
    from knot_shore.output import (  # noqa: PLC0415
        dimensions_exist,
        load_promotions,
        promotions_exist,
        update_manifest,
        write_daily,
    )
    from knot_shore.reports import generate_all_reports  # noqa: PLC0415
    from knot_shore.sales_generator import generate_day  # noqa: PLC0415

    # Guard: init must run first
    if not dimensions_exist(output_dir):
        logger.error(
            "Dimension files not found in %s. Run 'python -m knot_shore init' first.",
            output_dir,
        )
        sys.exit(1)

    if not promotions_exist(output_dir):
        logger.error(
            "Promotions file not found in %s. Run 'python -m knot_shore init' first.",
            output_dir,
        )
        sys.exit(1)

    promos_df = load_promotions(output_dir)
    target_dates = _target_dates(anchor)
    today_date = anchor  # reports generated for this date only

    # Determine whether Stage 2 is active
    use_realism = (not no_realism) and realism.is_available(force_disable=no_realism)
    if use_realism:
        logger.info("Stage 2 (Realism Engine) active.")
    else:
        logger.info("Stage 2 (Realism Engine) inactive — outputting base data.")

    all_anomaly_summaries: list[dict] = []
    generated_dates: list[date] = []

    for target_date in target_dates:
        logger.info("Processing %s …", target_date.isoformat())

        # Check if date folder already exists
        date_dir = output_dir / "daily" / target_date.isoformat()
        if date_dir.exists():
            logger.warning(
                "Folder already exists for %s — skipping.", target_date.isoformat()
            )
            continue

        # Stage 1: Base generation
        dept_df, summary_df = generate_day(
            target_date=target_date,
            stores=STORES,
            departments=DEPARTMENTS,
            promos_df=promos_df,
            global_seed=seed,
        )

        # Stage 2: Realism engine (optional)
        if use_realism:
            dept_df, summary_df = realism.adjust(
                dept_df=dept_df,
                summary_df=summary_df,
                target_date=target_date,
                force_disable=False,
            )

        # Anomaly injection
        dept_df, summary_df, anomaly_log_df = anomalies.inject(
            dept_df=dept_df,
            summary_df=summary_df,
            target_date=target_date,
            global_seed=seed,
        )
        all_anomaly_summaries.append(anomaly_summary(anomaly_log_df))

        # Stage 3: Output
        written = write_daily(
            target_date=target_date,
            dept_df=dept_df,
            summary_df=summary_df,
            anomaly_log_df=anomaly_log_df,
            output_dir=output_dir,
        )

        if written:
            generated_dates.append(target_date)

        # Store reports for the current-year (anchor) date only
        if target_date == today_date and written:
            generate_all_reports(
                target_date=target_date,
                dept_df=dept_df,
                summary_df=summary_df,
                promos_df=promos_df,
                output_dir=output_dir,
                anomaly_log_df=anomaly_log_df,
            )
            logger.info("Store reports written for %s.", target_date.isoformat())

    # Update manifest for all run dates (including skipped ones so cumulative counts update)
    update_manifest(
        output_dir=output_dir,
        run_dates=target_dates,
        realism_active=use_realism,
        global_seed=seed,
        anomaly_summaries=all_anomaly_summaries,
    )

    logger.info(
        "run complete. %d dates newly generated (of %d attempted).",
        len(generated_dates),
        len(target_dates),
    )


# ---------------------------------------------------------------------------
# Command: reports
# ---------------------------------------------------------------------------

def cmd_reports(anchor: date, output_dir: Path) -> None:
    from knot_shore.output import load_promotions  # noqa: PLC0415
    from knot_shore.reports import generate_all_reports  # noqa: PLC0415

    date_str = anchor.isoformat()
    daily_dir = output_dir / "daily" / date_str

    if not daily_dir.exists():
        logger.error(
            "No daily data found for %s at %s. Run 'python -m knot_shore run' first.",
            date_str,
            daily_dir,
        )
        sys.exit(1)

    dept_df = pd.read_csv(daily_dir / "department_sales.csv", encoding="utf-8")
    summary_df = pd.read_csv(daily_dir / "store_summary.csv", encoding="utf-8")

    anomaly_path = daily_dir / "anomaly_log.csv"
    anomaly_log_df = (
        pd.read_csv(anomaly_path, encoding="utf-8")
        if anomaly_path.exists()
        else pd.DataFrame()
    )

    promos_df = load_promotions(output_dir) if promotions_exist(output_dir) else pd.DataFrame()

    generate_all_reports(
        target_date=anchor,
        dept_df=dept_df,
        summary_df=summary_df,
        promos_df=promos_df,
        output_dir=output_dir,
        anomaly_log_df=anomaly_log_df,
    )
    logger.info("Store reports written for %s.", date_str)


def promotions_exist(output_dir: Path) -> bool:
    return (output_dir / "promotions" / "promotions.csv").exists()


# ---------------------------------------------------------------------------
# Argument parser and main
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m knot_shore",
        description="Knot Shore Grocery — daily operational data generator.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # init
    init_p = sub.add_parser("init", help="Generate dimensions and promotion schedule.")
    init_p.add_argument("--seed", type=int, default=42, help="Global random seed (default 42).")
    init_p.add_argument("--output", type=Path, default=Path("./output"), help="Output directory.")

    # run
    run_p = sub.add_parser("run", help="Generate daily data for today + 3 prior-year dates.")
    run_p.add_argument("--seed", type=int, default=42, help="Global random seed (default 42).")
    run_p.add_argument("--output", type=Path, default=Path("./output"), help="Output directory.")
    run_p.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        default=None,
        help="Override anchor date (YYYY-MM-DD). Defaults to today.",
    )
    run_p.add_argument(
        "--no-realism",
        action="store_true",
        default=False,
        help="Disable Stage 2 realism engine even if KNOT_SHORE_DB_URL is set.",
    )

    # reports
    rep_p = sub.add_parser("reports", help="(Re-)generate store reports for a date.")
    rep_p.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        required=True,
        help="Date to generate reports for (YYYY-MM-DD).",
    )
    rep_p.add_argument("--output", type=Path, default=Path("./output"), help="Output directory.")

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "init":
        cmd_init(seed=args.seed, output_dir=args.output)

    elif args.command == "run":
        anchor = args.date or date.today()
        cmd_run(
            seed=args.seed,
            output_dir=args.output,
            anchor=anchor,
            no_realism=args.no_realism,
        )

    elif args.command == "reports":
        cmd_reports(anchor=args.date, output_dir=args.output)


if __name__ == "__main__":
    main()
