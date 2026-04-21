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
    Resolve the eight target dates for the given anchor: the trailing
    seven-day window (anchor through anchor-6) plus the same calendar
    date one year prior.  Anchor defaults to today; override with --date.
    For each date: skip if exists → Stage 1 → Stage 2 (optional) →
    anomaly injection → Stage 3.
    Generate store reports for the anchor date only.
    Update manifest.json.

  reports
    (Re-)generate store report files for a specific date.
    Requires daily CSV data to already exist for that date.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared pipeline runner
# ---------------------------------------------------------------------------

def _run_pipeline(
    target_dates: list[date],
    promos_df: pd.DataFrame,
    seed: int,
    output_dir: Path,
    no_realism: bool,
    generate_reports_for: date | None,
) -> tuple[list[date], list[dict]]:
    """Run Stage 1 → 2 → anomaly → Stage 3 for a list of target dates.

    Parameters
    ----------
    target_dates:
        Dates to process (in any order; each is checked for existence).
    promos_df:
        Full promotion schedule.
    seed:
        Global random seed.
    output_dir:
        Root output directory.
    no_realism:
        When True, skip Stage 2 even if DB is configured.
    generate_reports_for:
        If not None, generate store reports for this specific date after writing.
        Pass None to skip report generation.

    Returns
    -------
    (generated_dates, anomaly_summaries)
    """
    from knot_shore import anomalies, realism  # noqa: PLC0415
    from knot_shore.anomalies import anomaly_summary  # noqa: PLC0415
    from knot_shore.config import DEPARTMENTS, STORES  # noqa: PLC0415
    from knot_shore.output import daily_dir_for, write_daily  # noqa: PLC0415
    from knot_shore.reports import generate_all_reports  # noqa: PLC0415
    from knot_shore.sales_generator import generate_day  # noqa: PLC0415

    generated: list[date] = []
    anomaly_summaries: list[dict] = []

    for target_date in target_dates:
        date_dir = daily_dir_for(output_dir, target_date)
        if date_dir.exists():
            logger.debug("Folder exists for %s — skipping.", target_date.isoformat())
            continue

        # Stage 1
        dept_df, summary_df = generate_day(
            target_date=target_date,
            stores=STORES,
            departments=DEPARTMENTS,
            promos_df=promos_df,
            global_seed=seed,
        )

        # Stage 2 (optional)
        if not no_realism and realism.is_available(force_disable=no_realism):
            dept_df, summary_df = realism.adjust(
                dept_df=dept_df,
                summary_df=summary_df,
                target_date=target_date,
                force_disable=False,
                global_seed=seed,
            )

        # Anomaly injection
        dept_df, summary_df, anomaly_log_df = anomalies.inject(
            dept_df=dept_df,
            summary_df=summary_df,
            target_date=target_date,
            global_seed=seed,
        )
        anomaly_summaries.append(anomaly_summary(anomaly_log_df))

        # Stage 3: write CSVs
        written = write_daily(
            target_date=target_date,
            dept_df=dept_df,
            summary_df=summary_df,
            anomaly_log_df=anomaly_log_df,
            output_dir=output_dir,
        )

        if written:
            generated.append(target_date)

        # Store reports — only for the explicitly requested date
        if generate_reports_for is not None and target_date == generate_reports_for and written:
            generate_all_reports(
                target_date=target_date,
                dept_df=dept_df,
                summary_df=summary_df,
                promos_df=promos_df,
                output_dir=output_dir,
                anomaly_log_df=anomaly_log_df,
            )
            logger.info("Store reports written for %s.", target_date.isoformat())

    return generated, anomaly_summaries


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
    from knot_shore import realism  # noqa: PLC0415
    from knot_shore.date_resolver import resolve_required_dates  # noqa: PLC0415
    from knot_shore.output import (  # noqa: PLC0415
        dimensions_exist,
        load_promotions,
        promotions_exist,
        update_manifest,
    )

    _require_init(output_dir, dimensions_exist, promotions_exist)

    promos_df = load_promotions(output_dir)
    target_dates = resolve_required_dates(anchor)

    use_realism = _check_realism(no_realism, realism)

    generated, anomaly_summaries = _run_pipeline(
        target_dates=target_dates,
        promos_df=promos_df,
        seed=seed,
        output_dir=output_dir,
        no_realism=no_realism,
        generate_reports_for=anchor,
    )

    update_manifest(
        output_dir=output_dir,
        run_dates=target_dates,
        realism_active=use_realism,
        global_seed=seed,
        anomaly_summaries=anomaly_summaries,
        command="run",
    )

    logger.info(
        "run complete. %d dates newly generated (of %d attempted).",
        len(generated),
        len(target_dates),
    )


# ---------------------------------------------------------------------------
# Command: reports
# ---------------------------------------------------------------------------

def cmd_reports(anchor: date, output_dir: Path) -> None:
    from knot_shore.output import daily_dir_for, load_promotions  # noqa: PLC0415
    from knot_shore.reports import generate_all_reports  # noqa: PLC0415

    daily_dir = daily_dir_for(output_dir, anchor)
    date_str = anchor.isoformat()

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

    promos_df = (
        load_promotions(output_dir)
        if _promotions_exist(output_dir)
        else pd.DataFrame()
    )

    generate_all_reports(
        target_date=anchor,
        dept_df=dept_df,
        summary_df=summary_df,
        promos_df=promos_df,
        output_dir=output_dir,
        anomaly_log_df=anomaly_log_df,
    )
    logger.info("Store reports written for %s.", date_str)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _require_init(output_dir, dimensions_exist, promotions_exist) -> None:
    """Exit with an error if init has not been run."""
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


def _check_realism(no_realism: bool, realism_module) -> bool:
    """Log realism engine status and return whether it is active."""
    use_realism = (not no_realism) and realism_module.is_available(force_disable=no_realism)
    if use_realism:
        logger.info("Stage 2 (Realism Engine) active.")
    else:
        logger.info("Stage 2 (Realism Engine) inactive — outputting base data.")
    return use_realism


def _promotions_exist(output_dir: Path) -> bool:
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

    # ---- init ----
    init_p = sub.add_parser("init", help="Generate dimensions and promotion schedule.")
    init_p.add_argument("--seed", type=int, default=42, help="Global random seed (default 42).")
    init_p.add_argument("--output", type=Path, default=Path("./output"), help="Output directory.")

    # ---- run ----
    run_p = sub.add_parser(
        "run",
        help=(
            "Generate daily data for the anchor date, the six preceding days, "
            "and the same calendar date one year prior (eight dates total)."
        ),
    )
    run_p.add_argument("--seed", type=int, default=42)
    run_p.add_argument("--output", type=Path, default=Path("./output"))
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

    # ---- reports ----
    rep_p = sub.add_parser("reports", help="(Re-)generate store reports for a date.")
    rep_p.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        required=True,
        help="Date to generate reports for (YYYY-MM-DD).",
    )
    rep_p.add_argument("--output", type=Path, default=Path("./output"))

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
