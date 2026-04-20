"""
reports.py — Plain-text store report generator.

Produces one file per store for today's date only, formatted like a
manager's daily email to headquarters.  Historical dates do not get reports.

The report displays data as-is — anomalies are visible in the numbers but
are NOT identified as errors.  Anomaly descriptions in the NOTES section
are written as routine operational observations.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from knot_shore.config import DEPARTMENTS, STORES

# Map dept_name to report display name (short form for table)
_DEPT_DISPLAY: dict[str, str] = {
    "Produce":                "Produce",
    "Meat & Seafood":         "Meat & Seafood",
    "Dairy & Eggs":           "Dairy & Eggs",
    "Bakery":                 "Bakery",
    "Deli & Prepared":        "Deli & Prepared",
    "Frozen":                 "Frozen",
    "Grocery (Center Store)": "Grocery",
    "Beverages":              "Beverages",
    "Snacks & Candy":         "Snacks & Candy",
    "Health/Beauty/Household":"HBA/Household",
}

# Routine NOTES variants by day-of-week (no anomaly path)
_ROUTINE_NOTES: dict[int, list[str]] = {
    1: [  # Monday
        "Weekend review complete. Strong Saturday traffic across all departments.",
        "Weekly manager meeting at 9 AM. Produce delivery confirmed for 6:00 AM Tuesday.",
        "Weekend comps reviewed. Labor schedule adjusted for mid-week coverage.",
    ],
    2: [  # Tuesday
        "Produce delivery received and stocked. Shrink within normal range.",
        "Mid-week staffing plan confirmed. Dairy cooler temperature check passed.",
        "Bakery fresh-bake schedule on track. No supply disruptions.",
    ],
    3: [  # Wednesday
        "Hump-day traffic steady. Deli line moving well through lunch rush.",
        "Mid-week reorder submitted. Frozen inventory levels nominal.",
        "Weekly ad drop tomorrow — signage team briefed and ready.",
    ],
    4: [  # Thursday
        "Weekly ad live. Promotional items stocked and signed.",
        "Produce floor reset complete. End-cap displays updated per planogram.",
        "Pre-weekend labor schedule confirmed. Extra register coverage Saturday.",
    ],
    5: [  # Friday
        "Strong Friday traffic. Deli and Prepared hitting peak throughput.",
        "Weekend staffing plan confirmed. Produce delivery on schedule for 6:00 AM tomorrow.",
        "Meat case fully stocked for weekend. Grill display set.",
    ],
    6: [  # Saturday
        "Peak day traffic as expected. All registers staffed through close.",
        "Produce refresh completed at noon. Bakery ran short on artisan bread — reorder placed.",
        "Strong basket sizes. Weekend promo lift tracking above plan.",
    ],
    7: [  # Sunday
        "Sunday volume steady. Families well-represented across Produce and Meat.",
        "Closing crew strong. Store reset for Monday delivery.",
        "Weekend traffic met plan. Inventory positions reviewed for early-week orders.",
    ],
}

# Anomaly-style notes (natural language, no red flags)
_ANOMALY_NOTES: list[str] = [
    "Register system showed a brief reconciliation variance during the evening shift — will review with assistant manager at Monday morning stand-up.",
    "POS terminal in lane 3 required a mid-day restart; transactions processed normally after reset. IT ticket submitted.",
    "Minor discrepancy in end-of-day cash drawer reconciliation — standard recount procedure initiated.",
    "Inventory scan on one pallet came back with an unexpected count; recount in progress.",
    "One department tally appeared off during nightly close — reviewing with department lead tomorrow morning.",
]


def _format_currency(value: float) -> str:
    return f"${value:,.0f}"


def _format_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def generate_store_report(
    store: dict,
    dept_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    promos_df: pd.DataFrame,
    target_date: date,
    anomaly_log_df: pd.DataFrame | None = None,
) -> str:
    """Generate the plain-text report string for one store.

    Parameters
    ----------
    store:
        Store dict from config.STORES.
    dept_df:
        Department sales for this date (all stores).
    summary_df:
        Store summary for this date (all stores).
    promos_df:
        Full promotion schedule.
    target_date:
        Date being reported.
    anomaly_log_df:
        Anomaly log for this date. Pass None or empty DataFrame for no anomaly.
    """
    store_id = store["store_id"]
    store_name = store["store_name"]

    # Extract display name after the em-dash
    display_name = store_name.split("\u2014")[-1].strip() if "\u2014" in store_name else store_name

    # Store-level summary row
    summary_row = summary_df[summary_df["store_id"] == store_id]
    if summary_row.empty:
        return ""
    summary_row = summary_row.iloc[0]

    # Department rows for this store, in canonical order
    store_dept = dept_df[dept_df["store_id"] == store_id].copy()
    dept_order = {d["department_id"]: i for i, d in enumerate(DEPARTMENTS)}
    store_dept["_sort"] = store_dept["department_id"].map(dept_order)
    store_dept = store_dept.sort_values("_sort")

    # Active promotions on this date for this store
    active_promos: list[str] = []
    if not promos_df.empty:
        dept_id_to_name = {d["department_id"]: d["department_name"] for d in DEPARTMENTS}
        active = promos_df[
            (promos_df["start_date"] <= target_date)
            & (promos_df["end_date"] >= target_date)
        ]
        for _, promo_row in active.iterrows():
            d_name = dept_id_to_name.get(int(promo_row["department_id"]), "")
            disc = int(round(float(promo_row["discount_pct"]) * 100))
            end_d = promo_row["end_date"]
            end_str = f"{end_d.month}/{end_d.day}"
            active_promos.append(
                f"- {promo_row['promo_name']}: {d_name} ({disc}% off, ends {end_str})"
            )

    # Determine anomaly note
    has_anomaly = False
    if anomaly_log_df is not None and not anomaly_log_df.empty:
        store_anomalies = anomaly_log_df[anomaly_log_df["store_id"] == store_id]
        has_anomaly = not store_anomalies.empty

    if has_anomaly:
        # Use a seeded but simple index to pick consistent note
        note_idx = store_id % len(_ANOMALY_NOTES)
        notes_text = _ANOMALY_NOTES[note_idx]
    else:
        dow = target_date.isoweekday()  # 1=Mon … 7=Sun
        note_choices = _ROUTINE_NOTES[dow]
        note_idx = store_id % len(note_choices)
        notes_text = note_choices[note_idx]

    # --- Build report ---
    day_str = target_date.strftime("%A, %B %d, %Y")
    lines: list[str] = [
        "KNOT SHORE GROCERY \u2014 DAILY STORE REPORT",
        "=" * 40,
        f"Store: {display_name} (#{store_id:03d})",
        f"Date:  {day_str}",
        "Prepared by: Store Manager",
        "",
        "DEPARTMENT PERFORMANCE",
        "\u2500" * 57,
        f"{'Department':<26} {'Net Sales':>10} {'Margin%':>8} {'Transactions':>13}",
    ]

    net_total_check = 0.0
    txn_total_check = 0

    for _, dept_row in store_dept.iterrows():
        dept_id = int(dept_row["department_id"])
        dept_name = next(
            (d["department_name"] for d in DEPARTMENTS if d["department_id"] == dept_id),
            f"Dept {dept_id}",
        )
        disp_name = _DEPT_DISPLAY.get(dept_name, dept_name)
        net = float(dept_row["net_sales"])
        margin = float(dept_row["gross_margin_pct"])
        txns = int(dept_row["transactions"])

        net_total_check += net
        txn_total_check += txns

        lines.append(
            f"{disp_name:<26} {_format_currency(net):>10} {_format_pct(margin):>8} {txns:>13,}"
        )

    lines.append("\u2500" * 57)

    # Store total line — use the summary values (which may differ if anomaly injected)
    net_total = float(summary_row["net_sales_total"])
    labor = float(summary_row["labor_cost"])
    labor_pct = float(summary_row["labor_cost_pct"])
    txn_total = int(summary_row["transactions_total"])

    # Overall margin: derive from the department rows actually displayed
    gross_total_display = float(store_dept["gross_sales"].sum())
    net_total_display = float(store_dept["net_sales"].sum())
    cogs_total_display = float(store_dept["cogs"].sum())
    gross_margin_display = net_total_display - cogs_total_display
    overall_margin_pct = (
        gross_margin_display / net_total_display if net_total_display != 0 else 0.0
    )

    lines.append(
        f"{'STORE TOTAL':<26} {_format_currency(net_total_display):>10} "
        f"{_format_pct(overall_margin_pct):>8} {txn_total_check:>13,}"
    )
    lines.append(
        f"Labor Cost: {_format_currency(labor)} ({_format_pct(labor_pct)} of net sales)"
    )

    # Active promotions section
    lines.append("")
    lines.append("ACTIVE PROMOTIONS")
    if active_promos:
        lines.extend(active_promos)
    else:
        lines.append("- No active promotions today.")

    # Notes section
    lines.append("")
    lines.append("NOTES")
    lines.append(f"- {notes_text}")

    return "\n".join(lines) + "\n"


def generate_all_reports(
    target_date: date,
    dept_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    promos_df: pd.DataFrame,
    output_dir: Path,
    anomaly_log_df: pd.DataFrame | None = None,
) -> None:
    """Generate and write store_NNN_report.txt for all 8 stores.

    Files are written to output_dir/reports/YYYY-MM-DD/.
    """
    reports_dir = output_dir / "reports" / target_date.isoformat()
    reports_dir.mkdir(parents=True, exist_ok=True)

    for store in STORES:
        store_id = store["store_id"]
        content = generate_store_report(
            store=store,
            dept_df=dept_df,
            summary_df=summary_df,
            promos_df=promos_df,
            target_date=target_date,
            anomaly_log_df=anomaly_log_df,
        )
        if not content:
            continue
        report_path = reports_dir / f"store_{store_id:03d}_report.txt"
        report_path.write_text(content, encoding="utf-8")
