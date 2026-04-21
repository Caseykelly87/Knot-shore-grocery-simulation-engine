"""
anomalies.py — Anomaly injection (post-realism, pre-output).

On each generated date, per store there is a 5% probability that one
anomaly is injected.  Only one anomaly type per store per date.

Anomaly types (§6):
  Type 1 — Integrity Breach   (40%): net_sales ≠ gross_sales − discount_amount
  Type 2 — Missing Department (30%): one department row removed entirely
  Type 3 — Margin Outlier     (20%): one dept has unrealistic gross_margin_pct
  Type 4 — Duplicate Row      (10%): one department row duplicated exactly

The anomaly_log is always written (headers-only when no anomalies injected).

Seeding: date_seed = global_seed + target_date.toordinal()  (same as Stage 1).
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from knot_shore.config import (
    ANOMALY_DUPLICATE_ROW_WEIGHT,
    ANOMALY_INTEGRITY_BREACH_WEIGHT,
    ANOMALY_INTEGRITY_OFFSET_MAX,
    ANOMALY_INTEGRITY_OFFSET_MIN,
    ANOMALY_MARGIN_OUTLIER_WEIGHT,
    ANOMALY_MISSING_DEPT_WEIGHT,
    ANOMALY_PROBABILITY,
    GLOBAL_SEED,
    STORES,
)

# Anomaly type labels used in anomaly_log.csv
TYPE_INTEGRITY = "integrity_breach"
TYPE_MISSING = "missing_department"
TYPE_MARGIN = "margin_outlier"
TYPE_DUPLICATE = "duplicate_row"

_TYPE_NAMES = [TYPE_INTEGRITY, TYPE_MISSING, TYPE_MARGIN, TYPE_DUPLICATE]
_TYPE_WEIGHTS = [
    ANOMALY_INTEGRITY_BREACH_WEIGHT,
    ANOMALY_MISSING_DEPT_WEIGHT,
    ANOMALY_MARGIN_OUTLIER_WEIGHT,
    ANOMALY_DUPLICATE_ROW_WEIGHT,
]


def inject(
    dept_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    target_date: date,
    global_seed: int = GLOBAL_SEED,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Inject anomalies into dept_df / summary_df and return anomaly_log_df.

    Parameters
    ----------
    dept_df:
        Department sales DataFrame (may still contain helper columns; anomaly
        injection operates only on the data columns).
    summary_df:
        Store summary DataFrame.
    target_date:
        The date being processed.
    global_seed:
        Master seed; date_seed = global_seed + target_date.toordinal().

    Returns
    -------
    (dept_df, summary_df, anomaly_log_df)
        dept_df and summary_df may be modified in place (copies are made).
        anomaly_log_df always has the correct columns, with 0 rows if none injected.
    """
    date_seed = global_seed + target_date.toordinal()
    rng = np.random.default_rng(date_seed + 1_000_000)  # offset avoids collision with Stage 1

    dept_df = dept_df.copy()
    summary_df = summary_df.copy()

    log_rows: list[dict] = []

    store_ids = [s["store_id"] for s in STORES]

    for store_id in store_ids:
        if rng.random() >= ANOMALY_PROBABILITY:
            continue

        # Choose anomaly type
        type_idx = int(rng.choice(len(_TYPE_NAMES), p=_TYPE_WEIGHTS))
        anomaly_type = _TYPE_NAMES[type_idx]

        store_mask = dept_df["store_id"] == store_id
        store_rows = dept_df[store_mask]

        if store_rows.empty:
            continue

        dept_ids = sorted(store_rows["department_id"].unique().tolist())
        chosen_dept_id = int(rng.choice(dept_ids))

        if anomaly_type == TYPE_INTEGRITY:
            dept_df, description = _inject_integrity_breach(
                dept_df, store_id, chosen_dept_id, rng
            )
            log_rows.append(
                {
                    "date_key": target_date,
                    "store_id": store_id,
                    "department_id": chosen_dept_id,
                    "anomaly_type": TYPE_INTEGRITY,
                    "description": description,
                }
            )

        elif anomaly_type == TYPE_MISSING:
            dept_df, summary_df, description = _inject_missing_department(
                dept_df, summary_df, store_id, chosen_dept_id
            )
            log_rows.append(
                {
                    "date_key": target_date,
                    "store_id": store_id,
                    "department_id": chosen_dept_id,
                    "anomaly_type": TYPE_MISSING,
                    "description": description,
                }
            )

        elif anomaly_type == TYPE_MARGIN:
            dept_df, description = _inject_margin_outlier(
                dept_df, store_id, chosen_dept_id, rng
            )
            log_rows.append(
                {
                    "date_key": target_date,
                    "store_id": store_id,
                    "department_id": chosen_dept_id,
                    "anomaly_type": TYPE_MARGIN,
                    "description": description,
                }
            )

        elif anomaly_type == TYPE_DUPLICATE:
            dept_df, description = _inject_duplicate_row(
                dept_df, store_id, chosen_dept_id
            )
            log_rows.append(
                {
                    "date_key": target_date,
                    "store_id": store_id,
                    "department_id": chosen_dept_id,
                    "anomaly_type": TYPE_DUPLICATE,
                    "description": description,
                }
            )

    anomaly_log_df = pd.DataFrame(
        log_rows if log_rows else [],
        columns=["date_key", "store_id", "department_id", "anomaly_type", "description"],
    )

    return dept_df, summary_df, anomaly_log_df


# ---------------------------------------------------------------------------
# Individual anomaly injectors
# ---------------------------------------------------------------------------

def _inject_integrity_breach(
    dept_df: pd.DataFrame,
    store_id: int,
    dept_id: int,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, str]:
    """Add a random offset to net_sales, breaking net = gross − discount."""
    mask = (dept_df["store_id"] == store_id) & (dept_df["department_id"] == dept_id)
    offset = round(
        float(rng.uniform(ANOMALY_INTEGRITY_OFFSET_MIN, ANOMALY_INTEGRITY_OFFSET_MAX))
        * rng.choice([-1.0, 1.0]),
        2,
    )
    dept_df.loc[mask, "net_sales"] = (dept_df.loc[mask, "net_sales"] + offset).round(2)
    description = (
        f"net_sales adjusted by {offset:+.2f} — "
        f"breaks net_sales = gross_sales − discount_amount for dept {dept_id}"
    )
    return dept_df, description


def _inject_missing_department(
    dept_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    store_id: int,
    dept_id: int,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Remove one department row from a store and update store_summary totals."""
    remove_mask = (dept_df["store_id"] == store_id) & (dept_df["department_id"] == dept_id)
    dept_df = dept_df[~remove_mask].reset_index(drop=True)

    # Recalculate store_summary totals from remaining dept rows
    store_mask_sum = summary_df["store_id"] == store_id
    remaining = dept_df[dept_df["store_id"] == store_id]

    summary_df.loc[store_mask_sum, "gross_sales_total"] = round(
        float(remaining["gross_sales"].sum()), 2
    )
    summary_df.loc[store_mask_sum, "net_sales_total"] = round(
        float(remaining["net_sales"].sum()), 2
    )
    summary_df.loc[store_mask_sum, "transactions_total"] = int(
        remaining["transactions"].sum()
    )

    description = f"Department {dept_id} row omitted from store {store_id} output"
    return dept_df, summary_df, description


def _inject_margin_outlier(
    dept_df: pd.DataFrame,
    store_id: int,
    dept_id: int,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, str]:
    """Set cogs to an extreme value producing an unrealistic gross_margin_pct."""
    mask = (dept_df["store_id"] == store_id) & (dept_df["department_id"] == dept_id)

    # 50% chance negative margin, 50% chance >85% margin
    if rng.random() < 0.5:
        # Negative margin: cogs > net_sales
        multiplier = float(rng.uniform(1.05, 1.30))
        extreme_cogs = (dept_df.loc[mask, "net_sales"] * multiplier).round(2)
        description = (
            f"Negative margin injected for store {store_id} dept {dept_id} "
            f"(cogs = {multiplier:.2f}× net_sales)"
        )
    else:
        # Very high margin: cogs ≈ 0
        extreme_cogs = (dept_df.loc[mask, "net_sales"] * 0.05).round(2)
        description = (
            f"Margin outlier (>85%) injected for store {store_id} dept {dept_id} "
            f"(cogs = 5% of net_sales)"
        )

    dept_df.loc[mask, "cogs"] = extreme_cogs
    dept_df.loc[mask, "gross_margin"] = (
        dept_df.loc[mask, "net_sales"] - dept_df.loc[mask, "cogs"]
    ).round(2)
    net = dept_df.loc[mask, "net_sales"]
    dept_df.loc[mask, "gross_margin_pct"] = np.where(
        net != 0,
        (dept_df.loc[mask, "gross_margin"] / net).round(4),
        0.0,
    )

    return dept_df, description


def _inject_duplicate_row(
    dept_df: pd.DataFrame,
    store_id: int,
    dept_id: int,
) -> tuple[pd.DataFrame, str]:
    """Duplicate one department row exactly."""
    mask = (dept_df["store_id"] == store_id) & (dept_df["department_id"] == dept_id)
    dup_rows = dept_df[mask]
    dept_df = pd.concat([dept_df, dup_rows], ignore_index=True)
    description = (
        f"Duplicate row injected for store {store_id} dept {dept_id} "
        f"— same date/store/dept/values appear twice"
    )
    return dept_df, description


def anomaly_summary(anomaly_log_df: pd.DataFrame) -> dict:
    """Return a summary dict for manifest.json from an anomaly log DataFrame."""
    if anomaly_log_df.empty:
        return {
            "total_injected": 0,
            "by_type": {
                "integrity_breach": 0,
                "missing_department": 0,
                "margin_outlier": 0,
                "duplicate_row": 0,
            },
        }

    by_type = anomaly_log_df["anomaly_type"].value_counts().to_dict()
    return {
        "total_injected": len(anomaly_log_df),
        "by_type": {
            "integrity_breach": int(by_type.get(TYPE_INTEGRITY, 0)),
            "missing_department": int(by_type.get(TYPE_MISSING, 0)),
            "margin_outlier": int(by_type.get(TYPE_MARGIN, 0)),
            "duplicate_row": int(by_type.get(TYPE_DUPLICATE, 0)),
        },
    }
