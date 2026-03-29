"""
realism.py — Stage 2: Realism Engine.

Reads real economic series from the ETL pipeline's Postgres database and
applies multipliers to the base-generated DataFrames.  If the database is
unavailable, Stage 2 is skipped and base data is returned unchanged.

Connection: set KNOT_SHORE_DB_URL environment variable.
  e.g. postgresql://user:pass@host:5432/dbname

Series used (§5.3):
  ERS_FOOD_HOME — food-at-home CPI   → sales volume multiplier
  SENTIMENT     — consumer sentiment → sales volume multiplier
  UNRATE        — unemployment rate  → sales volume multiplier
  ERS_*         — category-level CPI → margin pressure per department
  AVG_WAGES     — average wages      → labor cost multiplier

Application order (§5.5):
  1. Query all series for target dates (cached per run)
  2. Apply sales_volume_multiplier → gross_sales
  3. Recalculate derivation chain (§4.10 steps 2-10)
  4. Apply margin_pressure per department (additive adjustment to base_margin_pct)
  5. Recalculate cogs, gross_margin, gross_margin_pct (steps 4-6)
  6. Re-aggregate store_summary from adjusted dept_sales
  7. Apply labor_cost_multiplier → labor_cost
  8. Recalculate labor_cost_pct
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from knot_shore.config import (
    DEPARTMENTS,
    ERS_DEPT_MAP,
    REALISM_CPI_FOOD_COEFF,
    REALISM_LABOR_CLAMP,
    REALISM_MARGIN_COEFF,
    REALISM_MARGIN_MAX,
    REALISM_MARGIN_MIN,
    REALISM_SALES_CLAMP,
    REALISM_SENTIMENT_COEFF,
    REALISM_UNEMP_COEFF,
    REALISM_WAGES_COEFF,
    SERIES_AVG_WAGES,
    SERIES_ERS_ALL_FOOD,
    SERIES_ERS_FOOD_HOME,
    SERIES_SENTIMENT,
    SERIES_UNRATE,
    STORES,
)
from knot_shore.factors import labor_pct_adjusted

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database connection (lazy, optional)
# ---------------------------------------------------------------------------

_DB_ENGINE: Any = None       # sqlalchemy Engine or None
_DB_AVAILABLE: bool | None = None  # None means not yet checked

# In-memory caches cleared by clear_cache()
_series_cache: dict[str, pd.DataFrame] = {}
_baseline_cache: dict[str, float] = {}


def _get_engine() -> Any | None:
    """Return a SQLAlchemy engine if KNOT_SHORE_DB_URL is set and reachable."""
    global _DB_ENGINE, _DB_AVAILABLE

    if _DB_AVAILABLE is not None:
        return _DB_ENGINE if _DB_AVAILABLE else None

    db_url = os.environ.get("KNOT_SHORE_DB_URL", "").strip()
    if not db_url:
        logger.info("KNOT_SHORE_DB_URL not set — Stage 2 (Realism Engine) skipped.")
        _DB_AVAILABLE = False
        return None

    try:
        from sqlalchemy import create_engine, text  # noqa: PLC0415

        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        _DB_ENGINE = engine
        _DB_AVAILABLE = True
        logger.info("Realism Engine connected to database.")
        return _DB_ENGINE
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Realism Engine: database connection failed (%s) — Stage 2 skipped.", exc
        )
        _DB_AVAILABLE = False
        return None


# ---------------------------------------------------------------------------
# Series lookup helpers
# ---------------------------------------------------------------------------

def _load_series(engine: Any, series_key: str) -> pd.DataFrame:
    """Load a monthly series from the DB as a DataFrame with columns [date, value]."""
    if series_key in _series_cache:
        return _series_cache[series_key]

    try:
        from sqlalchemy import text  # noqa: PLC0415

        query = """
            SELECT series_date AS date, value
            FROM economic_series
            WHERE series_key = :key
            ORDER BY series_date ASC
        """
        with engine.connect() as conn:
            result = conn.execute(text(query), {"key": series_key})
            rows = result.fetchall()

        if not rows:
            df = pd.DataFrame(columns=["date", "value"])
        else:
            df = pd.DataFrame(rows, columns=["date", "value"])
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df = df.sort_values("date").reset_index(drop=True)

    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not load series %s: %s", series_key, exc)
        df = pd.DataFrame(columns=["date", "value"])

    _series_cache[series_key] = df
    return df


def _get_baseline(engine: Any, series_key: str) -> float | None:
    """Return the first available value for a series (baseline anchor)."""
    if series_key in _baseline_cache:
        return _baseline_cache[series_key]

    df = _load_series(engine, series_key)
    if df.empty:
        return None
    val = float(df.iloc[0]["value"])
    _baseline_cache[series_key] = val
    return val


def _lookup(engine: Any, series_key: str, target_date: date) -> float | None:
    """Forward-fill monthly series to target_date (most recent value on or before)."""
    df = _load_series(engine, series_key)
    if df.empty:
        return None
    past = df[df["date"] <= target_date]
    if past.empty:
        return None
    return float(past.iloc[-1]["value"])


# ---------------------------------------------------------------------------
# Multiplier functions (§5.3)
# ---------------------------------------------------------------------------

def _sales_volume_multiplier(engine: Any, target_date: date) -> float:
    """Combine CPI-food, sentiment, and unemployment into a single sales multiplier."""
    multiplier = 1.0

    cpi_food = _lookup(engine, SERIES_ERS_FOOD_HOME, target_date)
    cpi_baseline = _get_baseline(engine, SERIES_ERS_FOOD_HOME)
    if cpi_food is not None and cpi_baseline:
        multiplier *= 1.0 + REALISM_CPI_FOOD_COEFF * (cpi_food / cpi_baseline - 1.0)

    sentiment = _lookup(engine, SERIES_SENTIMENT, target_date)
    sentiment_baseline = _get_baseline(engine, SERIES_SENTIMENT)
    if sentiment is not None and sentiment_baseline:
        multiplier *= 1.0 + REALISM_SENTIMENT_COEFF * (sentiment / sentiment_baseline - 1.0)

    unemp = _lookup(engine, SERIES_UNRATE, target_date)
    unemp_baseline = _get_baseline(engine, SERIES_UNRATE)
    if unemp is not None and unemp_baseline:
        multiplier *= 1.0 + REALISM_UNEMP_COEFF * (unemp / unemp_baseline - 1.0)

    return float(np.clip(multiplier, *REALISM_SALES_CLAMP))


def _margin_adjustment(engine: Any, target_date: date, department_name: str) -> float:
    """Return additive margin adjustment for a department on a date.

    A negative return compresses margin; positive widens it.
    Caller is responsible for clamping the final adjusted margin.
    """
    series_key = ERS_DEPT_MAP.get(department_name, SERIES_ERS_ALL_FOOD)
    ers_value = _lookup(engine, series_key, target_date)
    ers_baseline = _get_baseline(engine, series_key)

    if ers_value is None or not ers_baseline:
        return 0.0

    return REALISM_MARGIN_COEFF * (ers_value / ers_baseline - 1.0)


def _labor_cost_multiplier(engine: Any, target_date: date) -> float:
    """Return multiplier on labor_pct_adjusted for rising wage costs."""
    wages = _lookup(engine, SERIES_AVG_WAGES, target_date)
    wages_baseline = _get_baseline(engine, SERIES_AVG_WAGES)

    if wages is None or not wages_baseline:
        return 1.0

    mult = 1.0 + REALISM_WAGES_COEFF * (wages / wages_baseline - 1.0)
    return float(np.clip(mult, *REALISM_LABOR_CLAMP))


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def is_available(force_disable: bool = False) -> bool:
    """Return True if the realism engine can connect to the database."""
    if force_disable:
        return False
    return _get_engine() is not None


def adjust(
    dept_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    target_date: date,
    force_disable: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply realism engine adjustments to dept and summary DataFrames.

    Returns DataFrames unchanged if the database is unavailable or
    force_disable is True.

    Expects dept_df to contain helper columns produced by Stage 1:
    base_margin_pct, avg_ticket_base, items_per_transaction, discount_pct.
    """
    if force_disable:
        return dept_df, summary_df

    engine = _get_engine()
    if engine is None:
        return dept_df, summary_df

    from knot_shore.sales_generator import apply_derivations  # noqa: PLC0415

    dept_df = dept_df.copy()
    summary_df = summary_df.copy()

    # Step 2: Apply sales volume multiplier to gross_sales
    vol_mult = _sales_volume_multiplier(engine, target_date)
    dept_df["gross_sales"] = (dept_df["gross_sales"] * vol_mult).round(2)

    # Step 3: Recalculate derivation chain (uses helper cols still present from Stage 1)
    rng = np.random.default_rng(target_date.toordinal() + 999_999)
    dept_df = apply_derivations(dept_df, rng)

    # Steps 4-5: Apply margin pressure per department
    dept_id_to_name = {d["department_id"]: d["department_name"] for d in DEPARTMENTS}
    dept_id_to_base_margin = {d["department_id"]: d["base_margin_pct"] for d in DEPARTMENTS}

    for dept_id, dept_name in dept_id_to_name.items():
        mask = dept_df["department_id"] == dept_id
        if not mask.any():
            continue

        base_m = dept_id_to_base_margin[dept_id]
        adj = _margin_adjustment(engine, target_date, dept_name)
        adjusted_m = float(np.clip(base_m + adj, REALISM_MARGIN_MIN, REALISM_MARGIN_MAX))

        net = dept_df.loc[mask, "net_sales"]
        dept_df.loc[mask, "cogs"] = (net * (1.0 - adjusted_m)).round(2)
        dept_df.loc[mask, "gross_margin"] = (net - dept_df.loc[mask, "cogs"]).round(2)
        dept_df.loc[mask, "gross_margin_pct"] = np.where(
            net != 0,
            (dept_df.loc[mask, "gross_margin"] / net).round(4),
            0.0,
        )

    # Step 6: Re-aggregate store_summary totals from adjusted dept_df
    store_id_to_profile = {s["store_id"]: s["trade_area_profile"] for s in STORES}
    labor_mult = _labor_cost_multiplier(engine, target_date)

    new_summary_rows = []
    for _, row in summary_df.iterrows():
        store_id = int(row["store_id"])
        s_rows = dept_df[dept_df["store_id"] == store_id]
        gross_total = round(float(s_rows["gross_sales"].sum()), 2)
        net_total = round(float(s_rows["net_sales"].sum()), 2)
        txn_total = int(s_rows["transactions"].sum())

        profile = store_id_to_profile[store_id]
        lp_base = labor_pct_adjusted(profile, target_date.year)

        # Scale existing labor_cost by the labor multiplier (preserves Stage 1 noise)
        new_labor = round(float(row["labor_cost"]) * labor_mult, 2)
        new_labor_pct = round(new_labor / net_total, 4) if net_total != 0 else 0.0

        new_summary_rows.append(
            {
                "date_key": row["date_key"],
                "store_id": store_id,
                "gross_sales_total": gross_total,
                "net_sales_total": net_total,
                "transactions_total": txn_total,
                "labor_cost": new_labor,
                "labor_cost_pct": new_labor_pct,
            }
        )

    summary_df = pd.DataFrame(new_summary_rows)
    return dept_df, summary_df


def clear_cache() -> None:
    """Clear series and baseline caches and reset DB connection state."""
    global _DB_ENGINE, _DB_AVAILABLE
    _series_cache.clear()
    _baseline_cache.clear()
    _DB_ENGINE = None
    _DB_AVAILABLE = None
