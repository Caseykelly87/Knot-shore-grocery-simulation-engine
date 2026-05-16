"""
realism.py — Stage 2: Realism Engine.

Reads real economic series and applies multipliers to the base-generated
DataFrames. The data source is resolved once per run via a three-tier
precedence:

  1. Database — if KNOT_SHORE_DB_URL is set, the database is reachable,
     and raw.fact_economic_observations supplies the full realism series
     set, the database is used.
  2. Bundled fixture — if the database is unavailable or incomplete, the
     bundled parquet fixture at seed_data/economic/ is used for the
     whole run. Falling back is logged at warning level so a person
     running in what they think is "live" mode sees the degraded path.
  3. Skip — if both are absent, Stage 2 is skipped and base data is
     returned unchanged. This is a broken-install state, not a normal
     operating mode.

Whole-layer fallback: the decision is made once per call to adjust();
sources are never mixed within a run.

Connection (DB path): set KNOT_SHORE_DB_URL.
  e.g. postgresql://user:pass@host:5432/dbname

Series used (§5.3):
  ERS_FOOD_HOME — food-at-home CPI   → sales volume multiplier
  SENTIMENT     — consumer sentiment → sales volume multiplier
  UNRATE        — unemployment rate  → sales volume multiplier
  ERS_*         — category-level CPI → margin pressure per department
  AVG_WAGES     — average wages      → labor cost multiplier
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import structlog

from knot_shore.config import (
    DEPARTMENTS,
    ERS_DEPT_MAP,
    GLOBAL_SEED,
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
    SERIES_ERS_BEVERAGES,
    SERIES_ERS_CEREALS,
    SERIES_ERS_DAIRY,
    SERIES_ERS_FOOD_AWAY,
    SERIES_ERS_FOOD_HOME,
    SERIES_ERS_FRUITS_VEG,
    SERIES_ERS_MEATS,
    SERIES_SENTIMENT,
    SERIES_UNRATE,
)

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Series the realism layer requires from any data source. Both the DB-query
# path and the bundled fixture must supply every name in this set, otherwise
# the source is considered incomplete.
# ---------------------------------------------------------------------------

REALISM_SERIES: frozenset[str] = frozenset(
    {
        SERIES_SENTIMENT,
        SERIES_UNRATE,
        SERIES_AVG_WAGES,
        SERIES_ERS_ALL_FOOD,
        SERIES_ERS_FOOD_HOME,
        SERIES_ERS_FOOD_AWAY,
        SERIES_ERS_CEREALS,
        SERIES_ERS_MEATS,
        SERIES_ERS_DAIRY,
        SERIES_ERS_FRUITS_VEG,
        SERIES_ERS_BEVERAGES,
    }
)

# ---------------------------------------------------------------------------
# Bundled fixture location (offline-mode data source).
# ---------------------------------------------------------------------------

# src/knot_shore/realism.py → parents[2] is the repo root
_REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLED_FIXTURE_PATH: Path = _REPO_ROOT / "seed_data" / "economic" / "economic_observations.parquet"

# ---------------------------------------------------------------------------
# Module-level state (cleared by clear_cache())
# ---------------------------------------------------------------------------

_DB_ENGINE: Any = None       # sqlalchemy Engine or None
_DB_AVAILABLE: bool | None = None  # None means not yet checked

# Source resolution state for the current run.
# "database" / "bundled_fixture" / "none" / None (not yet resolved)
_SOURCE: str | None = None
_FIXTURE_FRAME: pd.DataFrame | None = None  # full parquet, loaded once

# Per-series caches (populated lazily once a source is resolved)
_series_cache: dict[str, pd.DataFrame] = {}
_baseline_cache: dict[str, float] = {}


# ---------------------------------------------------------------------------
# Database connection (lazy, optional)
# ---------------------------------------------------------------------------

def _get_engine() -> Any | None:
    """Return a SQLAlchemy engine if KNOT_SHORE_DB_URL is set and reachable.

    Does not emit user-facing logs about resolved state — that is the job
    of _resolve_source(). This helper only reports low-level connection
    failures.
    """
    global _DB_ENGINE, _DB_AVAILABLE

    if _DB_AVAILABLE is not None:
        return _DB_ENGINE if _DB_AVAILABLE else None

    db_url = os.environ.get("KNOT_SHORE_DB_URL", "").strip()
    if not db_url:
        _DB_AVAILABLE = False
        return None

    try:
        from sqlalchemy import create_engine, text  # noqa: PLC0415

        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        _DB_ENGINE = engine
        _DB_AVAILABLE = True
        return _DB_ENGINE
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "realism_db_connect_failed",
            stage=2,
            error=str(exc),
        )
        _DB_AVAILABLE = False
        return None


# ---------------------------------------------------------------------------
# Database series loading (unchanged SQL)
# ---------------------------------------------------------------------------

def _load_series_from_db(engine: Any, series_key: str) -> pd.DataFrame:
    """Load a series from raw.fact_economic_observations as [date, value]."""
    try:
        from sqlalchemy import text  # noqa: PLC0415

        query = """
            SELECT date, value
            FROM raw.fact_economic_observations
            WHERE series_name = :key
            ORDER BY date ASC
        """
        with engine.connect() as conn:
            result = conn.execute(text(query), {"key": series_key})
            rows = result.fetchall()

        if not rows:
            return pd.DataFrame(columns=["date", "value"])

        df = pd.DataFrame(rows, columns=["date", "value"])
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.sort_values("date").reset_index(drop=True)
        return df

    except Exception as exc:  # noqa: BLE001
        logger.warning("realism_db_load_failed", series=series_key, error=str(exc))
        return pd.DataFrame(columns=["date", "value"])


# ---------------------------------------------------------------------------
# Bundled fixture loading
# ---------------------------------------------------------------------------

def _load_fixture_frame() -> pd.DataFrame | None:
    """Read the bundled parquet once and cache. Return None if missing or unreadable.

    Returns the full multi-series DataFrame with columns
    [series_id, series_name, date, value, source]. Per-series filtering
    happens in _load_series_from_fixture.
    """
    global _FIXTURE_FRAME

    if _FIXTURE_FRAME is not None:
        return _FIXTURE_FRAME

    if not BUNDLED_FIXTURE_PATH.exists():
        return None

    try:
        df = pd.read_parquet(BUNDLED_FIXTURE_PATH)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "realism_fixture_read_failed",
            path=str(BUNDLED_FIXTURE_PATH),
            error=str(exc),
        )
        return None

    _FIXTURE_FRAME = df
    return _FIXTURE_FRAME


def _fixture_series_set() -> set[str]:
    """Return the set of series_names present in the bundled fixture."""
    df = _load_fixture_frame()
    if df is None or df.empty or "series_name" not in df.columns:
        return set()
    return set(df["series_name"].unique())


def _load_series_from_fixture(series_key: str) -> pd.DataFrame:
    """Slice the bundled fixture for a series; return [date, value] like the DB path."""
    df = _load_fixture_frame()
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "value"])

    sub = df[df["series_name"] == series_key][["date", "value"]].copy()
    if sub.empty:
        return pd.DataFrame(columns=["date", "value"])

    sub["date"] = pd.to_datetime(sub["date"]).dt.date
    sub["value"] = sub["value"].astype(float)
    sub = sub.sort_values("date").reset_index(drop=True)
    return sub


# ---------------------------------------------------------------------------
# Source resolution (the three-tier precedence)
# ---------------------------------------------------------------------------

def _resolve_source() -> str:
    """Resolve the data source once per run and log the resolution.

    Returns one of:
      - "database"         — DB reachable and supplies the full realism set
      - "bundled_fixture"  — DB unavailable or incomplete; fixture is used
      - "none"             — neither available; Stage 2 will be skipped
    """
    global _SOURCE

    if _SOURCE is not None:
        return _SOURCE

    db_url_set = bool(os.environ.get("KNOT_SHORE_DB_URL", "").strip())
    engine = _get_engine()

    # If the DB connected, check that it supplies the full realism set.
    if engine is not None:
        try:
            from sqlalchemy import text  # noqa: PLC0415

            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT DISTINCT series_name "
                        "FROM raw.fact_economic_observations"
                    )
                ).fetchall()
            db_series = {r[0] for r in rows}
        except Exception as exc:  # noqa: BLE001
            logger.warning("realism_db_series_probe_failed", error=str(exc))
            db_series = set()

        missing = sorted(REALISM_SERIES - db_series)
        if not missing:
            _SOURCE = "database"
            logger.info(
                "realism_source",
                stage=2,
                source="database",
                series_count=len(REALISM_SERIES),
            )
            return _SOURCE

        # DB connected but incomplete — fall back to fixture.
        fixture_missing = sorted(REALISM_SERIES - _fixture_series_set())
        if not fixture_missing and _load_fixture_frame() is not None:
            _SOURCE = "bundled_fixture"
            logger.warning(
                "realism_source",
                stage=2,
                source="bundled_fixture",
                reason="database_missing_series",
                missing_series=missing,
            )
            return _SOURCE

        _SOURCE = "none"
        logger.warning(
            "realism_source",
            stage=2,
            source="none",
            reason="database_incomplete_and_fixture_missing",
            missing_series=missing,
        )
        return _SOURCE

    # DB not connected — try the fixture.
    fixture_df = _load_fixture_frame()
    if fixture_df is not None:
        fixture_missing = sorted(REALISM_SERIES - _fixture_series_set())
        if not fixture_missing:
            reason = (
                "database_unreachable" if db_url_set else "db_url_not_set"
            )
            log = logger.warning if db_url_set else logger.info
            log(
                "realism_source",
                stage=2,
                source="bundled_fixture",
                reason=reason,
            )
            _SOURCE = "bundled_fixture"
            return _SOURCE

        _SOURCE = "none"
        logger.warning(
            "realism_source",
            stage=2,
            source="none",
            reason="fixture_missing_series",
            missing_series=fixture_missing,
        )
        return _SOURCE

    # Neither DB nor fixture.
    _SOURCE = "none"
    logger.warning(
        "realism_source",
        stage=2,
        source="none",
        reason="no_db_and_no_fixture",
    )
    return _SOURCE


# ---------------------------------------------------------------------------
# Unified series accessors used by the multiplier functions
# ---------------------------------------------------------------------------

def _load_series(engine: Any, series_key: str) -> pd.DataFrame:
    """Load a series from the resolved source as [date, value].

    The ``engine`` argument is retained for back-compatibility with
    existing tests that pass a mock engine; when the resolved source is
    the database it is consulted, when the source is the fixture it is
    ignored.
    """
    if series_key in _series_cache:
        return _series_cache[series_key]

    source = _SOURCE if _SOURCE is not None else _resolve_source()

    if source == "database":
        df = _load_series_from_db(engine, series_key)
    elif source == "bundled_fixture":
        df = _load_series_from_fixture(series_key)
    else:
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
# Multiplier functions (§5.3) — formulas unchanged
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
    """Return additive margin adjustment for a department on a date."""
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
    """Return True if the realism engine has a usable data source.

    A source is usable if either the database supplies the full realism
    series set, or the bundled fixture is present and complete.
    """
    if force_disable:
        return False
    return _resolve_source() in ("database", "bundled_fixture")


def adjust(
    dept_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    target_date: date,
    force_disable: bool = False,
    global_seed: int = GLOBAL_SEED,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply realism engine adjustments to dept and summary DataFrames.

    Returns DataFrames unchanged if no data source is available or
    force_disable is True.

    Expects dept_df to contain helper columns produced by Stage 1:
    base_margin_pct, avg_ticket_base, items_per_transaction, discount_pct.
    """
    if force_disable:
        return dept_df, summary_df

    source = _resolve_source()
    if source == "none":
        return dept_df, summary_df

    # engine is consulted only when source == "database"; for the fixture
    # path the unified _load_series ignores it.
    engine = _get_engine() if source == "database" else None

    from knot_shore.sales_generator import apply_derivations  # noqa: PLC0415

    dept_df = dept_df.copy()
    summary_df = summary_df.copy()

    # Step 2: Apply sales volume multiplier to gross_sales
    vol_mult = _sales_volume_multiplier(engine, target_date)
    dept_df["gross_sales"] = (dept_df["gross_sales"] * vol_mult).round(2)

    # Step 3: Recalculate derivation chain (uses helper cols still present from Stage 1)
    rng = np.random.default_rng(global_seed + target_date.toordinal() + 999_999)
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
    labor_mult = _labor_cost_multiplier(engine, target_date)

    new_summary_rows = []
    for _, row in summary_df.iterrows():
        store_id = int(row["store_id"])
        s_rows = dept_df[dept_df["store_id"] == store_id]
        gross_total = round(float(s_rows["gross_sales"].sum()), 2)
        net_total = round(float(s_rows["net_sales"].sum()), 2)
        txn_total = int(s_rows["transactions"].sum())

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
    """Clear series and baseline caches, fixture cache, and DB connection state."""
    global _DB_ENGINE, _DB_AVAILABLE, _SOURCE, _FIXTURE_FRAME
    _series_cache.clear()
    _baseline_cache.clear()
    _DB_ENGINE = None
    _DB_AVAILABLE = None
    _SOURCE = None
    _FIXTURE_FRAME = None
