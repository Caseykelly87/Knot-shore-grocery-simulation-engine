"""Refresh the bundled economic fixture from FRED and BLS.

Standalone maintenance script. Not imported by the engine's normal code
paths and not part of the CLI. Run manually about three to four times a
year to refresh the bundled parquet that feeds the realism layer's
offline mode.

Usage
-----
    # API keys are read from .env at the repo root
    python scripts/refresh_economic_fixtures.py

Inputs
------
- realism series list: imported from `knot_shore.config` so this script
  cannot drift from what the realism layer actually consults
- FRED_API_KEY, BLS_API_KEY: read from the repo-root `.env` (via
  `python-dotenv`), or from the process environment if already set

Outputs
-------
- seed_data/economic/economic_observations.parquet
- seed_data/economic/metadata.json (carries last_updated timestamp)

API budget reference
--------------------
- FRED free tier: 120 requests/minute. This script makes one request
  per FRED series (2 series). 60x headroom.
- BLS registered tier: 500 queries/day with up to 50 series per query.
  This script makes one batched query for all BLS series (9 series:
  8 food-category CPI indexes + 1 average-wages series).

Failure policy
--------------
Any failed fetch is a hard error. The script aborts and does NOT write
a partial fixture — a fixture missing any realism-set series is a
broken fixture by definition.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# requests, truststore, and python-dotenv are imported lazily inside the
# functions that use them so the module can be imported without the
# [fixtures] extra installed. The drift-guard test in
# tests/test_realism_fixture_fallback.py imports this script to compare
# its series catalog against the realism layer's REALISM_SERIES; that
# import path should not require the HTTP-client stack.

# The sim engine's package lives under src/; make it importable when
# this script is run from the repo root.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src"))

from knot_shore.config import (  # noqa: E402
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

logger = logging.getLogger("refresh_economic_fixtures")

# ---------------------------------------------------------------------------
# Series catalog. Maps the sim engine's realism-set series names to the
# source-specific identifiers the upstream APIs use. The realism layer's
# SERIES_ERS_* constants are retained for historical reasons — the food
# CPI data is now sourced from BLS (the underlying publisher), not the
# USDA ERS Food Price Outlook (which only forecasts annual percent
# changes, the wrong granularity for the multiplier math).
# ---------------------------------------------------------------------------

FRED_SERIES: dict[str, str] = {
    SERIES_SENTIMENT: "UMCSENT",
    SERIES_UNRATE:    "UNRATE",
}

# BLS series: average hourly earnings + 8 monthly food-category CPI
# indexes (CUUR* = U.S. city average, not seasonally adjusted). The 8
# CPI series IDs were verified against the live BLS API to confirm
# monthly granularity and 2023-present coverage.
BLS_SERIES: dict[str, str] = {
    SERIES_AVG_WAGES:       "CES0500000003",  # Avg hourly earnings, total private (SA)
    SERIES_ERS_ALL_FOOD:    "CUUR0000SAF1",   # Food
    SERIES_ERS_FOOD_HOME:   "CUUR0000SAF11",  # Food at home
    SERIES_ERS_FOOD_AWAY:   "CUUR0000SEFV",   # Food away from home
    SERIES_ERS_CEREALS:     "CUUR0000SAF111", # Cereals and bakery products
    SERIES_ERS_MEATS:       "CUUR0000SAF112", # Meats, poultry, fish, and eggs
    SERIES_ERS_DAIRY:       "CUUR0000SEFJ",   # Dairy and related products
    SERIES_ERS_FRUITS_VEG:  "CUUR0000SAF113", # Fruits and vegetables
    SERIES_ERS_BEVERAGES:   "CUUR0000SAF114", # Nonalcoholic beverages
}

# The complete set the refresh must produce; cross-checked against the
# realism layer's REALISM_SERIES constant at startup so the two cannot
# drift apart.
EXPECTED_SERIES: set[str] = set(FRED_SERIES.keys()) | set(BLS_SERIES.keys())


# Realism-set guard (imported lazily to keep import-time work minimal).
def _assert_series_match_realism_layer() -> None:
    from knot_shore.realism import REALISM_SERIES  # noqa: PLC0415

    if EXPECTED_SERIES != set(REALISM_SERIES):
        missing = sorted(set(REALISM_SERIES) - EXPECTED_SERIES)
        extra = sorted(EXPECTED_SERIES - set(REALISM_SERIES))
        raise RuntimeError(
            "Refresh script series catalog has drifted from realism layer: "
            f"missing={missing} extra={extra}"
        )


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

FIXTURE_DIR = _REPO_ROOT / "seed_data" / "economic"
FIXTURE_PATH = FIXTURE_DIR / "economic_observations.parquet"
METADATA_PATH = FIXTURE_DIR / "metadata.json"

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_BLS_BASE = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# Polite spacing between FRED requests. Well under the 120/min limit but
# documents intent for the next maintainer.
_FRED_REQUEST_SPACING_SECONDS = 0.5

_HTTP_TIMEOUT_SECONDS = 15


def fetch_with_retry(func):
    """Retry decorator with exponential backoff for transient network errors.

    Mirrors the ETL pipeline's pattern in src/extract.py.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        import requests  # noqa: PLC0415

        last_exc: BaseException | None = None
        for attempt in range(3):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                logger.warning(
                    "http_attempt_failed",
                    extra={
                        "attempt": attempt + 1,
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                    },
                )
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    raise
        # Defensive: should be unreachable, but keep mypy/pyright honest.
        raise RuntimeError(f"unreachable: {last_exc}")
    return wrapper


# ---------------------------------------------------------------------------
# FRED fetch
# ---------------------------------------------------------------------------

@fetch_with_retry
def fetch_fred_series(series_name: str, series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch a FRED observation series. Returns long-format DataFrame."""
    import requests  # noqa: PLC0415

    started = time.perf_counter()
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": "2023-01-01",
    }
    response = requests.get(_FRED_BASE, params=params, timeout=_HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()
    data = response.json()

    if "observations" not in data:
        raise RuntimeError(f"FRED response malformed for {series_id}")

    rows = []
    for obs in data["observations"]:
        if obs["value"] == ".":
            continue
        rows.append(
            {
                "series_id": series_id,
                "series_name": series_name,
                "date": pd.Timestamp(obs["date"]),
                "value": float(obs["value"]),
                "source": "FRED",
            }
        )

    elapsed_ms = round((time.perf_counter() - started) * 1000)
    logger.info(
        "fred_fetched",
        extra={
            "series_name": series_name,
            "series_id": series_id,
            "status": response.status_code,
            "row_count": len(rows),
            "elapsed_ms": elapsed_ms,
        },
    )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# BLS fetch (batched)
# ---------------------------------------------------------------------------

@fetch_with_retry
def fetch_bls_batch(series_map: dict[str, str], api_key: str) -> pd.DataFrame:
    """Batched BLS POST for all configured BLS series. Returns long-format DataFrame."""
    import requests  # noqa: PLC0415

    started = time.perf_counter()
    end_year = datetime.now(timezone.utc).year
    payload = {
        "seriesid": list(series_map.values()),
        "startyear": "2023",
        "endyear": str(end_year),
        "registrationkey": api_key,
    }
    response = requests.post(
        _BLS_BASE,
        json=payload,
        headers={"Content-type": "application/json"},
        timeout=_HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    data = response.json()

    if data.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API error: {data.get('message')}")

    id_to_name = {v: k for k, v in series_map.items()}

    rows = []
    for series in data["Results"]["series"]:
        series_id = series["seriesID"]
        series_name = id_to_name.get(series_id, series_id)
        for obs in series["data"]:
            period = obs["period"]
            # Skip annual averages ("M13") and any non-monthly periods.
            if not period.startswith("M") or period == "M13":
                continue
            month = int(period[1:])
            year = int(obs["year"])
            value = obs["value"]
            if value in ("-", "."):
                continue
            rows.append(
                {
                    "series_id": series_id,
                    "series_name": series_name,
                    "date": pd.Timestamp(year=year, month=month, day=1),
                    "value": float(value),
                    "source": "BLS",
                }
            )

    elapsed_ms = round((time.perf_counter() - started) * 1000)
    logger.info(
        "bls_fetched",
        extra={
            "series_count": len(series_map),
            "status": response.status_code,
            "row_count": len(rows),
            "elapsed_ms": elapsed_ms,
        },
    )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def _configure_logging() -> None:
    """Configure stdlib logging in a JSON-friendly shape.

    The sim engine ships a structlog configurator in
    src/knot_shore/observability.py, but this script intentionally stays
    on stdlib logging so it has no dependency on the package's runtime
    wiring (e.g., it can be invoked even before `pip install -e .`).
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s %(props)s")
    )

    class _PropsFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            extras = {
                k: v
                for k, v in record.__dict__.items()
                if k
                not in {
                    "name", "msg", "args", "levelname", "levelno", "pathname",
                    "filename", "module", "exc_info", "exc_text", "stack_info",
                    "lineno", "funcName", "created", "msecs", "relativeCreated",
                    "thread", "threadName", "processName", "process", "message",
                    "taskName",
                }
            }
            record.props = json.dumps(extras, default=str) if extras else ""
            return True

    handler.addFilter(_PropsFilter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)


def refresh() -> None:
    """Fetch every realism-set series and write the bundled fixture."""
    import truststore  # noqa: PLC0415
    from dotenv import load_dotenv  # noqa: PLC0415

    # Route TLS verification through the OS certificate store so the
    # script works under corporate cert-intercepting proxies that inject
    # a root CA into the system store but not into certifi's bundle.
    # No-op on a system whose trust roots match certifi's defaults.
    truststore.inject_into_ssl()

    _configure_logging()
    load_dotenv(_REPO_ROOT / ".env")
    _assert_series_match_realism_layer()

    fred_key = os.environ.get("FRED_API_KEY", "").strip()
    bls_key = os.environ.get("BLS_API_KEY", "").strip()
    if not fred_key:
        raise SystemExit("FRED_API_KEY is not set")
    if not bls_key:
        raise SystemExit("BLS_API_KEY is not set")

    frames: list[pd.DataFrame] = []

    # FRED — one request per series, with explicit spacing.
    for series_name, series_id in FRED_SERIES.items():
        frames.append(fetch_fred_series(series_name, series_id, fred_key))
        time.sleep(_FRED_REQUEST_SPACING_SECONDS)

    # BLS — single batched POST for all 9 BLS series.
    frames.append(fetch_bls_batch(BLS_SERIES, bls_key))

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["value"]).sort_values(
        ["series_name", "date"]
    ).reset_index(drop=True)

    # Hard guard: refuse to write a fixture missing any realism-set series.
    produced = set(combined["series_name"].unique())
    missing = EXPECTED_SERIES - produced
    if missing:
        raise RuntimeError(
            f"Refresh produced an incomplete fixture; missing series: {sorted(missing)}. "
            "Aborting without writing — a fixture missing series is a broken fixture."
        )

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(FIXTURE_PATH, engine="pyarrow", index=False)

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata = {
        "is_placeholder": False,
        "last_updated": now_iso,
        "series": sorted(produced),
        "row_count": int(len(combined)),
        "date_range": {
            "start": combined["date"].min().strftime("%Y-%m-%d"),
            "end":   combined["date"].max().strftime("%Y-%m-%d"),
        },
    }
    METADATA_PATH.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    logger.info(
        "refresh_complete",
        extra={
            "fixture_path": str(FIXTURE_PATH),
            "metadata_path": str(METADATA_PATH),
            "row_count": int(len(combined)),
            "series_count": len(produced),
            "last_updated": now_iso,
        },
    )


if __name__ == "__main__":
    refresh()
