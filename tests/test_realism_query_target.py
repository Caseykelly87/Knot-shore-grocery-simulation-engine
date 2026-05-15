"""
test_realism_query_target.py

Regression guard for the realism layer's SQL query target.

The realism layer reads economic series from the ETL pipeline's database
table `raw.fact_economic_observations`, filtering on the `series_name`
column.  If the query target ever drifts away from that table or column,
every per-series lookup silently returns an empty DataFrame and every
multiplier defaults to neutral — Stage 2 connects to the database, logs
that it's active, then applies no adjustments at all.

These tests seed a SQLite database with two years of economic data
(2024 = baseline, 2025 = drift) and confirm:

  1. `_load_series` returns the seeded rows (the query finds data)
  2. Multipliers diverge meaningfully between a 2024 date and a 2025
     date (the query feeds real numbers into the multiplier formulas)
  3. `adjust()` end-to-end produces output that differs from the
     force-disabled baseline (the realism path actually mutates data)

The fixture uses SQLite with `ATTACH DATABASE ... AS raw`, mirroring the
ETL repo's dialect-aware schema handling so the same `raw.<table>` SQL
works against SQLite in tests and PostgreSQL in production.

Skipped when SQLAlchemy is not installed (the `realism` extra is
optional).
"""

from __future__ import annotations

from datetime import date

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")

from sqlalchemy import create_engine, event, text  # noqa: E402

import knot_shore.realism as realism  # noqa: E402


_SEED_ROWS = [
    # (series_id, series_name, date, value, source)
    ("UMCSENT", "SENTIMENT", "2024-01-01", 70.0, "FRED"),
    ("UMCSENT", "SENTIMENT", "2025-01-01", 85.0, "FRED"),
    ("UNRATE", "UNRATE", "2024-01-01", 4.0, "FRED"),
    ("UNRATE", "UNRATE", "2025-01-01", 3.5, "FRED"),
    ("CES0500000003", "AVG_WAGES", "2024-01-01", 30.0, "BLS"),
    ("CES0500000003", "AVG_WAGES", "2025-01-01", 33.0, "BLS"),
    ("ERS_FOOD_HOME", "ERS_FOOD_HOME", "2024-01-01", 100.0, "BLS"),
    ("ERS_FOOD_HOME", "ERS_FOOD_HOME", "2025-01-01", 110.0, "BLS"),
    ("ERS_ALL_FOOD", "ERS_ALL_FOOD", "2024-01-01", 100.0, "BLS"),
    ("ERS_ALL_FOOD", "ERS_ALL_FOOD", "2025-01-01", 108.0, "BLS"),
    ("ERS_FRUITS_VEG", "ERS_FRUITS_VEG", "2024-01-01", 100.0, "BLS"),
    ("ERS_FRUITS_VEG", "ERS_FRUITS_VEG", "2025-01-01", 112.0, "BLS"),
    ("ERS_MEATS", "ERS_MEATS", "2024-01-01", 100.0, "BLS"),
    ("ERS_MEATS", "ERS_MEATS", "2025-01-01", 109.0, "BLS"),
    ("ERS_DAIRY", "ERS_DAIRY", "2024-01-01", 100.0, "BLS"),
    ("ERS_DAIRY", "ERS_DAIRY", "2025-01-01", 107.0, "BLS"),
    ("ERS_CEREALS", "ERS_CEREALS", "2024-01-01", 100.0, "BLS"),
    ("ERS_CEREALS", "ERS_CEREALS", "2025-01-01", 106.0, "BLS"),
    ("ERS_BEVERAGES", "ERS_BEVERAGES", "2024-01-01", 100.0, "BLS"),
    ("ERS_BEVERAGES", "ERS_BEVERAGES", "2025-01-01", 105.0, "BLS"),
    ("ERS_FOOD_AWAY", "ERS_FOOD_AWAY", "2024-01-01", 100.0, "BLS"),
    ("ERS_FOOD_AWAY", "ERS_FOOD_AWAY", "2025-01-01", 104.0, "BLS"),
]


@pytest.fixture
def realism_db(tmp_path):
    """SQLite engine with `raw.fact_economic_observations` seeded for 2024 and 2025."""
    main_path = tmp_path / "main.db"
    raw_path = tmp_path / "raw.db"
    engine = create_engine(f"sqlite:///{main_path.as_posix()}")

    # Match the ETL repo's pattern: ATTACH the raw sibling on every pooled
    # connection so `raw.<table>` resolves uniformly across dialects.
    @event.listens_for(engine, "connect")
    def _attach_raw(dbapi_conn, _record):
        cur = dbapi_conn.cursor()
        try:
            cur.execute(f"ATTACH DATABASE '{raw_path.as_posix()}' AS raw")
        finally:
            cur.close()

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.fact_economic_observations (
                series_id   TEXT NOT NULL,
                series_name TEXT NOT NULL,
                date        TEXT NOT NULL,
                value       DOUBLE PRECISION,
                source      TEXT NOT NULL,
                PRIMARY KEY (series_id, date)
            )
        """))
        for sid, sname, d, v, src in _SEED_ROWS:
            conn.execute(
                text("""
                    INSERT INTO raw.fact_economic_observations
                    (series_id, series_name, date, value, source)
                    VALUES (:sid, :sname, :d, :v, :src)
                """),
                {"sid": sid, "sname": sname, "d": d, "v": v, "src": src},
            )
        conn.commit()

    realism.clear_cache()
    yield engine
    realism.clear_cache()
    engine.dispose()


def test_load_series_returns_rows_from_raw_schema(realism_db):
    """The corrected query must find seeded rows in raw.fact_economic_observations.

    Targets _load_series_from_db directly so this remains a focused SQL
    regression guard regardless of how data resolution routes between the
    database and the bundled fixture.
    """
    df = realism._load_series_from_db(realism_db, "SENTIMENT")

    assert not df.empty, "_load_series_from_db returned empty — query did not find seeded rows"
    assert list(df.columns) == ["date", "value"], \
        f"Expected columns ['date', 'value'], got {list(df.columns)}"
    assert len(df) == 2, f"Expected 2 SENTIMENT rows, got {len(df)}"
    assert df["value"].tolist() == [70.0, 85.0]


def test_multipliers_differ_between_2024_and_2025(realism_db, monkeypatch):
    """Multipliers must diverge between baseline year and drift year.

    At a 2024 date every series equals its first (= baseline) observation,
    so all multipliers come out neutral.  At a 2025 date every series has
    drifted, so the multipliers must move off neutral.  If the SQL query
    silently returns empty rows, both years collapse to neutral and this
    assertion fails — that's the regression we're guarding.
    """
    monkeypatch.setattr(realism, "_get_engine", lambda: realism_db)
    realism.clear_cache()

    date_2024 = date(2024, 6, 15)
    date_2025 = date(2025, 6, 15)

    sales_2024 = realism._sales_volume_multiplier(realism_db, date_2024)
    sales_2025 = realism._sales_volume_multiplier(realism_db, date_2025)
    labor_2024 = realism._labor_cost_multiplier(realism_db, date_2024)
    labor_2025 = realism._labor_cost_multiplier(realism_db, date_2025)
    margin_2024 = realism._margin_adjustment(realism_db, date_2024, "Produce")
    margin_2025 = realism._margin_adjustment(realism_db, date_2025, "Produce")

    assert sales_2024 == pytest.approx(1.0), \
        f"2024 sales multiplier should be neutral (baseline year), got {sales_2024}"
    assert labor_2024 == pytest.approx(1.0), \
        f"2024 labor multiplier should be neutral (baseline year), got {labor_2024}"
    assert margin_2024 == pytest.approx(0.0), \
        f"2024 Produce margin adjustment should be neutral, got {margin_2024}"

    assert abs(sales_2025 - 1.0) > 0.01, \
        f"2025 sales multiplier should deviate from neutral, got {sales_2025}"
    assert abs(labor_2025 - 1.0) > 0.01, \
        f"2025 labor multiplier should deviate from neutral, got {labor_2025}"
    assert abs(margin_2025) > 0.001, \
        f"2025 Produce margin adjustment should deviate from neutral, got {margin_2025}"


def test_adjust_changes_output_when_realism_active(realism_db, monkeypatch):
    """End-to-end: adjust() with realism active must produce output that differs
    from the force-disabled baseline.  Confirms the corrected query feeds data
    all the way through the Stage 2 pipeline.
    """
    from knot_shore.config import DEPARTMENTS, GLOBAL_SEED, STORES
    from knot_shore.promotions import generate_promotions
    from knot_shore.sales_generator import generate_day

    target_date = date(2025, 6, 15)
    promos = generate_promotions(seed=GLOBAL_SEED)
    dept_df, summary_df = generate_day(
        target_date=target_date,
        stores=STORES,
        departments=DEPARTMENTS,
        promos_df=promos,
        global_seed=GLOBAL_SEED,
    )

    monkeypatch.setattr(realism, "_get_engine", lambda: realism_db)
    realism.clear_cache()

    dept_active, summary_active = realism.adjust(
        dept_df.copy(), summary_df.copy(), target_date, global_seed=GLOBAL_SEED
    )
    dept_disabled, summary_disabled = realism.adjust(
        dept_df.copy(), summary_df.copy(), target_date,
        force_disable=True, global_seed=GLOBAL_SEED,
    )

    assert not dept_active["gross_sales"].equals(dept_disabled["gross_sales"]), \
        "gross_sales identical with realism on vs off — Stage 2 did not engage"
    assert not summary_active["labor_cost"].equals(summary_disabled["labor_cost"]), \
        "labor_cost identical with realism on vs off — Stage 2 did not engage"
