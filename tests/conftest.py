"""
conftest.py — Shared fixtures for the test suite.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from knot_shore.config import DEPARTMENTS, GLOBAL_SEED, STORES
from knot_shore.promotions import generate_promotions
from knot_shore.sales_generator import generate_day

# A stable test date that falls on a non-holiday weekday in mid-range of the calendar
TEST_DATE = date(2025, 6, 15)  # Sunday — exercises weekend logic
TEST_DATE_WEEKDAY = date(2025, 6, 10)  # Tuesday


@pytest.fixture(scope="session")
def promos_df() -> pd.DataFrame:
    """Full 4-year promotion schedule (generated once for the whole test session)."""
    return generate_promotions(seed=GLOBAL_SEED)


@pytest.fixture(scope="session")
def dept_df_and_summary(promos_df) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Department sales and store summary DataFrames for TEST_DATE."""
    return generate_day(
        target_date=TEST_DATE,
        stores=STORES,
        departments=DEPARTMENTS,
        promos_df=promos_df,
        global_seed=GLOBAL_SEED,
    )


@pytest.fixture(scope="session")
def dept_df(dept_df_and_summary) -> pd.DataFrame:
    return dept_df_and_summary[0]


@pytest.fixture(scope="session")
def summary_df(dept_df_and_summary) -> pd.DataFrame:
    return dept_df_and_summary[1]
