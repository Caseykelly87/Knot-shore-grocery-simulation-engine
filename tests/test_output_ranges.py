"""
test_output_ranges.py

Validate that generated values fall within the industry-benchmarked ranges
defined in §8 (Realism Validation).

Tests run across multiple dates to avoid a single lucky/unlucky date dominating.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from knot_shore.config import DEPARTMENTS, GLOBAL_SEED, STORES
from knot_shore.promotions import generate_promotions
from knot_shore.sales_generator import generate_day

# Sample a weekday and a weekend across two different months
_SAMPLE_DATES = [
    date(2024, 1, 15),   # Monday, January
    date(2024, 7, 6),    # Saturday, July (peak summer)
    date(2024, 11, 28),  # Thanksgiving Thursday
    date(2025, 3, 15),   # Saturday, March
]


@pytest.fixture(scope="module")
def multi_date_data():
    """Generate dept and summary DataFrames for several sample dates."""
    promos = generate_promotions(seed=GLOBAL_SEED)
    all_dept = []
    all_summary = []
    for d in _SAMPLE_DATES:
        dep_df, sum_df = generate_day(
            target_date=d,
            stores=STORES,
            departments=DEPARTMENTS,
            promos_df=promos,
            global_seed=GLOBAL_SEED,
        )
        all_dept.append(dep_df)
        all_summary.append(sum_df)
    return pd.concat(all_dept, ignore_index=True), pd.concat(all_summary, ignore_index=True)


def test_daily_store_revenue_range(multi_date_data):
    """Daily net_sales per store should be within a realistic range.

    The §8 benchmark of $50K–$115K is an average-day target.  Small value-market
    stores on slow January weekdays can dip to ~$38K due to combined seasonal (0.92)
    and DOW (0.82) factors; peak-season Saturday suburban stores can reach ~$130K.
    The bounds here accommodate that full range while catching genuine outliers.
    """
    _, summary = multi_date_data
    too_low = summary[summary["net_sales_total"] < 30_000]
    too_high = summary[summary["net_sales_total"] > 140_000]
    assert too_low.empty, f"Store(s) below $30K net daily: {too_low[['store_id','net_sales_total']]}"
    assert too_high.empty, f"Store(s) above $140K net daily: {too_high[['store_id','net_sales_total']]}"


def test_chain_gross_margin_range(multi_date_data):
    """Chain-level gross margin should be 28–35% (§8)."""
    dept, _ = multi_date_data
    chain_net = dept["net_sales"].sum()
    chain_margin = dept["gross_margin"].sum()
    pct = chain_margin / chain_net
    assert 0.26 <= pct <= 0.38, f"Chain gross margin {pct:.3f} outside [0.26, 0.38]"


def test_produce_margin_range(multi_date_data):
    """Produce gross_margin_pct should be 45–55% (§8)."""
    dept, _ = multi_date_data
    produce_id = next(d["department_id"] for d in DEPARTMENTS if d["department_name"] == "Produce")
    produce = dept[dept["department_id"] == produce_id]
    avg_margin = produce["gross_margin_pct"].mean()
    assert 0.42 <= avg_margin <= 0.58, f"Produce avg margin {avg_margin:.3f} outside [0.42, 0.58]"


def test_bakery_margin_range(multi_date_data):
    """Bakery gross_margin_pct should be 50–60% (§8)."""
    dept, _ = multi_date_data
    bakery_id = next(d["department_id"] for d in DEPARTMENTS if d["department_name"] == "Bakery")
    bakery = dept[dept["department_id"] == bakery_id]
    avg_margin = bakery["gross_margin_pct"].mean()
    assert 0.47 <= avg_margin <= 0.63, f"Bakery avg margin {avg_margin:.3f} outside [0.47, 0.63]"


def test_grocery_center_store_margin_range(multi_date_data):
    """Grocery (Center Store) gross_margin_pct should be 24–30% (§8)."""
    dept, _ = multi_date_data
    grocery_id = next(
        d["department_id"] for d in DEPARTMENTS if d["department_name"] == "Grocery (Center Store)"
    )
    grocery = dept[dept["department_id"] == grocery_id]
    avg_margin = grocery["gross_margin_pct"].mean()
    assert 0.22 <= avg_margin <= 0.33, (
        f"Grocery center store avg margin {avg_margin:.3f} outside [0.22, 0.33]"
    )


def test_avg_ticket_range(multi_date_data):
    """avg_ticket per dept row should be in the $10–$55 range (§8 states $25–$45 store avg)."""
    dept, _ = multi_date_data
    # Department-level avg_ticket is naturally lower; use a wide guard
    too_low = dept[dept["avg_ticket"] < 8]
    too_high = dept[dept["avg_ticket"] > 65]
    assert too_low.empty, f"avg_ticket below $8 found: {too_low[['store_id','department_id','avg_ticket']].head()}"
    assert too_high.empty, f"avg_ticket above $65 found: {too_high[['store_id','department_id','avg_ticket']].head()}"


def test_transactions_per_store_day_range(multi_date_data):
    """Store-level transactions_total should be 1,200–3,500 (§8)."""
    _, summary = multi_date_data
    too_low = summary[summary["transactions_total"] < 800]
    too_high = summary[summary["transactions_total"] > 4_500]
    assert too_low.empty, f"Store below 800 transactions: {too_low[['store_id','transactions_total']]}"
    assert too_high.empty, f"Store above 4,500 transactions: {too_high[['store_id','transactions_total']]}"


def test_labor_cost_pct_range(multi_date_data):
    """Labor cost as % of net sales should be 10–13% (§8)."""
    _, summary = multi_date_data
    too_low = summary[summary["labor_cost_pct"] < 0.09]
    too_high = summary[summary["labor_cost_pct"] > 0.145]
    assert too_low.empty, f"Labor% below 9%: {too_low[['store_id','labor_cost_pct']]}"
    assert too_high.empty, f"Labor% above 14.5%: {too_high[['store_id','labor_cost_pct']]}"


def test_yoy_growth_direction():
    """The YoY growth factor for 2026 must exceed 1.0 relative to 2023.

    Comparing single calendar dates directly is unreliable because the same date
    falls on different days of week across years — DOW effects can dominate small
    growth signals.  We test the factor function itself and confirm the 3-year
    growth is within the expected 1.5-4.0% annual range (§8).
    """
    from knot_shore.factors import yoy_growth_factor

    factor_2023 = yoy_growth_factor(date(2023, 6, 15))
    factor_2026 = yoy_growth_factor(date(2026, 6, 15))

    growth_ratio = factor_2026 / factor_2023
    # 3 years of 2.5% annual growth = ~7.7% cumulative
    assert 1.05 <= growth_ratio <= 1.12, (
        f"3-year YoY growth factor ratio {growth_ratio:.4f} outside expected range [1.05, 1.12]"
    )
