"""
test_waterfall_integrity.py

Verify that the derivation chain invariants hold for every row produced by Stage 1:
  net_sales       = gross_sales − discount_amount
  gross_margin    = net_sales − cogs
  gross_margin_pct = gross_margin / net_sales
  avg_ticket      = net_sales / transactions
  discount_rate   = discount_amount / gross_sales  (0 when gross_sales == 0)
  promo_flag=False → discount_amount == 0.00
"""

from __future__ import annotations

import pytest
import pandas as pd
import numpy as np


TOLERANCE = 0.02  # $0.02 rounding tolerance


def test_net_sales_equals_gross_minus_discount(dept_df):
    expected = (dept_df["gross_sales"] - dept_df["discount_amount"]).round(2)
    diff = (dept_df["net_sales"] - expected).abs()
    assert diff.max() <= TOLERANCE, (
        f"net_sales ≠ gross_sales − discount_amount (max diff {diff.max():.4f})"
    )


def test_gross_margin_equals_net_minus_cogs(dept_df):
    expected = (dept_df["net_sales"] - dept_df["cogs"]).round(2)
    diff = (dept_df["gross_margin"] - expected).abs()
    assert diff.max() <= TOLERANCE, (
        f"gross_margin ≠ net_sales − cogs (max diff {diff.max():.4f})"
    )


def test_gross_margin_pct_matches_ratio(dept_df):
    mask = dept_df["net_sales"] != 0
    df = dept_df[mask]
    expected = (df["gross_margin"] / df["net_sales"]).round(4)
    diff = (df["gross_margin_pct"] - expected).abs()
    assert diff.max() <= 0.001, (
        f"gross_margin_pct ≠ gross_margin / net_sales (max diff {diff.max():.6f})"
    )


def test_avg_ticket_matches_ratio(dept_df):
    mask = dept_df["transactions"] > 0
    df = dept_df[mask]
    expected = (df["net_sales"] / df["transactions"]).round(2)
    diff = (df["avg_ticket"] - expected).abs()
    assert diff.max() <= TOLERANCE, (
        f"avg_ticket ≠ net_sales / transactions (max diff {diff.max():.4f})"
    )


def test_discount_rate_matches_ratio(dept_df):
    mask = dept_df["gross_sales"] != 0
    df = dept_df[mask]
    expected = (df["discount_amount"] / df["gross_sales"]).round(4)
    diff = (df["discount_rate"] - expected).abs()
    assert diff.max() <= 0.001, (
        f"discount_rate ≠ discount_amount / gross_sales (max diff {diff.max():.6f})"
    )


def test_no_discount_when_promo_flag_false(dept_df):
    no_promo = dept_df[~dept_df["promo_flag"]]
    bad = no_promo[no_promo["discount_amount"] != 0.0]
    assert bad.empty, (
        f"promo_flag=False rows have non-zero discount_amount: {bad[['store_id','department_id','discount_amount']].head()}"
    )


def test_no_negative_gross_sales(dept_df):
    bad = dept_df[dept_df["gross_sales"] < 0]
    assert bad.empty, "Negative gross_sales found"


def test_no_negative_transactions(dept_df):
    bad = dept_df[dept_df["transactions"] < 1]
    assert bad.empty, "Zero or negative transactions found"


def test_no_negative_units_sold(dept_df):
    bad = dept_df[dept_df["units_sold"] < 1]
    assert bad.empty, "Zero or negative units_sold found"
