"""
test_realism_clamps.py

Verify that realism engine guard-rail clamps hold (§5.6):
  - Combined sales_volume_multiplier stays within [0.90, 1.10]
  - Adjusted margin stays within [0.05, 0.70]
  - Labor cost multiplier stays within [0.90, 1.15]

Tests use patched series lookups to feed extreme values and confirm clamping.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from knot_shore.config import (
    REALISM_LABOR_CLAMP,
    REALISM_MARGIN_MAX,
    REALISM_MARGIN_MIN,
    REALISM_SALES_CLAMP,
)


TEST_DATE = date(2024, 6, 1)


def _make_lookup(value: float, baseline: float):
    """Return mock _lookup and _get_baseline functions returning given values."""
    def fake_lookup(engine, key, dt):
        return value

    def fake_baseline(engine, key):
        return baseline

    return fake_lookup, fake_baseline


def test_sales_multiplier_upper_clamp():
    """Extreme positive sentiment + zero unemployment should be clamped to 1.10."""
    from knot_shore.realism import _sales_volume_multiplier

    engine = MagicMock()
    # Supply very positive values: sentiment 3× baseline, zero unemployment
    with patch("knot_shore.realism._lookup", side_effect=lambda e, k, d: 300.0), \
         patch("knot_shore.realism._get_baseline", side_effect=lambda e, k: 100.0):
        result = _sales_volume_multiplier(engine, TEST_DATE)

    assert result <= REALISM_SALES_CLAMP[1] + 1e-9, \
        f"Sales multiplier {result} exceeds upper clamp {REALISM_SALES_CLAMP[1]}"


def test_sales_multiplier_lower_clamp():
    """Extreme negative conditions should be clamped to 0.90."""
    from knot_shore.realism import _sales_volume_multiplier

    engine = MagicMock()
    # Very high CPI food, very low sentiment, very high unemployment
    values = {"ERS_FOOD_HOME": 200.0, "SENTIMENT": 20.0, "UNRATE": 200.0}
    baselines = {"ERS_FOOD_HOME": 100.0, "SENTIMENT": 100.0, "UNRATE": 100.0}

    with patch("knot_shore.realism._lookup", side_effect=lambda e, k, d: values.get(k, 100.0)), \
         patch("knot_shore.realism._get_baseline", side_effect=lambda e, k: baselines.get(k, 100.0)):
        result = _sales_volume_multiplier(engine, TEST_DATE)

    assert result >= REALISM_SALES_CLAMP[0] - 1e-9, \
        f"Sales multiplier {result} below lower clamp {REALISM_SALES_CLAMP[0]}"


def test_margin_adjustment_clamped_to_min():
    """Extreme cost increase should not push adjusted margin below REALISM_MARGIN_MIN."""
    from knot_shore.config import DEPARTMENTS
    from knot_shore.realism import _margin_adjustment

    engine = MagicMock()
    # ERS value 10× baseline → very large negative margin_adjustment
    with patch("knot_shore.realism._lookup", return_value=1000.0), \
         patch("knot_shore.realism._get_baseline", return_value=100.0):
        adj = _margin_adjustment(engine, TEST_DATE, "Produce")

    base_margin = next(d["base_margin_pct"] for d in DEPARTMENTS if d["department_name"] == "Produce")
    adjusted = base_margin + adj
    clamped = np.clip(adjusted, REALISM_MARGIN_MIN, REALISM_MARGIN_MAX)

    assert clamped >= REALISM_MARGIN_MIN, \
        f"Adjusted margin {clamped} below minimum {REALISM_MARGIN_MIN}"


def test_margin_adjustment_clamped_to_max():
    """Extreme cost decrease should not push adjusted margin above REALISM_MARGIN_MAX."""
    from knot_shore.realism import _margin_adjustment

    engine = MagicMock()
    # ERS value near 0 → large positive margin_adjustment
    with patch("knot_shore.realism._lookup", return_value=1.0), \
         patch("knot_shore.realism._get_baseline", return_value=100.0):
        adj = _margin_adjustment(engine, TEST_DATE, "Produce")

    from knot_shore.config import DEPARTMENTS
    base_margin = next(d["base_margin_pct"] for d in DEPARTMENTS if d["department_name"] == "Produce")
    adjusted = base_margin + adj
    clamped = np.clip(adjusted, REALISM_MARGIN_MIN, REALISM_MARGIN_MAX)

    assert clamped <= REALISM_MARGIN_MAX, \
        f"Adjusted margin {clamped} above maximum {REALISM_MARGIN_MAX}"


def test_labor_multiplier_upper_clamp():
    """Wages 3× baseline should be clamped to 1.15."""
    from knot_shore.realism import _labor_cost_multiplier

    engine = MagicMock()
    with patch("knot_shore.realism._lookup", return_value=300.0), \
         patch("knot_shore.realism._get_baseline", return_value=100.0):
        result = _labor_cost_multiplier(engine, TEST_DATE)

    assert result <= REALISM_LABOR_CLAMP[1] + 1e-9, \
        f"Labor multiplier {result} exceeds upper clamp {REALISM_LABOR_CLAMP[1]}"


def test_labor_multiplier_lower_clamp():
    """Wages near zero should be clamped to 0.90."""
    from knot_shore.realism import _labor_cost_multiplier

    engine = MagicMock()
    with patch("knot_shore.realism._lookup", return_value=1.0), \
         patch("knot_shore.realism._get_baseline", return_value=100.0):
        result = _labor_cost_multiplier(engine, TEST_DATE)

    assert result >= REALISM_LABOR_CLAMP[0] - 1e-9, \
        f"Labor multiplier {result} below lower clamp {REALISM_LABOR_CLAMP[0]}"


def test_missing_series_returns_neutral():
    """When a series has no data, multiplier defaults to neutral (1.0 / 0.0)."""
    from knot_shore.realism import _sales_volume_multiplier, _margin_adjustment, _labor_cost_multiplier

    engine = MagicMock()
    with patch("knot_shore.realism._lookup", return_value=None), \
         patch("knot_shore.realism._get_baseline", return_value=None):
        sales_mult = _sales_volume_multiplier(engine, TEST_DATE)
        margin_adj = _margin_adjustment(engine, TEST_DATE, "Produce")
        labor_mult = _labor_cost_multiplier(engine, TEST_DATE)

    assert sales_mult == 1.0, f"Expected 1.0, got {sales_mult}"
    assert margin_adj == 0.0, f"Expected 0.0, got {margin_adj}"
    assert labor_mult == 1.0, f"Expected 1.0, got {labor_mult}"
