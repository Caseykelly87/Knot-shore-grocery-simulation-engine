"""
test_store_locations_drift.py

Guard against silent drift between seed_data/store_locations.json and
config.STORES. Both encode the same eight store records; this test
enforces field-by-field equality so an edit to either source is caught.
"""

from __future__ import annotations

import json
from pathlib import Path

from knot_shore.config import STORES

_FIELDS = (
    "store_id",
    "store_name",
    "address",
    "city",
    "zip",
    "county_fips",
    "trade_area_profile",
    "sqft",
    "base_daily_revenue",
    "open_date",
)


def test_stores_config_matches_seed_json():
    repo_root = Path(__file__).resolve().parents[1]
    data = json.loads((repo_root / "seed_data" / "store_locations.json").read_text())["stores"]

    assert len(data) == len(STORES) == 8, (
        f"Record count mismatch: json={len(data)}, config={len(STORES)}"
    )

    for j, c in zip(data, STORES):
        for f in _FIELDS:
            jv, cv = j[f], c[f]
            if f == "base_daily_revenue":
                assert float(jv) == float(cv), (
                    f"{f} drift on store {j['store_id']}: json={jv} config={cv}"
                )
            else:
                assert jv == cv, (
                    f"{f} drift on store {j['store_id']}: json={jv} config={cv}"
                )
