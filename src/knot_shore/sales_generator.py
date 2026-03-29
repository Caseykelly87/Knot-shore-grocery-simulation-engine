"""
sales_generator.py — Stage 1: Base generation waterfall.

Produces two DataFrames for a given date:
  department_sales_df  — one row per store × department (§3.1)
  store_summary_df     — one row per store (§3.2)

All generation is economically blind. No database access.
Deterministic per (date, global_seed): date_seed = global_seed + date.toordinal().
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from knot_shore.config import (
    AVG_TICKET_BASE,
    DEPARTMENTS,
    DEPT_SHARE,
    GLOBAL_SEED,
    ITEMS_PER_TRANSACTION,
    NOISE_CLIP_LOWER,
    NOISE_CLIP_UPPER,
    NOISE_SIGMA_LABOR,
    NOISE_SIGMA_SALES,
    NOISE_SIGMA_TICKET,
    NOISE_SIGMA_UNITS,
    STORES,
)
from knot_shore.factors import (
    dow_factor,
    labor_pct_adjusted,
    promo_volume_factor,
    seasonal_factor,
    snap_factor,
    yoy_growth_factor,
)


def _noise(rng: np.random.Generator, sigma: float, n: int = 1) -> np.ndarray:
    """Draw n noise values from N(1.0, sigma) clipped to [0.88, 1.12]."""
    vals = rng.normal(loc=1.0, scale=sigma, size=n)
    return np.clip(vals, NOISE_CLIP_LOWER, NOISE_CLIP_UPPER)


def apply_derivations(
    df: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Re-derive all fields from gross_sales down.

    Steps (§4.10):
      2. discount_amount = gross_sales × discount_pct   (0 if no promo)
      3. net_sales       = gross_sales − discount_amount
      4. cogs            = net_sales × (1 − base_margin_pct)
      5. gross_margin    = net_sales − cogs
      6. gross_margin_pct= gross_margin / net_sales
      7. transactions    = round(net_sales / (avg_ticket_base × noise_ticket))
      8. units_sold      = round(transactions × items_per_transaction × noise_units)
      9. avg_ticket      = net_sales / transactions
     10. discount_rate   = discount_amount / gross_sales  (0 if gross_sales == 0)

    Expects columns already present:
      gross_sales, discount_pct, promo_flag, base_margin_pct,
      avg_ticket_base, items_per_transaction

    The rng is advanced in row-order for ticket noise then units noise.
    """
    n = len(df)

    # Step 2
    df["discount_amount"] = (
        df["gross_sales"] * df["discount_pct"] * df["promo_flag"].astype(float)
    ).round(2)

    # Step 3
    df["net_sales"] = (df["gross_sales"] - df["discount_amount"]).round(2)

    # Step 4
    df["cogs"] = (df["net_sales"] * (1.0 - df["base_margin_pct"])).round(2)

    # Step 5
    df["gross_margin"] = (df["net_sales"] - df["cogs"]).round(2)

    # Step 6 — guard against zero net_sales
    df["gross_margin_pct"] = np.where(
        df["net_sales"] != 0,
        (df["gross_margin"] / df["net_sales"]).round(4),
        0.0,
    )

    # Step 7 — ticket noise drawn per row
    ticket_noise = _noise(rng, NOISE_SIGMA_TICKET, n)
    raw_transactions = df["net_sales"].values / (
        df["avg_ticket_base"].values * ticket_noise
    )
    df["transactions"] = np.maximum(raw_transactions, 1).round(0).astype(int)

    # Step 8 — units noise drawn per row
    units_noise = _noise(rng, NOISE_SIGMA_UNITS, n)
    raw_units = df["transactions"].values * df["items_per_transaction"].values * units_noise
    df["units_sold"] = np.maximum(raw_units, 1).round(0).astype(int)

    # Step 9
    df["avg_ticket"] = np.where(
        df["transactions"] != 0,
        (df["net_sales"] / df["transactions"]).round(2),
        0.0,
    )

    # Step 10
    df["discount_rate"] = np.where(
        df["gross_sales"] != 0,
        (df["discount_amount"] / df["gross_sales"]).round(4),
        0.0,
    )

    return df


def generate_day(
    target_date: date,
    stores: list[dict] | None = None,
    departments: list[dict] | None = None,
    promos_df: pd.DataFrame | None = None,
    global_seed: int = GLOBAL_SEED,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate department_sales and store_summary DataFrames for one date.

    Parameters
    ----------
    target_date:
        The date to generate data for.
    stores:
        List of store dicts (defaults to config.STORES).
    departments:
        List of department dicts (defaults to config.DEPARTMENTS).
    promos_df:
        Full promotion schedule DataFrame. Pass an empty DataFrame to
        disable promotions.
    global_seed:
        Master seed; date_seed = global_seed + target_date.toordinal().
    """
    if stores is None:
        stores = STORES
    if departments is None:
        departments = DEPARTMENTS
    if promos_df is None:
        promos_df = pd.DataFrame()

    # Deterministic seed for this date
    date_seed = global_seed + target_date.toordinal()
    rng = np.random.default_rng(date_seed)

    dow_num = target_date.isoweekday()          # 1=Monday, 7=Sunday
    month = target_date.month
    year = target_date.year
    is_snap = target_date.day <= 10
    yoy_factor = yoy_growth_factor(target_date)

    dept_rows: list[dict] = []

    for store in stores:
        profile = store["trade_area_profile"]
        base_rev = store["base_daily_revenue"]
        store_id = store["store_id"]

        for dept in departments:
            dept_name = dept["department_name"]
            dept_id = dept["department_id"]
            seasonal_profile = dept["seasonal_profile"]
            base_margin = dept["base_margin_pct"]

            # --- Waterfall (§4.1) ---
            share = DEPT_SHARE[profile][dept_name]
            sf = seasonal_factor(seasonal_profile, month)
            df_val = dow_factor(profile, dow_num)
            snapf = snap_factor(profile, is_snap)
            lift, disc_pct, promo_active = promo_volume_factor(
                dept_name, target_date, promos_df
            )
            sales_noise = float(_noise(rng, NOISE_SIGMA_SALES, 1)[0])

            gross = (
                base_rev
                * share
                * sf
                * df_val
                * snapf
                * lift
                * yoy_factor
                * sales_noise
            )
            gross = round(gross, 2)

            dept_rows.append(
                {
                    "date_key": target_date,
                    "store_id": store_id,
                    "department_id": dept_id,
                    # Waterfall anchor — derivation chain runs after all rows built
                    "gross_sales": gross,
                    # Carry-forward fields needed by apply_derivations
                    "discount_pct": disc_pct,
                    "promo_flag": promo_active,
                    "base_margin_pct": base_margin,
                    "avg_ticket_base": AVG_TICKET_BASE[profile],
                    "items_per_transaction": ITEMS_PER_TRANSACTION[dept_name],
                }
            )

    dept_df = pd.DataFrame(dept_rows)

    # Run derivation chain for all rows (ticket + units noise drawn here)
    dept_df = apply_derivations(dept_df, rng)

    # Helper columns (base_margin_pct, avg_ticket_base, items_per_transaction, discount_pct)
    # are kept in the DataFrame so Stage 2 (realism engine) can recalculate the derivation
    # chain after modifying gross_sales or margins.  output.py strips them before writing CSV.

    # --- Store summary (§3.2) ---
    summary_rows: list[dict] = []
    for store in stores:
        store_id = store["store_id"]
        profile = store["trade_area_profile"]

        s_rows = dept_df[dept_df["store_id"] == store_id]
        gross_total = round(s_rows["gross_sales"].sum(), 2)
        net_total = round(s_rows["net_sales"].sum(), 2)
        txn_total = int(s_rows["transactions"].sum())

        lp = labor_pct_adjusted(profile, year)
        labor_noise = float(_noise(rng, NOISE_SIGMA_LABOR, 1)[0])
        labor = round(net_total * lp * labor_noise, 2)
        labor_pct = round(labor / net_total, 4) if net_total != 0 else 0.0

        summary_rows.append(
            {
                "date_key": target_date,
                "store_id": store_id,
                "gross_sales_total": gross_total,
                "net_sales_total": net_total,
                "transactions_total": txn_total,
                "labor_cost": labor,
                "labor_cost_pct": labor_pct,
            }
        )

    summary_df = pd.DataFrame(summary_rows)
    return dept_df, summary_df
