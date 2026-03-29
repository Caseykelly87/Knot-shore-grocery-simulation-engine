"""
config.py — All static constants for the simulation engine.

Covers:
  - Store definitions and profiles (§2.1)
  - Department definitions (§2.2)
  - Department revenue share by store profile (§2.3)
  - Seasonal factor tables (§4.2)
  - Day-of-week factor tables (§4.3)
  - SNAP window factors (§4.4)
  - Labor cost percentages (§4.6)
  - Average ticket and items-per-transaction (§4.7)
  - YoY growth rate (§4.8)
  - Noise distribution parameters (§4.1)
  - Realism engine series keys and guard-rail clamps (§5)
"""

from datetime import date

# ---------------------------------------------------------------------------
# Global defaults
# ---------------------------------------------------------------------------

GLOBAL_SEED: int = 42
GENERATOR_VERSION: str = "1.0.0"

# Full calendar range covered by dim_calendar and promo schedule
CALENDAR_START: date = date(2023, 1, 1)
CALENDAR_END: date = date(2026, 12, 31)

# Noise distribution: N(µ=1.0, σ) clipped to [lower, upper]
NOISE_SIGMA_SALES: float = 0.04
NOISE_CLIP_LOWER: float = 0.88
NOISE_CLIP_UPPER: float = 1.12
NOISE_SIGMA_LABOR: float = 0.02
NOISE_SIGMA_TICKET: float = 0.06
NOISE_SIGMA_UNITS: float = 0.05

# ---------------------------------------------------------------------------
# YoY Growth (§4.8)
# ---------------------------------------------------------------------------

YOY_GROWTH_RATE: float = 0.025          # 2.5% annual compounding
YOY_BASE_DATE: date = date(2023, 1, 1)  # reference anchor (factor ≈ 1.00)

# ---------------------------------------------------------------------------
# Store profile slugs
# ---------------------------------------------------------------------------

PROFILE_SUBURBAN = "suburban-family"
PROFILE_URBAN = "urban-dense"
PROFILE_VALUE = "value-market"

# ---------------------------------------------------------------------------
# Store definitions (§2.1)
# Each entry matches seed_data/store_locations.json; kept here for in-memory use.
# ---------------------------------------------------------------------------

STORES: list[dict] = [
    {
        "store_id": 1,
        "store_name": "Knot Shore \u2014 Kirkwood",
        "address": "10250 Manchester Rd",
        "city": "Kirkwood",
        "zip": "63122",
        "county_fips": "29189",
        "trade_area_profile": PROFILE_SUBURBAN,
        "sqft": 45_000,
        "base_daily_revenue": 95_000.00,
        "open_date": "2009-04-15",
    },
    {
        "store_id": 2,
        "store_name": "Knot Shore \u2014 Chesterfield",
        "address": "18180 Chesterfield Airport Rd",
        "city": "Chesterfield",
        "zip": "63017",
        "county_fips": "29189",
        "trade_area_profile": PROFILE_SUBURBAN,
        "sqft": 52_000,
        "base_daily_revenue": 110_000.00,
        "open_date": "2011-08-22",
    },
    {
        "store_id": 3,
        "store_name": "Knot Shore \u2014 Oakville",
        "address": "5740 Telegraph Rd",
        "city": "Oakville",
        "zip": "63129",
        "county_fips": "29189",
        "trade_area_profile": PROFILE_SUBURBAN,
        "sqft": 42_000,
        "base_daily_revenue": 85_000.00,
        "open_date": "2013-03-10",
    },
    {
        "store_id": 4,
        "store_name": "Knot Shore \u2014 Central West End",
        "address": "4340 Lindell Blvd",
        "city": "St. Louis",
        "zip": "63108",
        "county_fips": "29510",
        "trade_area_profile": PROFILE_URBAN,
        "sqft": 28_000,
        "base_daily_revenue": 68_000.00,
        "open_date": "2007-11-05",
    },
    {
        "store_id": 5,
        "store_name": "Knot Shore \u2014 Soulard",
        "address": "1701 S 7th St",
        "city": "St. Louis",
        "zip": "63104",
        "county_fips": "29510",
        "trade_area_profile": PROFILE_URBAN,
        "sqft": 25_000,
        "base_daily_revenue": 58_000.00,
        "open_date": "2010-06-18",
    },
    {
        "store_id": 6,
        "store_name": "Knot Shore \u2014 Tower Grove",
        "address": "3200 S Grand Blvd",
        "city": "St. Louis",
        "zip": "63110",
        "county_fips": "29510",
        "trade_area_profile": PROFILE_URBAN,
        "sqft": 26_000,
        "base_daily_revenue": 62_000.00,
        "open_date": "2012-09-28",
    },
    {
        "store_id": 7,
        "store_name": "Knot Shore \u2014 North County",
        "address": "8200 W Florissant Ave",
        "city": "Jennings",
        "zip": "63136",
        "county_fips": "29189",
        "trade_area_profile": PROFILE_VALUE,
        "sqft": 35_000,
        "base_daily_revenue": 55_000.00,
        "open_date": "2015-02-14",
    },
    {
        "store_id": 8,
        "store_name": "Knot Shore \u2014 South City",
        "address": "4140 Gravois Ave",
        "city": "St. Louis",
        "zip": "63116",
        "county_fips": "29510",
        "trade_area_profile": PROFILE_VALUE,
        "sqft": 32_000,
        "base_daily_revenue": 52_000.00,
        "open_date": "2016-07-01",
    },
]

# ---------------------------------------------------------------------------
# Department definitions (§2.2)
# seasonal_profile drives the monthly multiplier table used in §4.2.
# ---------------------------------------------------------------------------

SEASONAL_SUMMER_PEAK = "summer-peak"
SEASONAL_WINTER_PEAK = "winter-peak"
SEASONAL_HOLIDAY_SPIKE = "holiday-spike"
SEASONAL_STABLE = "stable"

DEPARTMENTS: list[dict] = [
    {"department_id": 1,  "department_name": "Produce",                "is_perishable": True,  "seasonal_profile": SEASONAL_SUMMER_PEAK,   "base_margin_pct": 0.48},
    {"department_id": 2,  "department_name": "Meat & Seafood",         "is_perishable": True,  "seasonal_profile": SEASONAL_HOLIDAY_SPIKE, "base_margin_pct": 0.34},
    {"department_id": 3,  "department_name": "Dairy & Eggs",           "is_perishable": True,  "seasonal_profile": SEASONAL_STABLE,        "base_margin_pct": 0.32},
    {"department_id": 4,  "department_name": "Bakery",                 "is_perishable": True,  "seasonal_profile": SEASONAL_HOLIDAY_SPIKE, "base_margin_pct": 0.55},
    {"department_id": 5,  "department_name": "Deli & Prepared",        "is_perishable": True,  "seasonal_profile": SEASONAL_STABLE,        "base_margin_pct": 0.42},
    {"department_id": 6,  "department_name": "Frozen",                 "is_perishable": False, "seasonal_profile": SEASONAL_WINTER_PEAK,   "base_margin_pct": 0.38},
    {"department_id": 7,  "department_name": "Grocery (Center Store)", "is_perishable": False, "seasonal_profile": SEASONAL_HOLIDAY_SPIKE, "base_margin_pct": 0.27},
    {"department_id": 8,  "department_name": "Beverages",              "is_perishable": False, "seasonal_profile": SEASONAL_SUMMER_PEAK,   "base_margin_pct": 0.35},
    {"department_id": 9,  "department_name": "Snacks & Candy",         "is_perishable": False, "seasonal_profile": SEASONAL_SUMMER_PEAK,   "base_margin_pct": 0.40},
    {"department_id": 10, "department_name": "Health/Beauty/Household","is_perishable": False, "seasonal_profile": SEASONAL_STABLE,        "base_margin_pct": 0.42},
]

# ---------------------------------------------------------------------------
# Department revenue share by store profile (§2.3)
# Keys: department_name → share fraction (must sum to 1.00 per profile)
# ---------------------------------------------------------------------------

DEPT_SHARE: dict[str, dict[str, float]] = {
    PROFILE_SUBURBAN: {
        "Produce":                0.12,
        "Meat & Seafood":         0.14,
        "Dairy & Eggs":           0.10,
        "Bakery":                 0.04,
        "Deli & Prepared":        0.06,
        "Frozen":                 0.07,
        "Grocery (Center Store)": 0.25,
        "Beverages":              0.09,
        "Snacks & Candy":         0.05,
        "Health/Beauty/Household":0.08,
    },
    PROFILE_URBAN: {
        "Produce":                0.11,
        "Meat & Seafood":         0.12,
        "Dairy & Eggs":           0.09,
        "Bakery":                 0.05,
        "Deli & Prepared":        0.10,
        "Frozen":                 0.06,
        "Grocery (Center Store)": 0.22,
        "Beverages":              0.10,
        "Snacks & Candy":         0.06,
        "Health/Beauty/Household":0.09,
    },
    PROFILE_VALUE: {
        "Produce":                0.10,
        "Meat & Seafood":         0.13,
        "Dairy & Eggs":           0.11,
        "Bakery":                 0.03,
        "Deli & Prepared":        0.05,
        "Frozen":                 0.08,
        "Grocery (Center Store)": 0.29,
        "Beverages":              0.09,
        "Snacks & Candy":         0.05,
        "Health/Beauty/Household":0.07,
    },
}

# ---------------------------------------------------------------------------
# Seasonal factor tables (§4.2)
# Index 0 = January, index 11 = December
# ---------------------------------------------------------------------------

SEASONAL_FACTORS: dict[str, list[float]] = {
    SEASONAL_SUMMER_PEAK:  [0.90, 0.90, 0.95, 1.00, 1.08, 1.15, 1.18, 1.15, 1.05, 0.95, 0.88, 0.90],
    SEASONAL_WINTER_PEAK:  [1.05, 1.02, 0.98, 0.95, 0.92, 0.88, 0.85, 0.88, 0.95, 1.00, 1.08, 1.12],
    SEASONAL_HOLIDAY_SPIKE:[0.92, 0.90, 0.95, 1.00, 1.00, 0.95, 0.95, 0.95, 1.00, 1.02, 1.18, 1.30],
    SEASONAL_STABLE:       [0.97, 0.96, 0.98, 1.00, 1.01, 1.02, 1.01, 1.00, 0.99, 1.00, 1.02, 1.04],
}

# ---------------------------------------------------------------------------
# Day-of-week factor tables (§4.3)
# Index 0 = Monday (dow_num 1) … Index 6 = Sunday (dow_num 7)
# Access: DOW_FACTORS[profile][day_of_week_num - 1]
# ---------------------------------------------------------------------------

DOW_FACTORS: dict[str, list[float]] = {
    PROFILE_SUBURBAN: [0.85, 0.88, 0.92, 0.95, 1.10, 1.15, 1.15],
    PROFILE_URBAN:    [0.92, 0.93, 0.95, 0.97, 1.05, 1.08, 1.10],
    PROFILE_VALUE:    [0.82, 0.85, 0.90, 0.93, 1.12, 1.18, 1.20],
}

# ---------------------------------------------------------------------------
# SNAP window factors (§4.4)
# Applied when day-of-month is 1–10 (is_snap_window = True)
# ---------------------------------------------------------------------------

SNAP_FACTORS: dict[str, float] = {
    PROFILE_SUBURBAN: 1.02,
    PROFILE_URBAN:    1.04,
    PROFILE_VALUE:    1.10,
}
SNAP_FACTOR_OFF: float = 1.00  # factor when outside SNAP window

# ---------------------------------------------------------------------------
# Labor cost percentages by store profile (§4.6)
# Base fraction of net_sales; adjusted annually for wage inflation.
# ---------------------------------------------------------------------------

LABOR_PCT: dict[str, float] = {
    PROFILE_SUBURBAN: 0.105,
    PROFILE_URBAN:    0.115,
    PROFILE_VALUE:    0.120,
}

# Annual labor cost drift (wage inflation): +1.5% compounded per year from 2023
LABOR_WAGE_DRIFT: float = 0.015

# ---------------------------------------------------------------------------
# Average ticket base by store profile (§4.7)
# ---------------------------------------------------------------------------

AVG_TICKET_BASE: dict[str, float] = {
    PROFILE_SUBURBAN: 38.00,
    PROFILE_URBAN:    28.00,
    PROFILE_VALUE:    32.00,
}

# Items per transaction by department name (§4.7)
ITEMS_PER_TRANSACTION: dict[str, float] = {
    "Produce":                3.2,
    "Meat & Seafood":         1.8,
    "Dairy & Eggs":           2.5,
    "Bakery":                 1.6,
    "Deli & Prepared":        1.4,
    "Frozen":                 2.0,
    "Grocery (Center Store)": 4.5,
    "Beverages":              2.2,
    "Snacks & Candy":         2.0,
    "Health/Beauty/Household":1.7,
}

# ---------------------------------------------------------------------------
# Promotion generation parameters (§3.3)
# ---------------------------------------------------------------------------

PROMO_TYPES: list[str] = ["pct_off", "bogo", "bundle", "loss_leader"]

# discount_pct and lift_factor ranges by promo_type: (min, max)
PROMO_DISCOUNT_RANGE: dict[str, tuple[float, float]] = {
    "pct_off":     (0.10, 0.25),
    "bogo":        (0.40, 0.50),
    "bundle":      (0.08, 0.15),
    "loss_leader": (0.25, 0.35),
}
PROMO_LIFT_RANGE: dict[str, tuple[float, float]] = {
    "pct_off":     (1.10, 1.20),
    "bogo":        (1.25, 1.40),
    "bundle":      (1.08, 1.15),
    "loss_leader": (1.30, 1.50),
}

# Promo duration bounds (days, inclusive)
PROMO_DURATION_MIN: int = 3
PROMO_DURATION_MAX: int = 10

# Seasonal promo calendar: month → list of preferred department names
# Used to weight random promo selection; months are 1-indexed.
SEASONAL_PROMO_MAP: dict[int, list[str]] = {
    1:  ["Produce", "Health/Beauty/Household"],
    2:  ["Bakery", "Snacks & Candy", "Beverages"],
    3:  ["Meat & Seafood", "Bakery", "Dairy & Eggs"],
    4:  ["Meat & Seafood", "Bakery", "Dairy & Eggs"],
    5:  ["Meat & Seafood", "Beverages", "Snacks & Candy"],
    6:  ["Beverages", "Frozen", "Produce", "Snacks & Candy"],
    7:  ["Beverages", "Frozen", "Produce", "Snacks & Candy"],
    8:  ["Beverages", "Frozen", "Produce", "Snacks & Candy"],
    9:  ["Grocery (Center Store)", "Snacks & Candy"],
    10: ["Snacks & Candy", "Bakery"],
    11: ["Meat & Seafood", "Produce", "Dairy & Eggs", "Bakery", "Grocery (Center Store)"],
    12: ["Meat & Seafood", "Produce", "Dairy & Eggs", "Bakery", "Grocery (Center Store)",
         "Beverages", "Frozen", "Snacks & Candy", "Deli & Prepared", "Health/Beauty/Household"],
}

# Target promotions per month (used during schedule generation)
PROMOS_PER_MONTH_MIN: int = 6
PROMOS_PER_MONTH_MAX: int = 10

# ---------------------------------------------------------------------------
# Anomaly injection parameters (§6)
# ---------------------------------------------------------------------------

ANOMALY_PROBABILITY: float = 0.05          # 5% per store per date
ANOMALY_INTEGRITY_BREACH_WEIGHT: float = 0.40
ANOMALY_MISSING_DEPT_WEIGHT: float = 0.30
ANOMALY_MARGIN_OUTLIER_WEIGHT: float = 0.20
ANOMALY_DUPLICATE_ROW_WEIGHT: float = 0.10
ANOMALY_INTEGRITY_OFFSET_MIN: float = 50.0
ANOMALY_INTEGRITY_OFFSET_MAX: float = 200.0

# ---------------------------------------------------------------------------
# Realism engine — series keys and guard-rail clamps (§5)
# ---------------------------------------------------------------------------

# FRED series
SERIES_PCE_NOMINAL = "PCE_NOMINAL"
SERIES_PCE_REAL = "PCE_REAL"
SERIES_RETAIL_SALES = "RETAIL_SALES"
SERIES_SENTIMENT = "SENTIMENT"
SERIES_CPI_ALL = "CPI_ALL"
SERIES_GDP_REAL = "GDP_REAL"
SERIES_UNRATE = "UNRATE"
SERIES_SAVINGS_RATE = "SAVINGS_RATE"
SERIES_MONEY_COST = "MONEY_COST"
SERIES_GROCERY_SALES_MO = "GROCERY_SALES_MO"

# BLS series
SERIES_CPI_URBAN = "CPI_URBAN"
SERIES_CPI_CORE = "CPI_CORE"
SERIES_GAS_PRICE = "GAS_PRICE"
SERIES_AVG_WAGES = "AVG_WAGES"
SERIES_WAGE_INDEX = "WAGE_INDEX"

# ERS series
SERIES_ERS_ALL_FOOD = "ERS_ALL_FOOD"
SERIES_ERS_FOOD_HOME = "ERS_FOOD_HOME"
SERIES_ERS_FOOD_AWAY = "ERS_FOOD_AWAY"
SERIES_ERS_CEREALS = "ERS_CEREALS"
SERIES_ERS_MEATS = "ERS_MEATS"
SERIES_ERS_DAIRY = "ERS_DAIRY"
SERIES_ERS_FRUITS_VEG = "ERS_FRUITS_VEG"
SERIES_ERS_BEVERAGES = "ERS_BEVERAGES"

# Realism engine: ERS category → department mapping (§5.3)
ERS_DEPT_MAP: dict[str, str] = {
    "Produce":                SERIES_ERS_FRUITS_VEG,
    "Meat & Seafood":         SERIES_ERS_MEATS,
    "Dairy & Eggs":           SERIES_ERS_DAIRY,
    "Bakery":                 SERIES_ERS_CEREALS,
    "Grocery (Center Store)": SERIES_ERS_CEREALS,
    "Beverages":              SERIES_ERS_BEVERAGES,
    "Deli & Prepared":        SERIES_ERS_FOOD_AWAY,
    # Frozen, Snacks & Candy, Health/Beauty/Household → ERS_ALL_FOOD (fallback)
}

# Realism coefficients (§5.3)
REALISM_CPI_FOOD_COEFF: float = -0.04
REALISM_SENTIMENT_COEFF: float = 0.06
REALISM_UNEMP_COEFF: float = -0.05
REALISM_MARGIN_COEFF: float = -0.20  # additive fraction of (ratio - 1)
REALISM_WAGES_COEFF: float = 0.50

# Guard-rail clamps (§5.6)
REALISM_SALES_CLAMP: tuple[float, float] = (0.90, 1.10)
REALISM_MARGIN_MIN: float = 0.05
REALISM_MARGIN_MAX: float = 0.70
REALISM_LABOR_CLAMP: tuple[float, float] = (0.90, 1.15)
