# Knot Shore Grocery — Simulation Engine

A Python-based daily operational data generator for **Knot Shore Grocery**, a fictional
8-store grocery chain in the St. Louis, MO metropolitan area.

---

## Purpose

This engine is a **data utility**, not the portfolio project. It generates realistic mock
operational data that a separate Airflow-based ETL pipeline ingests, transforms, and joins
with real BLS/FRED/ERS economic indicators. The portfolio value lives in the ETL pipeline,
data modeling, and analysis dashboards.

---

## How It Works

Each run generates data for **4 dates**:

| Date | Description |
|------|-------------|
| Today | Current calendar date |
| −1 year | Same calendar date, 1 year prior |
| −2 years | Same calendar date, 2 years prior |
| −3 years | Same calendar date, 3 years prior |

Running daily for a full year produces a complete 4-year dataset.

### Three-Stage Pipeline

```
Stage 1: BASE GENERATION    →  Stage 2: REALISM ENGINE       →  Stage 3: OUTPUT
Economically-blind waterfall   Economic multipliers from         Write CSVs and
using seasonality, DOW,        real FRED/BLS/ERS data            store reports
promos, and noise              (optional — skipped if no DB)
```

---

## Quick Start

### 1. Install

```bash
pip install -e .
# For realism engine support (requires Postgres):
pip install -e ".[realism]"
# For development / testing:
pip install -e ".[dev]"
```

### 2. Initialize (run once)

Generates dimension tables and the full 4-year promotion schedule.

```bash
python -m knot_shore init --seed 42 --output ./output
```

### 3. Daily Run

```bash
# Today + 3 prior-year same dates
python -m knot_shore run --seed 42 --output ./output

# Specific date
python -m knot_shore run --date 2026-03-28 --seed 42 --output ./output

# Force-disable realism engine (even if DB_URL is set)
python -m knot_shore run --seed 42 --output ./output --no-realism
```

### 4. Backfill (catch up from mid-year start)

If the engine is started mid-year, backfill generates every calendar date
from January 1 through today, producing the 4-year window for each date.
Existing folders are skipped automatically — safe to interrupt and resume.

```bash
# Default: January 1 of this year through today
python -m knot_shore backfill --seed 42 --output ./output

# Custom range
python -m knot_shore backfill --from 2026-01-01 --to 2026-03-31 --seed 42 --output ./output
```

### 5. Reports Only

```bash
python -m knot_shore reports --date 2026-03-28 --output ./output
```

---

## Realism Engine

Set `KNOT_SHORE_DB_URL` to connect to the ETL pipeline's Postgres database:

```bash
export KNOT_SHORE_DB_URL=postgresql://user:pass@host:5432/dbname
```

If unset or the connection fails, Stage 2 is skipped and base data is written as-is.

---

## Output Structure

```
output/
├── dimensions/
│   ├── dim_stores.csv
│   ├── dim_departments.csv
│   └── dim_calendar.csv
├── promotions/
│   └── promotions.csv
├── daily/
│   └── {MM}/                   # Month (01–12)
│       └── {DD}/               # Day of month (01–31)
│           ├── 2023/           # All years for this calendar date side by side
│           ├── 2024/
│           ├── 2025/
│           └── 2026/
│               ├── department_sales.csv
│               ├── store_summary.csv
│               └── anomaly_log.csv
├── reports/
│   └── YYYY-MM-DD/
│       └── store_NNN_report.txt
└── manifest.json
```

This layout groups all years' data for the same calendar date together.
`daily/06/15/` holds 2023–2026 subdirectories side by side, making
year-over-year comparison browsing natural and aligning with how the
ETL pipeline ingests daily file drops.

---

## Project Structure

```
src/knot_shore/
├── config.py          # All constants: store profiles, department shares, multipliers
├── dimensions.py      # dim_stores, dim_departments, dim_calendar generators
├── promotions.py      # Full 4-year promotion schedule generator
├── factors.py         # Seasonal, DOW, SNAP, YoY factor lookups
├── sales_generator.py # Core waterfall — Stage 1
├── realism.py         # Realism engine — Stage 2 (optional)
├── anomalies.py       # Anomaly injection (post-realism, pre-output)
├── reports.py         # Plain-text store report generator
├── output.py          # CSV writer and manifest updater — Stage 3
└── cli.py             # Entry point: orchestrates Stage 1 → 2 → 3
```

---

## Tests

```bash
pytest
pytest --cov=knot_shore
```

Test suite covers waterfall integrity, summary/detail consistency, output ranges,
promo overlap, calendar correctness, anomaly behavior, deterministic seeding,
and realism engine skip/clamp behavior.

---

## Validation Benchmarks

| Metric | Expected Range |
|--------|---------------|
| Annual revenue per store | $18M–$42M |
| Daily revenue per store | $50K–$115K |
| Chain gross margin | 28–32% |
| Avg ticket | $25–$45 |
| Transactions per store/day | 1,200–3,500 |
| Labor cost as % of net sales | 10–13% |
| YoY comp sales growth | 1.5–4.0% |

---

## Stores

| # | Store | City | Profile |
|---|-------|------|---------|
| 1 | Knot Shore — Kirkwood | Kirkwood | Suburban-Family |
| 2 | Knot Shore — Chesterfield | Chesterfield | Suburban-Family |
| 3 | Knot Shore — Oakville | Oakville | Suburban-Family |
| 4 | Knot Shore — Central West End | St. Louis | Urban-Dense |
| 5 | Knot Shore — Soulard | St. Louis | Urban-Dense |
| 6 | Knot Shore — Tower Grove | St. Louis | Urban-Dense |
| 7 | Knot Shore — North County | Jennings | Value-Market |
| 8 | Knot Shore — South City | St. Louis | Value-Market |
