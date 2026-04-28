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

Each run generates data for **8 dates**:

| Date | Description |
|------|-------------|
| Today (anchor) | Current calendar date |
| anchor − 1 day | Yesterday |
| anchor − 2 days | Two days ago |
| anchor − 3 days | Three days ago |
| anchor − 4 days | Four days ago |
| anchor − 5 days | Five days ago |
| anchor − 6 days | Six days ago |
| anchor − 1 year | Same calendar date, one year prior |

Running the engine daily fills in the trailing seven-day window as it advances,
while always keeping the one-year-ago comparison date current.  Folders that
already exist on disk are skipped automatically — re-running is safe.

For one-shot historical backfills, see [Historical Backfill](#historical-backfill) below.

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
# Anchor = today: generates today, the six preceding days, and the same date one year ago
python -m knot_shore run --seed 42 --output ./output

# Specific anchor date
python -m knot_shore run --date 2026-03-28 --seed 42 --output ./output

# Force-disable realism engine (even if DB_URL is set)
python -m knot_shore run --seed 42 --output ./output --no-realism
```

### 4. Historical Backfill

For demo, fixture, and portfolio purposes, the engine can generate a contiguous
range of historical dates in a single invocation. This is the mode used to
populate the canonical fixtures consumed by the downstream ETL → API → portal
pipeline.

The default canonical window is **2025-07-01 through 2025-12-31** — six months
ending on December 31, 2025. This window aligns with the period for which macro
economic data (FRED CPI, BLS unemployment, USDA ERS food retail) is reliably
available downstream. The canonical window may shift in future regenerations as
new economic data becomes available; today's default is fixed for reference.

```bash
# Default — generates 2025-07-02 through 2025-12-31 (183 days)
python -m knot_shore backfill --output ./output

# Override with end date — six months ending given date
python -m knot_shore backfill --end-date 2025-09-30 --output ./output

# Override with start date — six months starting given date
python -m knot_shore backfill --start-date 2025-07-01 --output ./output

# Custom window length (works with either start or end)
python -m knot_shore backfill --days 30 --end-date 2025-12-31 --output ./output
```

`--start-date` and `--end-date` are mutually exclusive — provide at most one.

Notes:

- Backfill does **not** generate store reports. Reports are designed to summarize
  a single anchor date; running them across hundreds of days would be wrong scope.
  Use `python -m knot_shore reports --date <YYYY-MM-DD>` separately if needed.
- Re-running is safe: any date folder that already exists on disk is skipped.
- This mode produces the canonical fixtures that flow downstream to the ETL,
  API, and portal repositories.

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
│           └── {YYYY}/
│               ├── department_sales.csv
│               ├── store_summary.csv
│               └── anomaly_log.csv
├── reports/
│   └── YYYY-MM-DD/
│       └── store_NNN_report.txt
└── manifest.json
```

This layout groups all years' data for the same calendar date together.
`daily/06/15/` holds year subdirectories side by side, making year-over-year
comparison browsing natural and aligning with how the ETL pipeline ingests
daily file drops.

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
├── date_resolver.py   # Resolves the eight target dates for a run
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
realism engine skip/clamp behavior, date resolver logic, and CLI parser surface.

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
