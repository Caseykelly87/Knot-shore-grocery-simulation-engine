# Knot Shore Grocery — Simulation Engine

Synthetic data generator for Knot Shore Grocery, a fictional 8-store grocery chain in the St. Louis metropolitan area. Produces deterministic store and department-level retail data with injected anomalies and ground-truth labels. The output is the upstream source for a four-repo analytics platform — an ETL pipeline ingests it, an API serves the result as JSON, and a Next.js portal renders stakeholder dashboards.

## Table of contents

- [What it does](#what-it-does)
- [Quick start](#quick-start)
- [Commands](#commands)
- [Three-stage pipeline](#three-stage-pipeline)
- [Determinism](#determinism)
- [Anomaly injection](#anomaly-injection)
- [Output structure](#output-structure)
- [Realism layer (Stage 2)](#realism-layer-stage-2)
- [Logging](#logging)
- [Testing](#testing)
- [Where this fits in the platform](#where-this-fits-in-the-platform)

## What it does

The engine generates synthetic daily operational data for 8 stores: per-store summaries (revenue, transactions, average basket, labor cost percentage) and per-store-department detail (net sales, transactions, units sold, gross margin percentage). Output is a tree of CSV files under `output/daily/{MM}/{DD}/{YYYY}/` that downstream pipeline stages ingest.

Each generated date is seeded deterministically — the same seed and date produces byte-identical output across runs, machines, and operating systems. The platform's paired-year canonical fixtures depend on this property.

## Quick start

```bash
# Install
pip install -e .

# For Stage 2 realism layer (requires Postgres driver)
pip install -e ".[realism]"

# For development / testing
pip install -e ".[dev]"

# Initialize (run once per fresh output directory)
python -m knot_shore init --seed 42 --output ./output

# Generate daily data — anchor is today, generates today + 6 trailing days + same date one year prior
python -m knot_shore run --seed 42 --output ./output

# Or backfill a contiguous historical window
python -m knot_shore backfill --start-date 2025-07-01 --days 184 --output ./output
```

After `init`, output contains dimension tables (stores, departments, calendar) and a four-year promotion schedule. Subsequent `run` or `backfill` invocations populate the daily data tree.

Python 3.11+ is required. Runtime dependencies: `faker>=19`, `numpy>=1.26`, `pandas>=2.1`, `structlog>=24`. The realism extras add `sqlalchemy>=2` and `psycopg2-binary>=2.9`.

## Commands

The CLI has four commands. Run any of them with `--help` for full argument details.

### `init`

Generates dimension tables (`dim_stores.csv`, `dim_departments.csv`, `dim_calendar.csv`) and the full four-year promotion schedule. Idempotent — files that already exist are skipped. Run once per fresh output directory.

```bash
python -m knot_shore init --seed 42 --output ./output
```

### `run`

Generates daily data for 8 dates: an anchor date (defaults to today; override with `--date`), the 6 preceding days, and the same calendar date one year prior. Running daily fills in the trailing seven-day window and keeps the year-ago comparison current; date directories that already exist are skipped, so re-running is safe.

```bash
# Anchor = today
python -m knot_shore run --seed 42 --output ./output

# Specific anchor
python -m knot_shore run --date 2025-12-31 --seed 42 --output ./output

# Force-disable Stage 2 even if KNOT_SHORE_DB_URL is set
python -m knot_shore run --seed 42 --output ./output --no-realism
```

The `run` command also generates per-store reports under `output/reports/YYYY-MM-DD/` for the anchor date only.

### `backfill`

Generates a contiguous historical date range in a single invocation. No T-365 paired generation, no per-store reports — backfill is for filling explicit windows.

```bash
# Default — generates 2025-07-01 through 2025-12-31 (184 days, the canonical demo window)
python -m knot_shore backfill --output ./output

# Override start date — extends forward by --days
python -m knot_shore backfill --start-date 2024-07-01 --days 184 --output ./output

# Override end date — extends backward by --days
python -m knot_shore backfill --end-date 2025-09-30 --days 184 --output ./output

# Disable Stage 2 for the whole window
python -m knot_shore backfill --output ./output --no-realism
```

`--start-date` and `--end-date` are mutually exclusive. Both default to the canonical window if neither is provided. The `--days` argument controls the window length.

### `reports`

(Re-)generates the per-store report files for a specific date. Requires that daily CSV data already exists for that date.

```bash
python -m knot_shore reports --date 2025-12-31 --output ./output
```

## Three-stage pipeline

Each generated date passes through three stages in order:

```
Stage 1: BASE GENERATION    →   Stage 2: REALISM LAYER       →   Stage 3: ANOMALY INJECTION → OUTPUT
Economically-blind waterfall    Economic multipliers from        Inject up to 4 types of
using seasonality, day-of-week, real FRED/BLS economic data      data integrity anomalies,
promotions, and noise.          (skipped when DB unavailable     write CSVs and ground-truth
                                or --no-realism is passed).      anomaly_log.csv.
```

Stage 2 is optional. If `KNOT_SHORE_DB_URL` is unset or the connection fails, the engine logs a skip message and Stage 2 is bypassed. Base data is written to disk as-is.

Stage 3 always runs. The `anomaly_log.csv` file is written for every date, even when no anomalies are injected (in which case the file contains only its header row).

## Determinism

The engine seeds each generated day with a deterministic function of the global seed and the target date:

```python
date_seed = global_seed + target_date.toordinal()
rng = np.random.default_rng(date_seed)
```

Implications:

- A given date's output depends only on `(global_seed, date)`, not on what came before in the run, not on the order of the date list, not on whether realism was enabled for an earlier date.
- Regenerating any single date in isolation produces the same data as generating it as part of a larger backfill.
- The platform's paired-year canonical fixtures rely on this: a 2024-07-01 file produced by `backfill --start-date 2024-07-01` is byte-identical to a 2024-07-01 file produced by `run --date 2025-07-01` (which generates the t-365 paired data).

A test in `tests/test_deterministic_seed.py` asserts byte-identity across two successive runs of the same seed. This is the single most important property the test suite verifies.

## Anomaly injection

On each generated date, per store there is a 5% probability of injecting one anomaly. Only one anomaly type is injected per store-date when fired. Anomaly types and their relative weights:

| Type | Weight | Description |
|---|---:|---|
| `integrity_breach` | 40% | `net_sales` is forced to differ from `gross_sales − discount_amount` by a small offset. The summary row's totals are recomputed from departments, so this surfaces as a header-vs-detail mismatch. |
| `missing_department` | 30% | One department row is removed from the daily output, leaving the summary row referring to a department that doesn't appear in the detail. |
| `margin_outlier` | 20% | One department's `gross_margin_pct` is set to an unrealistically high value (0.95, when cogs collapses to 5% of net sales) or a negative value (when cogs is inflated to 1.05–1.30× net sales). |
| `duplicate_row` | 10% | One department row is duplicated exactly, inflating that department's totals on that date. |

The ground-truth `anomaly_log.csv` records every injection: date, store_id, anomaly_type, and any per-type details (e.g., the affected department_id). The platform's downstream detection rules (in `economic-data-etl`) look for statistical anomalies — sales bands, transaction bands, year-over-year ratios — which is a different set of phenomena than what's injected here. The injection log is the platform's ground truth for evaluating detection quality, but no platform code reads it at runtime; only the upstream `economic-data-etl/scripts/evaluate_detection.py` reads it.

Seeding for injection uses the same per-date scheme as Stage 1 with an offset: `date_seed + 1_000_000`. Determinism holds.

## Output structure

```
output/
├── dimensions/
│   ├── dim_stores.csv          # 8 rows: store_id, store_name, address, city, etc.
│   ├── dim_departments.csv     # 10 rows: department_id, department_name
│   └── dim_calendar.csv
├── promotions/
│   └── promotions.csv          # 4-year promotion schedule
├── daily/
│   └── {MM}/                   # Month (01–12)
│       └── {DD}/               # Day of month (01–31)
│           └── {YYYY}/         # Year
│               ├── store_summary.csv       # 8 rows per file (one per store)
│               ├── department_sales.csv    # ~80 rows per file (8 stores × 10 departments)
│               └── anomaly_log.csv         # 0+ rows per file (always written)
├── reports/                    # Generated by `run` for the anchor date only
│   └── YYYY-MM-DD/
│       └── store_NNN_report.txt
└── manifest.json               # Run history; updated by every command
```

The date-tree layout (`{MM}/{DD}/{YYYY}/`) is the contract consumed by the upstream ETL repo's source adapter. The adapter walks this tree, validates each file's schema, and ingests the rows into canonical parquet artifacts.

## Realism layer (Stage 2)

When the environment variable `KNOT_SHORE_DB_URL` is set, Stage 2 connects to a Postgres database and applies multipliers derived from real macroeconomic data:

```bash
export KNOT_SHORE_DB_URL=postgresql://user:pass@host:5432/dbname
```

The expected database is the one populated by the upstream `economic-data-etl` repository's macro pipeline (FRED, BLS, ERS series). Stage 2 reads economic indicators that the macro pipeline has loaded — Consumer Price Index (food at home), University of Michigan Consumer Sentiment, unemployment rate, average wages, and per-category CPI series — and applies them as multipliers to the base data:

- `ERS_FOOD_HOME`, `SENTIMENT`, `UNRATE` → sales volume multiplier
- `ERS_*` per-category CPI → margin pressure per department
- `AVG_WAGES` → labor cost multiplier

If the connection fails or the variable is unset, the engine logs a skip and writes Stage 1 output as-is. The `--no-realism` flag forces Stage 2 to skip even when the database is reachable, useful for pure synthetic data without macro context.

The cross-repo dependency is real: running Stage 2 against a freshly-cloned platform requires the upstream ETL's macro pipeline to have run at least once and populated the database. For most demo and development workflows, `--no-realism` is the appropriate path — Stage 1's output alone is the canonical demo data downstream consumers see.

## Logging

The engine emits structured logs via [structlog](https://www.structlog.org/). Output is human-readable colored text when stdout is a tty, single-line JSON otherwise. Format and verbosity are controlled by environment variables:

| Variable | Values | Default |
|---|---|---|
| `LOG_LEVEL` | `debug`, `info`, `warning`, `error`, `critical` | `info` |
| `LOG_FORMAT` | `console`, `json` | auto (console if tty, else json) |

Console output:

```
2025-12-31T17:34:42.118Z [info     ] backfill_started               command=backfill target_date_count=184 start_date=2025-07-01 end_date=2025-12-31
```

JSON output:

```json
{"event": "backfill_started", "command": "backfill", "target_date_count": 184, "start_date": "2025-07-01", "end_date": "2025-12-31", "level": "info", "logger": "knot_shore.cli", "timestamp": "2025-12-31T17:34:42.118Z"}
```

To debug a failing run:

```bash
LOG_LEVEL=debug python -m knot_shore backfill --output ./output --no-realism
```

To capture structured logs for offline analysis:

```bash
LOG_FORMAT=json python -m knot_shore backfill --output ./output --no-realism > run.log
```

The structlog configurator lives in `src/knot_shore/observability.py`. It uses an stdlib bridge with `structlog.stdlib.ExtraAdder()` so calls like `logging.info("foo", extra={"k": "v"})` propagate the structured fields through to the rendered output.

## Testing

```bash
# Run all tests
python -m pytest

# Verbose
python -m pytest -v

# Coverage report
python -m pytest --cov=src/knot_shore --cov-report=term-missing
```

The test suite has 122 tests covering:

- **Determinism** — byte-identity across successive runs of the same seed (the single most important property).
- **Anomaly injection** — bounded rate (5% per store-day) verified against tolerance, ground-truth log integrity.
- **Pipeline contracts** — Stage 1 → Stage 2 → Stage 3 composition; the `--no-realism` opt-out path produces base data identical to what realism would have received.
- **CLI surface** — argument parsing, mutual exclusion, command dispatch.
- **Output integrity** — directory layout, file presence, summary-vs-detail reconciliation.
- **Date resolution** — `run` produces 8 dates (anchor + 6 trailing + t-365); `backfill` produces a contiguous range with no t-365.

No live network or database calls are made. The realism layer is exercised against test doubles. CI runs the full suite on every push.

## Where this fits in the platform

The simulation engine is the upstream end of a four-repo data platform:

```
knot-shore-grocery-simulation-engine    →    economic-data-etl    →    economic-data-api    →    knot-shore-portal
(this repo)                                  ingestion + detection      service layer              dashboards
                                                                                                    + docs hub
```

The engine produces CSV files under `output/daily/{MM}/{DD}/{YYYY}/`. The ETL repo reads that tree, validates schemas, transforms into canonical parquet artifacts, and applies static-band detection rules. The API serves the canonical artifacts as JSON. The portal consumes the API and renders three primary dashboards plus an architectural documentation hub.

Reader-grade documentation for the engine — determinism, anomaly injection, paired-year mechanics — lives at the portal's [`/about/sim-engine`](https://github.com/Caseykelly87/knot-shore-portal) page. The platform-wide architectural narrative is at [`/about/architecture`](https://github.com/Caseykelly87/knot-shore-portal); decision records are at [`/about/decisions`](https://github.com/Caseykelly87/knot-shore-portal).

Adjacent repositories:

- [`economic-data-etl`](https://github.com/Caseykelly87/economic-data-etl) — ingests this engine's output into canonical parquet artifacts; runs the macro-economic pipeline that populates the database Stage 2 reads from.
- [`economic-data-api`](https://github.com/Caseykelly87/economic-data-api) — serves the canonical artifacts as JSON via FastAPI.
- [`knot-shore-portal`](https://github.com/Caseykelly87/knot-shore-portal) — Next.js 14 application with three primary dashboards and the platform's `/about/*` documentation hub.
