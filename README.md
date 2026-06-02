# Knot Shore Grocery — Simulation Engine

[![CI](https://github.com/Caseykelly87/Knot-shore-grocery-simulation-engine/actions/workflows/test.yml/badge.svg)](https://github.com/Caseykelly87/Knot-shore-grocery-simulation-engine/actions/workflows/test.yml)

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

# For Stage 2 realism layer (Postgres driver)
pip install -e ".[realism]"

# For refreshing the bundled economic fixture from FRED and BLS
pip install -e ".[fixtures]"

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

Python 3.11+ is required. Runtime dependencies: `faker>=19`, `numpy>=1.26`, `pandas>=2.1`, `structlog>=24`, `pyarrow>=14` (the latter so the realism layer can read the bundled economic fixture out of the box). The realism extras add `sqlalchemy>=2` and `psycopg2-binary>=2.9` for the database path. The fixtures extra adds `requests>=2.31`, `python-dotenv`, and `truststore>=0.10` for the refresh script.

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

Stage 2 has a three-tier data-source precedence: database, then a bundled parquet fixture committed under [`seed_data/economic/`](seed_data/economic/), then skip. If the database is unavailable or cannot supply the realism-set series, the layer falls back to the bundled fixture so Stage 2 still runs. Stage 2 is only bypassed when neither source is present — a broken-install state.

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

The ground-truth `anomaly_log.csv` records every injection: date, store_id, anomaly_type, and any per-type details (e.g., the affected department_id). The platform's downstream detection rules (in `economic-data-etl`) look for anomalies across seven rules at two grains — store-day bands for revenue, labor percentage, average ticket, and transactions, a year-over-year ratio rule, a rolling 28-day z-score on revenue, and a department-grain `department_coverage` structural rule — which is a different set of phenomena than what's injected here. The injection log is the platform's ground truth for evaluating detection quality, but no platform code reads it at runtime; only the upstream `economic-data-etl/scripts/evaluate_detection.py` reads it.

Seeding for injection uses the same per-date scheme as Stage 1 with an offset: `date_seed + RNG_OFFSET_ANOMALIES` (defined in `config.py`). Determinism holds.

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

The expected database is the one populated by the upstream `economic-data-etl` repository's macro pipeline (FRED and BLS series). Stage 2 reads economic indicators that the macro pipeline has loaded — Consumer Price Index (food at home), University of Michigan Consumer Sentiment, unemployment rate, average wages, and per-category food CPI series — and applies them as multipliers to the base data:

- `ERS_FOOD_HOME`, `SENTIMENT`, `UNRATE` → sales volume multiplier
- `ERS_*` per-category food CPI → margin pressure per department
- `AVG_WAGES` → labor cost multiplier

The `ERS_*` prefix on the food-category series names is historical, from when an earlier iteration of the platform pulled those categories from the USDA ERS Food Price Outlook. The data is now sourced from the underlying BLS monthly CPI indexes; the names are kept stable to avoid cascading renames through the realism layer and the test suite.

If the database is unset, unreachable, or reachable but missing series, Stage 2 falls back to the bundled parquet fixture for the whole run — see [Bundled economic fixture](#bundled-economic-fixture). The `--no-realism` flag forces Stage 2 to skip even when a source is available, useful for pure synthetic data without macro context.

The realism layer emits one `realism_source` event per run announcing the resolved source (`database`, `bundled_fixture`, or `none`). When the layer expected a database (`KNOT_SHORE_DB_URL` was set) but used the fixture instead, the event is at warning level so the fallback is visible in pipeline output.

### Bundled economic fixture

[`seed_data/economic/economic_observations.parquet`](seed_data/economic/) is the offline-mode data source. The realism layer reads it when no database is configured or when the configured database cannot supply the realism-set series. The accompanying [`metadata.json`](seed_data/economic/metadata.json) carries the fixture's provenance:

- **Last updated:** the `last_updated` field in `metadata.json` — the placeholder ships with `1970-01-01T00:00:00Z` and `is_placeholder: true`; the refresh script rewrites both fields with the current timestamp.

The fixture committed initially is a synthetic placeholder (round-number values, monthly cadence from 2023-01 through 2024-06) so the realism layer's offline path has data to read and the test suite has something to exercise. The first refresh against the live APIs replaces it with real 2023-present data.

Schema (mirrors the ETL pipeline's `raw.fact_economic_observations` table):

| column        | type     |
|---------------|----------|
| `series_id`   | string   |
| `series_name` | string   |
| `date`        | date     |
| `value`       | float64  |
| `source`      | string   |

The fixture covers all eleven series the realism layer queries: `SENTIMENT`, `UNRATE`, `AVG_WAGES`, `ERS_ALL_FOOD`, `ERS_FOOD_HOME`, `ERS_FOOD_AWAY`, `ERS_CEREALS`, `ERS_MEATS`, `ERS_DAIRY`, `ERS_FRUITS_VEG`, `ERS_BEVERAGES`.

#### Refreshing the fixture

[`scripts/refresh_economic_fixtures.py`](scripts/refresh_economic_fixtures.py) is a standalone maintenance script that fetches the realism-set series from FRED and BLS and rewrites the bundled parquet. It is not part of the engine's normal CLI; expected cadence is three to four times a year, since the underlying monthly series do not change more frequently than that.

```bash
pip install -e ".[fixtures]"
# Put FRED_API_KEY and BLS_API_KEY in the repo-root .env (see .env.example)
python scripts/refresh_economic_fixtures.py
```

The script reads `FRED_API_KEY` and `BLS_API_KEY` from the repo-root `.env` via `python-dotenv` (or from the process environment if already set). A failed fetch for any series is a hard error: the script aborts without writing rather than producing a fixture missing series.

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

The test suite has 142 tests covering:

- **Determinism** — byte-identity across successive runs of the same seed (the single most important property).
- **Anomaly injection** — the 5%-per-store-day rate verified against a binomial confidence interval over a large sample of independent trials, ground-truth log integrity.
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

The platform's deployed portal is at [https://knot-shore-portal.vercel.app](https://knot-shore-portal.vercel.app) (offline mode, bundled fixtures); the full-stack technical demo is the orchestration repo at [https://github.com/Caseykelly87/knot-shore-platform](https://github.com/Caseykelly87/knot-shore-platform).

Reader-grade documentation for the engine — determinism, anomaly injection, paired-year mechanics — lives at the portal's [`/about/sim-engine`](https://github.com/Caseykelly87/knot-shore-portal) page. The platform-wide architectural narrative is at [`/about/architecture`](https://github.com/Caseykelly87/knot-shore-portal); decision records are at [`/about/decisions`](https://github.com/Caseykelly87/knot-shore-portal).

Adjacent repositories:

- [`economic-data-etl`](https://github.com/Caseykelly87/economic-data-etl) — ingests this engine's output into canonical parquet artifacts; runs the macro-economic pipeline that populates the database Stage 2 reads from.
- [`economic-data-api`](https://github.com/Caseykelly87/economic-data-api) — serves the canonical artifacts as JSON via FastAPI.
- [`knot-shore-portal`](https://github.com/Caseykelly87/knot-shore-portal) — Next.js 14 application with three primary dashboards and the platform's `/about/*` documentation hub.
