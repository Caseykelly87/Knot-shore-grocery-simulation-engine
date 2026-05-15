# Bundled economic fixture

`economic_observations.parquet` is the offline-mode data source for the
Stage 2 realism layer. When `KNOT_SHORE_DB_URL` is unset, unreachable,
or reachable but missing series, the realism layer falls back to this
file so the engine produces realism-adjusted output without an external
database.

The committed file shipped with this directory is a placeholder
populated with obviously synthetic values (round-number CPI-like
indices and an `is_placeholder=true` marker in `metadata.json`). It is
replaced with real 2023-present data by running
`scripts/refresh_economic_fixtures.py` with `FRED_API_KEY` and
`BLS_API_KEY` set.

## Schema

| column        | type     | notes                                              |
|---------------|----------|----------------------------------------------------|
| `series_id`   | string   | source-system identifier (e.g. `UMCSENT`)          |
| `series_name` | string   | realism-layer key (e.g. `SENTIMENT`)               |
| `date`        | date     | observation date (first of month for monthlies)    |
| `value`       | float64  | observation value                                  |
| `source`      | string   | `FRED` or `BLS`                                    |

Schema mirrors the ETL pipeline's `raw.fact_economic_observations`
table so the realism layer's fixture-reading path produces a DataFrame
identical in shape to its database-query path.

## Metadata

`metadata.json` carries the fixture's provenance: `last_updated`
timestamp (written by the refresh script), `is_placeholder` flag, and
the list of series the fixture covers.
