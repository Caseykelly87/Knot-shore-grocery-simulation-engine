# Sim Engine Testing Notes

Reference for how the simulation engine's test suite is structured and what
"a good test" means in this codebase. Written for engineers extending the
suite here and in the downstream repositories that consume this engine's
output.

## Established patterns

The suite uses plain `pytest` — function-style tests, module- and
session-scoped fixtures, `pytest.mark.parametrize` for input sweeps, and
`monkeypatch` for environment and module-attribute isolation. No custom
framework, no shared assertion helpers beyond what `pytest` and
`pandas.testing` provide.

Tests are graded into three categories:

- **Business-correctness** — asserts specific values that are computable
  from the inputs independently of the implementation. Example:
  `test_known_holidays` asserts that 2024-11-28 is flagged as Thanksgiving,
  a fact derivable from the calendar rule "fourth Thursday of November",
  not from re-running the generator.
- **Structural** — asserts shape (types, columns, row counts, value
  ranges) but not specific values. Useful as entry-level coverage; not
  sufficient for hot-path code.
- **Ceremony** — runs code but verifies nothing beyond "it did not raise".

Business-correctness is the bar for hot-path code. Three techniques recur:

- **Independently-derived expectations.** Assert against an external
  standard or a hand-computed value, not against what the code emits.
  `test_fiscal_week_matches_iso_week` checks ISO 8601 week numbers;
  `test_fiscal_period_follows_445_pattern` checks the documented 4-4-5
  cumulative boundaries.
- **Invariant cross-checks.** Re-derive a field from its inputs and
  compare. The waterfall and summary-vs-detail suites assert
  `net_sales == gross_sales - discount_amount`, `summary total == sum of
  department rows`, and so on, within a stated rounding tolerance.
- **Determinism by hashing.** Generate twice, compare bytes. See below.

## Hot-path tests

The load-bearing data guarantees and the tests that hold them:

- **Deterministic seeding** — `test_deterministic_seed.py`. Same
  `(seed, date)` produces identical `generate_day` output;
  `test_realism_seeding.py` covers the Stage 2 re-derivation path.
- **End-to-end determinism** — `test_deterministic_seed.py::
  test_engine_output_byte_identical_across_runs` (see next section).
- **Realism injection** — `test_realism_clamps.py` (guard-rail clamps
  hold), `test_realism_query_target.py` (DB query feeds real numbers into
  the multipliers; multipliers move off neutral between a baseline and a
  drift year), `test_realism_fixture_fallback.py` (three-tier source
  resolution).
- **Anomaly injection** — `test_anomaly_injection.py`. Each injector
  produces its specific data effect; the anomaly log carries the
  five-column schema; every logged row references a real store and
  department; at most one anomaly per store per date.
- **Calendar dimension** — `test_calendar.py`. Known dates carry known
  holiday flags, ISO/fiscal weeks, fiscal periods, quarters, and
  day-of-week names; the ISO-week-53 cap is verified explicitly.
- **Store profiles** — `test_store_locations_drift.py`. `config.STORES`
  and `seed_data/store_locations.json` are held field-by-field equal.
- **Output generation** — `test_summary_matches_detail.py`,
  `test_waterfall_integrity.py`, `test_output_manifest.py`,
  `test_reports.py`. Written files have the expected schema and content;
  store reports name the correct store and report the correct totals.

## Determinism verification

The portal's about pages claim the canonical dataset can be regenerated
byte-identically by anyone with the repo. That claim is verified by
`test_deterministic_seed.py::test_engine_output_byte_identical_across_runs`.

It runs a full `cmd_init` + `cmd_run` cycle into two separate output
directories with the same seed and anchor date, then SHA-256-compares
every generated CSV — dimension tables, the promotion schedule, and the
daily files. `manifest.json` is excluded deliberately: it records a
wall-clock `last_run` timestamp and is intentionally not byte-stable.

The test lives in `tests/` and so runs in CI as part of the standard
`pytest -q` invocation; no separate CI configuration is required.

## Test categories observed

Reconnaissance classified the 136 tests present at the start of this pass:

| Category             | Count |
|----------------------|-------|
| Business-correctness | 125   |
| Structural           | 9     |
| Ceremony             | 2     |
| Uncategorizable      | 0     |

The suite was already mostly business-correctness. The work in this pass
converted eight structural/ceremony tests covering hot-path code into
business-correctness tests and added two tests (the ISO-week-53 cap and
the end-to-end determinism check), bringing the suite to 138 tests.

## Known weak areas

Tests left as structural or ceremony, with the reason each was not
strengthened:

- `test_calendar.py::test_day_of_week_num_range` — structural (range
  check only). Day-of-week correctness is already covered by
  `test_weekend_flag` and `test_dow_name_matches_num`, which assert the
  actual semantics; the range check is redundant but harmless.
- `test_anomaly_injection.py::test_anomaly_log_columns` — structural, but
  it is a deliberate schema-contract guard ("the log always has the
  required columns, even with zero rows"). Acceptable as written.
- `test_observability.py::test_default_invocation_runs_without_error` —
  ceremony. It exercises logging configuration, which is not a data hot
  path. A candidate for either strengthening or removal in a later pass;
  not removed here.

No production bugs were discovered while strengthening the targeted
tests. The strengthened tests pass against the current engine.

## Notes for downstream repositories

The ETL, API, and portal repositories consume this engine's output and
should carry the same testing conventions:

- **Business-correctness means independently-derived expectations.** A
  test that asserts a transformed value should compute the expected value
  from the inputs by hand or from a spec, not capture whatever the code
  currently returns. A snapshot of current output is a regression guard,
  not a correctness test.
- **Determinism is verified by hashing, upstream.** Downstream repos can
  treat this engine's CSV output as a stable fixture: the same seed and
  date produce byte-identical files. Tests that depend on engine output
  can pin a seed and date rather than tolerating drift.
- **Exclude timestamped artifacts from byte comparisons.** `manifest.json`
  carries a wall-clock field. Any downstream byte-identical assertion must
  compare data files and exclude metadata that records run time.
- **The three-category vocabulary** — business-correctness, structural,
  ceremony — is the shared language for grading test strength across the
  platform. Hot-path code earns business-correctness tests; structural
  coverage is acceptable only for non-load-bearing surfaces.
