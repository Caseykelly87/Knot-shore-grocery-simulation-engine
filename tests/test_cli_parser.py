"""Tests for the CLI argument parser surface."""

import pytest
from datetime import date

from knot_shore.cli import _build_parser


def _parse(args):
    return _build_parser().parse_args(args)


class TestRunSubcommand:
    def test_run_parses_seed(self):
        args = _parse(["run", "--seed", "99", "--output", "/tmp/x"])
        assert args.seed == 99

    def test_run_parses_output(self):
        args = _parse(["run", "--output", "/some/path"])
        from pathlib import Path
        assert args.output == Path("/some/path")

    def test_run_parses_date(self):
        args = _parse(["run", "--date", "2025-06-15"])
        assert args.date == date(2025, 6, 15)

    def test_run_parses_no_realism(self):
        args = _parse(["run", "--no-realism"])
        assert args.no_realism is True

    def test_run_no_realism_defaults_false(self):
        args = _parse(["run"])
        assert args.no_realism is False

    def test_run_date_defaults_none(self):
        args = _parse(["run"])
        assert args.date is None

    def test_run_seed_defaults_42(self):
        args = _parse(["run"])
        assert args.seed == 42


class TestBackfillSubcommand:
    def test_backfill_is_a_valid_command(self):
        args = _parse(["backfill"])
        assert args.command == "backfill"

    def test_backfill_seed_defaults_42(self):
        args = _parse(["backfill"])
        assert args.seed == 42

    def test_backfill_parses_seed(self):
        args = _parse(["backfill", "--seed", "99"])
        assert args.seed == 99

    def test_backfill_parses_output(self):
        from pathlib import Path
        args = _parse(["backfill", "--output", "/tmp/x"])
        assert args.output == Path("/tmp/x")

    def test_backfill_parses_start_date(self):
        args = _parse(["backfill", "--start-date", "2025-07-01"])
        assert args.start_date == date(2025, 7, 1)

    def test_backfill_parses_end_date(self):
        args = _parse(["backfill", "--end-date", "2025-09-30"])
        assert args.end_date == date(2025, 9, 30)

    def test_backfill_days_defaults_183(self):
        args = _parse(["backfill"])
        assert args.days == 183

    def test_backfill_parses_custom_days(self):
        args = _parse(["backfill", "--days", "30"])
        assert args.days == 30

    def test_backfill_parses_no_realism(self):
        args = _parse(["backfill", "--no-realism"])
        assert args.no_realism is True

    def test_backfill_no_realism_defaults_false(self):
        args = _parse(["backfill"])
        assert args.no_realism is False

    def test_backfill_start_and_end_mutually_exclusive(self):
        with pytest.raises(SystemExit) as exc_info:
            _build_parser().parse_args([
                "backfill",
                "--start-date", "2025-07-01",
                "--end-date", "2025-12-31",
            ])
        assert exc_info.value.code != 0
