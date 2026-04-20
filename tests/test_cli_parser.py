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


class TestBackfillRemoved:
    def test_backfill_raises_system_exit(self):
        with pytest.raises(SystemExit) as exc_info:
            _build_parser().parse_args(["backfill"])
        assert exc_info.value.code != 0

    def test_backfill_error_message_mentions_invalid_choice(self, capsys):
        with pytest.raises(SystemExit):
            _build_parser().parse_args(["backfill"])
        captured = capsys.readouterr()
        assert "invalid choice" in captured.err
