"""Tests for cmd_backfill — the run-loop wrapper around _run_pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from knot_shore.cli import cmd_backfill, resolve_backfill_dates


class TestCmdBackfill:
    def test_resolves_dates_from_args(self, tmp_path: Path):
        """cmd_backfill calls resolve_backfill_dates with provided args."""
        with patch("knot_shore.cli._run_pipeline") as mock_pipeline, \
             patch("knot_shore.cli._require_init"), \
             patch("knot_shore.cli.load_promotions"), \
             patch("knot_shore.cli.update_manifest"), \
             patch("knot_shore.cli._check_realism", return_value=False):
            mock_pipeline.return_value = ([], [])
            cmd_backfill(
                seed=42,
                output_dir=tmp_path,
                start_date=date(2025, 7, 1),
                end_date=None,
                days=7,
                no_realism=True,
            )
            target_dates = mock_pipeline.call_args.kwargs["target_dates"]
            assert target_dates[0] == date(2025, 7, 1)
            assert target_dates[-1] == date(2025, 7, 7)
            assert len(target_dates) == 7

    def test_passes_no_reports_to_pipeline(self, tmp_path: Path):
        """Backfill must NOT generate store reports — generate_reports_for is None."""
        with patch("knot_shore.cli._run_pipeline") as mock_pipeline, \
             patch("knot_shore.cli._require_init"), \
             patch("knot_shore.cli.load_promotions"), \
             patch("knot_shore.cli.update_manifest"), \
             patch("knot_shore.cli._check_realism", return_value=False):
            mock_pipeline.return_value = ([], [])
            cmd_backfill(
                seed=42,
                output_dir=tmp_path,
                start_date=None,
                end_date=date(2025, 12, 31),
                days=7,
                no_realism=True,
            )
            assert mock_pipeline.call_args.kwargs["generate_reports_for"] is None

    def test_records_backfill_command_in_manifest(self, tmp_path: Path):
        """Manifest update is called with command='backfill'."""
        with patch("knot_shore.cli._run_pipeline", return_value=([], [])), \
             patch("knot_shore.cli._require_init"), \
             patch("knot_shore.cli.load_promotions"), \
             patch("knot_shore.cli.update_manifest") as mock_manifest, \
             patch("knot_shore.cli._check_realism", return_value=False):
            cmd_backfill(
                seed=42,
                output_dir=tmp_path,
                start_date=None,
                end_date=date(2025, 12, 31),
                days=7,
                no_realism=True,
            )
            assert mock_manifest.call_args.kwargs["command"] == "backfill"

    def test_no_realism_flag_propagates(self, tmp_path: Path):
        """--no-realism is passed through to _run_pipeline."""
        with patch("knot_shore.cli._run_pipeline") as mock_pipeline, \
             patch("knot_shore.cli._require_init"), \
             patch("knot_shore.cli.load_promotions"), \
             patch("knot_shore.cli.update_manifest"), \
             patch("knot_shore.cli._check_realism", return_value=False):
            mock_pipeline.return_value = ([], [])
            cmd_backfill(
                seed=42,
                output_dir=tmp_path,
                start_date=None,
                end_date=date(2025, 12, 31),
                days=7,
                no_realism=True,
            )
            assert mock_pipeline.call_args.kwargs["no_realism"] is True
