"""Tests for the structlog configurator."""

from __future__ import annotations

import io
import json
import logging
import os
from contextlib import redirect_stdout
from unittest.mock import patch

import pytest
import structlog

from knot_shore.observability import configure_logging


@pytest.fixture(autouse=True)
def reset_structlog():
    """Each test starts with structlog defaults so configuration changes don't leak."""
    structlog.reset_defaults()
    yield
    structlog.reset_defaults()


class TestConfigureLogging:
    def test_default_invocation_runs_without_error(self):
        configure_logging()

    def test_json_format_emits_parseable_json(self):
        # Force json mode regardless of stdout tty status
        with patch.dict(os.environ, {"LOG_FORMAT": "json"}, clear=False):
            configure_logging()
            buf = io.StringIO()
            with redirect_stdout(buf):
                logger = structlog.get_logger("test")
                logger.info("test_event", store_id=3, sales=1234.56)
            line = buf.getvalue().strip()
            assert line, "expected at least one line of json output"
            # Each line must be valid json
            payload = json.loads(line.split("\n")[-1])
            assert payload["event"] == "test_event"
            assert payload["store_id"] == 3
            assert payload["sales"] == 1234.56
            assert "timestamp" in payload
            assert payload["level"] == "info"

    def test_log_level_env_var_filters_below_threshold(self):
        with patch.dict(os.environ, {"LOG_LEVEL": "warning", "LOG_FORMAT": "json"}, clear=False):
            configure_logging()
            buf = io.StringIO()
            with redirect_stdout(buf):
                logger = structlog.get_logger("test")
                logger.info("info_message")     # filtered out
                logger.warning("warn_message")  # passes
            output = buf.getvalue()
            assert "info_message" not in output
            assert "warn_message" in output
