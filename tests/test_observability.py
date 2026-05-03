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

    def test_stdlib_extra_kwarg_fields_appear_in_output(self):
        """Verify the stdlib bridge propagates extra={} fields to the renderer.

        Without structlog.stdlib.ExtraAdder() in the shared processor chain,
        ProcessorFormatter silently drops attributes injected via extra={};
        the rendered output omits them. This test guards against that
        regression.
        """
        import logging as stdlib_logging

        with patch.dict(os.environ, {"LOG_FORMAT": "json"}, clear=False):
            buf = io.StringIO()
            with redirect_stdout(buf):
                # configure_logging() must run inside redirect_stdout so the
                # stdlib bridge's StreamHandler binds to buf rather than the
                # real sys.stdout (the handler captures the stream reference
                # at __init__ time, unlike structlog's PrintLogger which
                # resolves sys.stdout lazily).
                configure_logging()
                stdlib_logging.info(
                    "extra_kwarg_test",
                    extra={"series_id": "PCEC", "status": "updated", "row_count": 42},
                )
            line = buf.getvalue().strip().split("\n")[-1]
            assert line, "expected at least one line of json output"
            payload = json.loads(line)
            assert payload["event"] == "extra_kwarg_test"
            assert payload["series_id"] == "PCEC"
            assert payload["status"] == "updated"
            assert payload["row_count"] == 42
            assert payload["level"] == "info"
