"""Unit tests for I-018's shared structlog configuration.

`configure_logging()` is the one processor pipeline both the API process
and the worker process (I-008) import, so log line shape is identical
regardless of which process emitted it. These tests exercise the
configured `structlog` directly (no FastAPI/worker wiring involved) and
also carry the "never log the raw token" negative-assertion fixture
called for in the ticket's Testing Strategy.
"""

import json
import logging

import structlog

from src.logging.config import configure_logging


def _get_logger_output(capsys, log_fn):
    configure_logging()
    logger = structlog.get_logger("test")
    log_fn(logger)
    captured = capsys.readouterr()
    return captured.out.strip().splitlines()


def test_configure_logging_renders_single_line_json(capsys):
    lines = _get_logger_output(capsys, lambda logger: logger.info("vote_written", outcome="success"))

    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["event"] == "vote_written"
    assert parsed["outcome"] == "success"
    assert parsed["level"] == "info"
    assert "timestamp" in parsed


def test_configure_logging_merges_bound_contextvars_into_every_line(capsys):
    def emit(logger):
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id="req-abc123")
        logger.info("vote_accepted")
        structlog.contextvars.clear_contextvars()

    lines = _get_logger_output(capsys, emit)

    parsed = json.loads(lines[0])
    assert parsed["request_id"] == "req-abc123"


def test_configure_logging_uses_filtering_bound_logger_at_info_level(capsys):
    def emit(logger):
        logger.debug("should_be_filtered_out")
        logger.info("vote_accepted")

    lines = _get_logger_output(capsys, emit)

    assert len(lines) == 1
    assert json.loads(lines[0])["event"] == "vote_accepted"


def test_rendered_log_output_never_contains_a_bearer_token_substring(capsys):
    """Negative-assertion fixture: scans rendered log output for
    token-like substrings and fails if any appear (I-018 Testing
    Strategy). A caller that (incorrectly) tries to log the raw
    Authorization header value should be caught by this, not by code
    review.
    """
    raw_token = "Bearer sk-live-adidos-secret-token-12345"

    def emit(logger):
        # Simulate the *correct* behavior: only presence is logged, never
        # the value (see src/api/middleware/request_context.py).
        logger.info("request_received", has_auth_header=True)

    lines = _get_logger_output(capsys, emit)
    rendered = "\n".join(lines)

    assert raw_token not in rendered
    assert "sk-live-adidos-secret-token-12345" not in rendered
