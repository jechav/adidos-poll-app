"""Shared `structlog` processor pipeline (I-018).

Both the API process (`src.api.app`, I-003) and the worker process
(`src.worker.main`, I-008) call `configure_logging()` at startup, so log
line shape is identical single-line JSON regardless of which process
emitted it — 12-factor style to stdout, consumable by ELK/CloudWatch
without transformation (see `docs/architecture/logging.md`).

`merge_contextvars` is what makes request-scoped correlation work: the
API's request-ID middleware and the worker's dequeue loop both call
`structlog.contextvars.bind_contextvars(...)`, and every log call made
afterward on that task/coroutine automatically carries those fields
without having to pass them explicitly at each call site.
"""

import logging

import structlog


def configure_logging(level: int = logging.INFO) -> None:
    """Safe to call from multiple entrypoints (API startup, worker
    startup, test setup) — `structlog.configure` simply replaces the
    prior global configuration each time, so repeated calls with the
    same processors/level are harmless.
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
