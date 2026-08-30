"""Deterministic vote-level log sampling (I-018, SPECIFICATION.md decision
#15).

Sampling decides whether a *successful* vote's entire lifecycle (API
accept, enqueue, worker dequeue, DB write) is logged at INFO — not
whether an individual log call fires independently at each stage.
Independent per-call random sampling would almost always produce broken
partial traces (API line sampled in, worker line sampled out, or vice
versa). `is_sampled` is therefore a pure function of `vote_id` alone, so
the API process and the worker process reach the identical decision for
the same vote without coordinating.

Errors and rejections are exempt from this entirely (see
`docs/architecture/logging.md`): this function is only consulted on the
success path.
"""

from uuid import UUID

_SAMPLE_RATE_DENOMINATOR = 1000


def is_sampled(vote_id: UUID) -> bool:
    """Return True for roughly 1-in-1000 `vote_id`s, deterministically."""
    return vote_id.int % _SAMPLE_RATE_DENOMINATOR == 0
