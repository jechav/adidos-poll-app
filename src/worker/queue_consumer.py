"""Batch dequeue from `queue:votes` (I-008), per spec decision #8.

`BRPOP` (blocking pop) is used instead of a tight polling loop so an idle
worker doesn't busy-wait Redis with empty reads, while still accumulating
100-500 votes per cycle before handing a batch off for processing —
trading a little latency for far fewer, larger DB transactions per shard.
"""

import time

from src.metrics.registry import QUEUE_LABEL_VOTES, set_queue_depth
from src.worker.models import VotePayload

QUEUE_KEY = "queue:votes"


async def dequeue_batch(
    redis,
    *,
    min_size: int = 100,
    max_size: int = 500,
    timeout_s: float = 2,
    poll_timeout_s: int = 1,
    queue_key: str = QUEUE_KEY,
) -> list[VotePayload]:
    """Accumulate up to `max_size` votes, blocking on `BRPOP` between reads.

    Stops early (before `timeout_s` elapses) once `max_size` is reached.
    Stops at `timeout_s` regardless of batch size — an idle/low-traffic
    queue must not block a worker forever waiting to reach `min_size`;
    `min_size` only suppresses returning a *tiny* batch early on an empty
    read, it is not a hard floor.
    """
    batch: list[VotePayload] = []
    deadline = time.monotonic() + timeout_s
    while len(batch) < max_size and time.monotonic() < deadline:
        item = await redis.brpop(queue_key, timeout=poll_timeout_s)
        if item is None:
            if len(batch) >= min_size:
                break
            continue
        _, raw = item
        batch.append(VotePayload.model_validate_json(raw))

    # One LLEN per drained batch (not per BRPOP) is enough to keep the
    # gauge fresh without adding a round trip to the hot per-item path.
    set_queue_depth(QUEUE_LABEL_VOTES, await redis.llen(queue_key))
    return batch
