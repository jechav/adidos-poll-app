"""Standalone vote processor entrypoint (I-008).

Run as `python -m src.worker.main --shard-id=N`, one Kubernetes Deployment
per shard (per SPECIFICATION.md decision #9) — independent of the
FastAPI API process (`src.api.app`, from I-003), so it can be scaled,
restarted, and monitored on its own.

Every worker pod (regardless of which shard it's assigned) `BRPOP`s from
the *same* shared `queue:votes` list, then keeps only the votes that
route to its own shard and immediately pushes the rest back onto the
queue for whichever pod does own them. This is deliberate, not wasted
work: it means adding pods (for the same shard or different shards)
increases total dequeue throughput with no coordination beyond Redis's
own atomicity on `BRPOP` — two pods can never both pop the same list
entry, so votes are never double-processed no matter how many workers run
concurrently.
"""

import argparse
import asyncio
import logging

from src.cache.redis_client import get_redis
from src.config import settings
from src.worker.db import ShardConnectionPool
from src.worker.processor import process_batch, requeue
from src.worker.queue_consumer import dequeue_batch
from src.worker.sharding import route_by_shard

logger = logging.getLogger(__name__)


async def run_worker(
    shard_id: int,
    num_shards: int,
    *,
    redis=None,
    pool: ShardConnectionPool | None = None,
    iterations: int | None = None,
    min_batch_size: int = 100,
    max_batch_size: int = 500,
    batch_timeout_s: float = 2,
    poll_timeout_s: int = 1,
) -> None:
    """Run the drain loop. `iterations=None` runs forever (production);
    tests pass a finite `iterations` (and usually smaller batch/timeout
    settings) to exercise a bounded number of dequeue/route/process
    cycles without waiting out production-sized timeouts.
    """
    redis = redis if redis is not None else await get_redis()
    pool = pool if pool is not None else ShardConnectionPool()

    completed = 0
    while iterations is None or completed < iterations:
        completed += 1
        batch = await dequeue_batch(
            redis,
            min_size=min_batch_size,
            max_size=max_batch_size,
            timeout_s=batch_timeout_s,
            poll_timeout_s=poll_timeout_s,
        )
        if not batch:
            continue

        shard_batches = route_by_shard(batch, num_shards)
        for other_shard_id, votes in shard_batches.items():
            if other_shard_id == shard_id:
                continue
            await requeue(redis, votes)

        my_votes = shard_batches.get(shard_id, [])
        if my_votes:
            await process_batch(shard_id, my_votes, pool, redis)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m src.worker.main")
    parser.add_argument(
        "--shard-id", type=int, required=True, help="Shard this pod is responsible for"
    )
    parser.add_argument(
        "--num-shards",
        type=int,
        default=settings.num_shards,
        help="Total shard count (must match I-001's provisioning)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv)
    asyncio.run(run_worker(args.shard_id, args.num_shards))


if __name__ == "__main__":
    main()
