"""Shard routing (I-008), matching I-001/SPECIFICATION.md decision #7's
`user_id % num_shards` rule.

`user_id` is a free-form string (VARCHAR(255) on `votes`, per I-001), not
an integer, so "mod" here means "hash to an integer, then mod" — the hash
must be **stable across processes and Python versions**, not Python's
built-in `hash()`, which is salted per-process (`PYTHONHASHSEED`) and would
route the same user_id to a different shard depending on which worker pod
computed it. blake2b gives a deterministic, well-distributed digest.
"""

import hashlib
from collections import defaultdict
from typing import Iterable

from src.worker.models import VotePayload


def shard_for_user(user_id: str, num_shards: int) -> int:
    if num_shards < 1:
        raise ValueError("num_shards must be >= 1")

    digest = hashlib.blake2b(user_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % num_shards


def route_by_shard(
    votes: Iterable[VotePayload], num_shards: int
) -> dict[int, list[VotePayload]]:
    buckets: dict[int, list[VotePayload]] = defaultdict(list)
    for vote in votes:
        buckets[shard_for_user(vote.user_id, num_shards)].append(vote)
    return dict(buckets)
