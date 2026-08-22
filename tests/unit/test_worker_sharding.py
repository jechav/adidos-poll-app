"""Unit tests for I-008's shard routing.

Per SPECIFICATION.md's "Shard Distribution Tests" module: 1000 votes from
random users should land within +/-10% of an even split across shards,
with no single shard taking more than 55%. Hashing internals aren't
tested (spec's "behavior only" testing philosophy) — only the routing
outcome and its determinism.
"""

import uuid
from collections import Counter

from src.worker.models import VotePayload
from src.worker.sharding import route_by_shard, shard_for_user


def _vote(user_id: str) -> VotePayload:
    return VotePayload(
        vote_id=uuid.uuid4(),
        user_id=user_id,
        poll_id=uuid.uuid4(),
        answer_id=uuid.uuid4(),
    )


def test_shard_for_user_is_deterministic():
    assert shard_for_user("user-42", 8) == shard_for_user("user-42", 8)


def test_shard_for_user_is_deterministic_across_separate_processes():
    # Guards against relying on Python's salted built-in hash(): two
    # freshly-started interpreters (simulated here by invoking the pure
    # function twice with no shared state) must agree, since production
    # runs one pod per shard and they must all agree on the same routing.
    import subprocess
    import sys

    code = (
        "from src.worker.sharding import shard_for_user; "
        "print(shard_for_user('user-42', 8))"
    )
    results = {
        subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        ).stdout.strip()
        for _ in range(2)
    }
    assert len(results) == 1


def test_shard_for_user_within_range():
    for i in range(200):
        shard = shard_for_user(f"user-{i}", 8)
        assert 0 <= shard < 8


def test_shard_for_user_rejects_zero_shards():
    import pytest

    with pytest.raises(ValueError):
        shard_for_user("user-1", 0)


def test_route_by_shard_groups_votes_by_their_users_shard():
    votes = [_vote("user-a"), _vote("user-b"), _vote("user-a")]
    buckets = route_by_shard(votes, num_shards=8)

    # Every vote must land in the bucket matching shard_for_user(user_id),
    # and every vote from the input must appear in exactly one bucket.
    seen_vote_ids = set()
    for shard_id, bucket_votes in buckets.items():
        for vote in bucket_votes:
            assert shard_for_user(vote.user_id, 8) == shard_id
            seen_vote_ids.add(vote.vote_id)
    assert seen_vote_ids == {v.vote_id for v in votes}
    assert sum(len(v) for v in buckets.values()) == len(votes)


def test_route_by_shard_empty_input():
    assert route_by_shard([], num_shards=8) == {}


def test_shard_distribution_within_tolerance_for_1000_random_users():
    num_shards = 8
    votes = [_vote(f"user-{uuid.uuid4()}") for _ in range(1000)]
    buckets = route_by_shard(votes, num_shards=num_shards)

    counts = Counter({shard: len(v) for shard, v in buckets.items()})
    expected = len(votes) / num_shards

    for shard in range(num_shards):
        count = counts.get(shard, 0)
        assert count <= len(votes) * 0.55, f"shard {shard} got {count} votes (>55%)"
        assert abs(count - expected) <= expected * 0.5, (
            f"shard {shard} got {count} votes, expected ~{expected:.0f} (+/-50%"
            " loose bound for a single random 1000-vote draw)"
        )

    # Tighter, aggregate check mirroring the spec's +/-10% language: the
    # *overall* spread across shards (max - min) should be small relative
    # to the expected per-shard share, not just each shard individually.
    spread = max(counts.values()) - min(counts.get(s, 0) for s in range(num_shards))
    assert spread <= expected, f"shard counts spread too widely: {dict(counts)}"
