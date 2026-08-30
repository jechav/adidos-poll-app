"""Unit tests for I-018's deterministic vote-level sampling.

`is_sampled` decides whether a *successful* vote's full lifecycle
(API accept -> enqueue -> worker dequeue -> DB write) is logged. It must
be a pure function of `vote_id` alone — the same vote_id always yields
the same answer, computed identically wherever it's needed (API process
and worker process), and roughly 1-in-1000 votes land in the sample.
"""

import uuid

from src.logging.sampling import is_sampled


def test_is_sampled_is_deterministic_for_the_same_vote_id():
    vote_id = uuid.uuid4()

    first = is_sampled(vote_id)
    second = is_sampled(vote_id)

    assert first == second


def test_is_sampled_true_for_a_vote_id_whose_int_is_a_multiple_of_1000():
    vote_id = uuid.UUID(int=2000)

    assert is_sampled(vote_id) is True


def test_is_sampled_false_for_a_vote_id_whose_int_is_not_a_multiple_of_1000():
    vote_id = uuid.UUID(int=2001)

    assert is_sampled(vote_id) is False


def test_is_sampled_distribution_is_roughly_one_in_a_thousand():
    sample_size = 200_000
    sampled_count = sum(1 for _ in range(sample_size) if is_sampled(uuid.uuid4()))

    expected = sample_size / 1000
    # Random UUIDs, so allow generous slack around the expected count
    # rather than asserting an exact ratio.
    assert expected * 0.5 <= sampled_count <= expected * 1.5
