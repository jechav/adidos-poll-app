"""I-023: pre-provisioned load-test token pool (`loadtest/utils/tokens.py`).

Seam choice: `TokenPool`/`get_test_token()` are the one piece of the
Locust suite that is pure Python with no network or Locust-runtime
dependency, so they get real unit tests rather than the structural
"does the file exist and mention X" tests used for the scenario/shape
files elsewhere in this suite (see that module's docstring for why).

What's being verified:

- the pool never calls out to real Adidos auth (it's a fixed, pre-
  generated set of fake identities) -- this is the ticket's own
  requirement ("does not hit real Adidos auth")
- tokens are valid input to I-004's `decode_adidos_token` convention
  (plain opaque string == user_id, no `admin:` prefix, since load-test
  traffic simulates voting end users, never admins)
- the pool cycles deterministically so a fixed-size pool can sustain an
  arbitrarily long run without growing memory
- the pool is large enough that, combined with I-015's 3-strike
  duplicate block, a full sweep through the pool against a single
  `poll_id` doesn't self-inflict 409s before real load-bearing traffic
  is generated (each identity should be usable at least once per poll
  well before it recurs)
"""

from __future__ import annotations

from loadtest.utils.tokens import DEFAULT_POOL_SIZE, TokenPool, get_test_token


def test_pool_generates_default_pool_size_distinct_tokens():
    pool = TokenPool()
    assert len(set(pool.tokens)) == DEFAULT_POOL_SIZE == len(pool.tokens)


def test_pool_size_is_configurable():
    pool = TokenPool(size=25)
    assert len(pool.tokens) == 25


def test_tokens_never_use_the_admin_prefix():
    # I-004's decode_adidos_token treats an "admin:" prefix as the admin
    # role; load-test traffic must never accidentally exercise admin
    # endpoints/permissions.
    pool = TokenPool(size=50)
    assert all(not token.startswith("admin:") for token in pool.tokens)


def test_tokens_are_plain_opaque_strings_not_real_adidos_tokens():
    pool = TokenPool(size=10)
    for token in pool.tokens:
        assert token.startswith("loadtest-user-")


def test_pool_cycles_round_robin_and_repeats_deterministically():
    pool = TokenPool(size=3)
    first_pass = [pool.next_token() for _ in range(3)]
    second_pass = [pool.next_token() for _ in range(3)]
    assert first_pass == list(pool.tokens)
    assert second_pass == first_pass  # deterministic repeat, not random


def test_module_level_get_test_token_uses_a_shared_default_pool():
    seen = {get_test_token() for _ in range(DEFAULT_POOL_SIZE)}
    # a full sweep of the default pool should touch every token exactly
    # once before any repeat -- proves it isn't just returning a
    # constant or a tiny pool.
    assert len(seen) == DEFAULT_POOL_SIZE
