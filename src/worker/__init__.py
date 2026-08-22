"""Standalone vote processor worker (I-008).

Drains `queue:votes`, routes votes to the correct PostgreSQL shard, and
writes them durably. See `src.worker.main` for the process entrypoint.
"""
