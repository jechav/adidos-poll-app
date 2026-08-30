"""`GET /metrics` — Prometheus scrape endpoint (I-017).

Deliberately not wrapped in I-003's JSON success/error envelope: a
Prometheus scraper expects the plain-text exposition format, not JSON.
It is also distinct from `GET /health` (I-003) — `/health` answers "is
this process alive and ready" (liveness/readiness probes); `/metrics`
answers "what happened" (the scrape target). Conflating the two would let
a slow metrics render fail a liveness probe, or gate a scrape behind
readiness logic it doesn't need.
"""

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from src.metrics.registry import REGISTRY

router = APIRouter()


@router.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
