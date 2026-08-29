"""FastAPI app setup (I-003).

Wires the middleware stack (request ID / logging / latency, body size
limit, error handling), mounts the v1 routers, and exposes the health
check.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from src.api.middleware.body_limit import BodySizeLimitMiddleware
from src.api.middleware.error_handler import register_exception_handlers
from src.api.middleware.request_context import RequestContextMiddleware
from src.api.routes.admin import router as admin_router
from src.api.routes.user import router as user_router
from src.cache.health import redis_health
from src.cache.redis_client import close_redis
from src.logging.config import configure_logging
from src.schemas.responses import success_envelope

# I-018: configured at import time (not inside create_app()) so it's in
# effect for every path that imports this module — the real `uvicorn`
# entrypoint (`src/main.py`) and every test doing
# `from src.api.app import app` alike. Both the API and the worker
# (`src/worker/main.py`) call this same function, so log line shape is
# identical regardless of which process emitted it.
configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await close_redis()


def create_app() -> FastAPI:
    app = FastAPI(title="Poll App API", lifespan=lifespan)

    # Added in reverse-of-execution order: the middleware added last runs
    # first, so the body-size cap sees raw traffic before anything else.
    app.add_middleware(RequestContextMiddleware)
    app.add_middleware(BodySizeLimitMiddleware)
    register_exception_handlers(app)

    app.include_router(user_router)
    app.include_router(admin_router)

    @app.get("/health")
    async def health(request: Request):
        return success_envelope({"status": "ok", **await redis_health()}, request)

    return app


app = create_app()
