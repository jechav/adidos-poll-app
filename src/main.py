"""Entrypoint: `python -m src.main` runs the API server on `settings.api_port`."""

import uvicorn

from src.config import settings

if __name__ == "__main__":
    uvicorn.run("src.api.app:app", host="0.0.0.0", port=settings.api_port)
