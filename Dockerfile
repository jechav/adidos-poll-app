# Local-dev container for the FastAPI app (I-025).
#
# This exists so the app can join the same Docker network as the Redis
# cluster nodes and address them by internal hostname/port, sidestepping
# the host<->container dual-address problem Redis Cluster can't solve on
# its own (see docs/issues/I-025-redis-cluster-announce-address.md).

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY scripts ./scripts

EXPOSE 8000

CMD ["uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
