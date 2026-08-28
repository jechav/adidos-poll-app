from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_startup_host: str = "localhost"
    redis_startup_port: int = 7001
    api_port: int = 3000

    # I-008: vote processor / shard routing. `database_url` is the fallback
    # DSN used when no per-shard override (`POLL_APP_SHARD_{N}_DSN`) is set
    # — sufficient for local dev, where a single Postgres instance stands
    # in for all shards (see docs/setup/sharding.md).
    num_shards: int = 8
    database_url: str = "postgresql://postgres@localhost:5432/poll_app"

    # I-016: outbound anomaly-reporting job. `adidos_service_token` is a
    # static bearer token from config (deeper auth negotiation with
    # Adidos is assumed pre-arranged, not designed here — see I-016's
    # Out of Scope).
    adidos_anomaly_webhook_url: str = "https://adidos.example.com/v1/anomalies"
    adidos_service_token: str = ""

    model_config = {"env_prefix": "POLL_APP_"}


settings = Settings()
