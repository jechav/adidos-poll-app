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

    model_config = {"env_prefix": "POLL_APP_"}


settings = Settings()
