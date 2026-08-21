from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_startup_host: str = "localhost"
    redis_startup_port: int = 7001

    model_config = {"env_prefix": "POLL_APP_"}


settings = Settings()
