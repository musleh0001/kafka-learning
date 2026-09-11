from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    orders_topic: str = Field(default="orders")
    notification_group: str = Field(default="notification-service")


config = Config()
