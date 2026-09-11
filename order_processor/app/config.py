from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    order_topic: str = Field(default="order-events")
    inventory_topic: str = Field(default="inventory-events")
    payment_topic: str = Field(default="payment-events")
    group_id: str = Field(default="order-processor")


config = Config()
