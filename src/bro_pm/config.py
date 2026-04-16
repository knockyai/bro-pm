import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Runtime configuration for the MVP backend."""

    app_name: str = "Bro-PM API"
    database_url: str = os.getenv("BRO_PM_DATABASE_URL", "sqlite:///./bro_pm.db")
    hermes_api_base: str | None = os.getenv("BRO_PM_HERMES_API_BASE")
    hermes_api_key: str | None = os.getenv("BRO_PM_HERMES_API_KEY")
    default_safe_pause: bool = True
    max_payload_bytes: int = int(os.getenv("BRO_PM_MAX_PAYLOAD_BYTES", "204800"))
    trusted_actor_header: str = "x-actor-trusted"


settings = Settings()
