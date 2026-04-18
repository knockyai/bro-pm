import os
from dataclasses import dataclass, field


def _env_optional(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _env_default(name: str, default: str) -> str:
    value = _env_optional(name)
    return value if value is not None else default


@dataclass(frozen=True)
class Settings:
    """Runtime configuration for the MVP backend."""

    app_name: str = "Bro-PM API"
    database_url: str = field(default_factory=lambda: os.getenv("BRO_PM_DATABASE_URL", "sqlite:///./bro_pm.db"))
    hermes_api_base: str | None = field(default_factory=lambda: _env_optional("BRO_PM_HERMES_API_BASE"))
    hermes_api_key: str | None = field(default_factory=lambda: _env_optional("BRO_PM_HERMES_API_KEY"))
    default_safe_pause: bool = True
    max_payload_bytes: int = field(default_factory=lambda: int(os.getenv("BRO_PM_MAX_PAYLOAD_BYTES", "204800")))
    trusted_actor_header: str = "x-actor-trusted"
    yandex_tracker_api_base: str = field(
        default_factory=lambda: _env_default("BRO_PM_YANDEX_TRACKER_API_BASE", "https://api.tracker.yandex.net/v2")
    )
    yandex_tracker_token: str | None = field(default_factory=lambda: _env_optional("BRO_PM_YANDEX_TRACKER_TOKEN"))
    yandex_tracker_auth_prefix: str = field(
        default_factory=lambda: _env_default(
            "BRO_PM_YANDEX_TRACKER_AUTH_PREFIX",
            _env_default("BRO_PM_YANDEX_TRACKER_AUTH_SCHEME", "OAuth"),
        )
    )
    yandex_tracker_org_header_name: str = field(
        default_factory=lambda: _env_default("BRO_PM_YANDEX_TRACKER_ORG_HEADER_NAME", "X-Org-ID")
    )
    yandex_tracker_org_id: str | None = field(default_factory=lambda: _env_optional("BRO_PM_YANDEX_TRACKER_ORG_ID"))
    yandex_tracker_default_queue: str | None = field(
        default_factory=lambda: _env_optional("BRO_PM_YANDEX_TRACKER_DEFAULT_QUEUE")
    )


settings = Settings()
