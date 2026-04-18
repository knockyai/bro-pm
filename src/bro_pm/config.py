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


def _env_bool(name: str, default: bool) -> bool:
    value = _env_optional(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


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
    yandex_tracker_backend: str = field(default_factory=lambda: _env_default("BRO_PM_YANDEX_TRACKER_BACKEND", "native"))
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
    yandex_tracker_mcp_command: str | None = field(default_factory=lambda: _env_optional("BRO_PM_YANDEX_TRACKER_MCP_COMMAND"))
    yandex_tracker_mcp_args_json: str | None = field(
        default_factory=lambda: _env_optional("BRO_PM_YANDEX_TRACKER_MCP_ARGS_JSON")
    )
    yandex_tracker_mcp_env_json: str | None = field(
        default_factory=lambda: _env_optional("BRO_PM_YANDEX_TRACKER_MCP_ENV_JSON")
    )
    yandex_tracker_mcp_cwd: str | None = field(default_factory=lambda: _env_optional("BRO_PM_YANDEX_TRACKER_MCP_CWD"))
    yandex_tracker_mcp_tool_name: str | None = field(
        default_factory=lambda: _env_optional("BRO_PM_YANDEX_TRACKER_MCP_TOOL_NAME")
    )
    yandex_tracker_mcp_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("BRO_PM_YANDEX_TRACKER_MCP_TIMEOUT_SECONDS", "45"))
    )
    timer_actions_enabled: bool = field(default_factory=lambda: _env_bool("BRO_PM_TIMER_ACTIONS_ENABLED", True))
    timer_actions_poll_interval_seconds: float = field(
        default_factory=lambda: float(os.getenv("BRO_PM_TIMER_ACTIONS_POLL_INTERVAL_SECONDS", "60"))
    )


settings = Settings()
