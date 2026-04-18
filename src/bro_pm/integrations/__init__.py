from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from ..config import Settings, settings as runtime_settings


class IntegrationError(RuntimeError):
    pass


class IntegrationResult:
    def __init__(self, *, ok: bool, detail: str = "", metadata: dict[str, Any] | None = None):
        self.ok = ok
        self.detail = detail
        self.metadata = metadata or {}


class Integration(Protocol):
    name: str

    def execute(self, *, action: str, payload: dict) -> IntegrationResult: ...

    def validate(self, *, action: str, payload: dict) -> None: ...


@dataclass
class NotionIntegration:
    name: str = "notion"

    def validate(self, *, action: str, payload: dict) -> None:
        if action not in {"noop", "create_task", "close_task", "publish_report"}:
            raise IntegrationError(f"unsupported action for notion: {action}")
        if action == "publish_report":
            if not payload.get("report"):
                raise IntegrationError("missing report payload for notion publish_report")
            if not payload.get("visibility"):
                raise IntegrationError("missing visibility for notion publish_report")

    def execute(self, *, action: str, payload: dict) -> IntegrationResult:
        self.validate(action=action, payload=payload)
        return IntegrationResult(ok=True, detail=f"notion executed: {action}")


@dataclass
class JiraIntegration:
    name: str = "jira"

    def validate(self, *, action: str, payload: dict) -> None:
        if action not in {"noop", "create_task", "close_task", "approve_action"}:
            raise IntegrationError(f"unsupported action for jira: {action}")

    def execute(self, *, action: str, payload: dict) -> IntegrationResult:
        self.validate(action=action, payload=payload)
        return IntegrationResult(ok=True, detail=f"jira executed: {action}")


@dataclass
class TrelloIntegration:
    name: str = "trello"

    def validate(self, *, action: str, payload: dict) -> None:
        if action not in {"noop", "create_task", "close_task"}:
            raise IntegrationError(f"unsupported action for trello: {action}")

    def execute(self, *, action: str, payload: dict) -> IntegrationResult:
        self.validate(action=action, payload=payload)
        return IntegrationResult(ok=True, detail=f"trello executed: {action}")


@dataclass
class YandexTrackerIntegration:
    name: str = "yandex_tracker"
    settings: Settings = field(default_factory=lambda: runtime_settings)
    urlopen: Callable[..., Any] = field(default_factory=lambda: urllib.request.urlopen, repr=False)
    timeout_seconds: int = 30

    def validate(self, *, action: str, payload: dict) -> None:
        if action not in {"noop", "create_task", "close_task"}:
            raise IntegrationError(f"unsupported action for yandex_tracker: {action}")
        if action != "create_task":
            return
        if not self._normalized_text(payload.get("project_id")):
            raise IntegrationError("missing project_id for yandex_tracker create_task")
        if not self._normalized_text(payload.get("title")):
            raise IntegrationError("missing title for yandex_tracker create_task")
        self._validated_context(payload)

    def execute(self, *, action: str, payload: dict) -> IntegrationResult:
        self.validate(action=action, payload=payload)
        if action != "create_task":
            return IntegrationResult(ok=True, detail=f"yandex_tracker executed: {action}")

        context = self._validated_context(payload)
        request_payload = {
            "queue": context["queue"],
            "summary": context["title"],
        }
        if context["description"]:
            request_payload["description"] = context["description"]

        request = urllib.request.Request(
            f"{context['api_base'].rstrip('/')}/issues/",
            data=json.dumps(request_payload).encode("utf-8"),
            headers={
                "Authorization": context["authorization"],
                context["org_header_name"]: context["org_id"],
                "Content-Type": "application/json; charset=utf-8",
                "Accept": "application/json",
            },
            method="POST",
        )

        try:
            with self.urlopen(request, timeout=self.timeout_seconds) as response:
                response_payload = self._load_json_bytes(response.read())
        except urllib.error.HTTPError as exc:
            error_detail = self._extract_error_detail(self._load_json_bytes(exc.read()), fallback=str(exc.reason or exc.msg))
            raise IntegrationError(f"yandex_tracker create_task failed with HTTP {exc.code}: {error_detail}") from exc
        except urllib.error.URLError as exc:
            raise IntegrationError(f"yandex_tracker create_task failed: {exc.reason}") from exc
        except OSError as exc:
            raise IntegrationError(f"yandex_tracker create_task failed: {exc}") from exc

        issue_key = self._normalized_text(response_payload.get("key")) if isinstance(response_payload, dict) else None
        issue_id = self._normalized_text(response_payload.get("id")) if isinstance(response_payload, dict) else None
        metadata = {"queue": context["queue"]}
        if issue_key:
            metadata["issue_key"] = issue_key
        if issue_id:
            metadata["issue_id"] = issue_id
        return IntegrationResult(
            ok=True,
            detail=self._success_detail(issue_key=issue_key, issue_id=issue_id),
            metadata=metadata,
        )

    def _validated_context(self, payload: dict) -> dict[str, str | None]:
        api_base = self._required_setting(self.settings.yandex_tracker_api_base, "missing yandex_tracker api base")
        token = self._required_setting(self.settings.yandex_tracker_token, "missing yandex_tracker token")
        auth_prefix = self._required_setting(self.settings.yandex_tracker_auth_prefix, "missing yandex_tracker auth prefix")
        org_header_name = self._required_setting(
            self.settings.yandex_tracker_org_header_name,
            "missing yandex_tracker org header name",
        )
        org_id = self._required_setting(self.settings.yandex_tracker_org_id, "missing yandex_tracker org id")
        queue = self._resolve_queue(payload)
        if not queue:
            raise IntegrationError("missing yandex_tracker queue")
        return {
            "api_base": api_base,
            "authorization": f"{auth_prefix} {token}",
            "org_header_name": org_header_name,
            "org_id": org_id,
            "queue": queue,
            "title": self._normalized_text(payload.get("title")),
            "description": self._normalized_text(payload.get("description")),
        }

    def _resolve_queue(self, payload: dict) -> str | None:
        direct_queue = self._normalized_text(payload.get("queue"))
        if direct_queue:
            return direct_queue
        project_metadata = payload.get("project_metadata")
        if isinstance(project_metadata, dict):
            integrations = project_metadata.get("integrations")
            if isinstance(integrations, dict):
                yandex_metadata = integrations.get("yandex_tracker")
                if isinstance(yandex_metadata, dict):
                    metadata_queue = self._normalized_text(yandex_metadata.get("queue"))
                    if metadata_queue:
                        return metadata_queue
        return self._normalized_text(self.settings.yandex_tracker_default_queue)

    @staticmethod
    def _normalized_text(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        normalized = value.strip()
        return normalized or None

    def _required_setting(self, value: Any, error_message: str) -> str:
        normalized = self._normalized_text(value)
        if not normalized:
            raise IntegrationError(error_message)
        return normalized

    @staticmethod
    def _load_json_bytes(raw_value: bytes) -> dict[str, Any] | list[Any] | str:
        text = raw_value.decode("utf-8", errors="replace").strip()
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    @staticmethod
    def _extract_error_detail(payload: dict[str, Any] | list[Any] | str, *, fallback: str) -> str:
        if isinstance(payload, dict):
            errors = payload.get("errors")
            if isinstance(errors, list):
                parts = [str(item).strip() for item in errors if str(item).strip()]
                if parts:
                    return "; ".join(parts)
            for key in ("error", "message", "description"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        if isinstance(payload, str) and payload.strip():
            return payload.strip()
        return fallback.strip() or "request failed"

    @staticmethod
    def _success_detail(*, issue_key: str | None, issue_id: str | None) -> str:
        if issue_key and issue_id:
            return f"yandex_tracker created task {issue_key} (id: {issue_id})"
        if issue_key:
            return f"yandex_tracker created task {issue_key}"
        if issue_id:
            return f"yandex_tracker created task id={issue_id}"
        return "yandex_tracker created task"


@dataclass
class TelegramIntegration:
    name: str = "telegram"

    def validate(self, *, action: str, payload: dict) -> None:
        # Telegram receives notifications and summaries in MVP
        if action not in {"noop", "notify", "announce"}:
            return

    def execute(self, *, action: str, payload: dict) -> IntegrationResult:
        self.validate(action=action, payload=payload)
        return IntegrationResult(ok=True, detail=f"telegram executed: {action}")


@dataclass
class SlackIntegration:
    name: str = "slack"

    def validate(self, *, action: str, payload: dict) -> None:
        if action not in {"noop", "notify", "announce"}:
            return

    def execute(self, *, action: str, payload: dict) -> IntegrationResult:
        self.validate(action=action, payload=payload)
        return IntegrationResult(ok=True, detail=f"slack executed: {action}")


INTEGRATIONS: dict[str, Integration] = {
    "notion": NotionIntegration(),
    "jira": JiraIntegration(),
    "trello": TrelloIntegration(),
    "yandex_tracker": YandexTrackerIntegration(),
    "telegram": TelegramIntegration(),
    "slack": SlackIntegration(),
}
