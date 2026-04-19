from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


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

    def supports_verification(self, *, action: str, payload: dict) -> bool: ...

    def verify_action_result(
        self,
        *,
        action: str,
        payload: dict,
        result: IntegrationResult,
    ) -> IntegrationResult: ...

    def fetch_state(
        self,
        *,
        action: str,
        payload: dict,
        result: IntegrationResult | None = None,
    ) -> dict[str, Any]: ...


class DurableIntegrationAdapter:
    def supports_verification(self, *, action: str, payload: dict) -> bool:
        return False

    def verify_action_result(
        self,
        *,
        action: str,
        payload: dict,
        result: IntegrationResult,
    ) -> IntegrationResult:
        state = self.fetch_state(action=action, payload=payload, result=result)
        return IntegrationResult(
            ok=False,
            detail=f"{getattr(self, 'name', 'integration')} verification not implemented for {action}",
            metadata={"state": state},
        )

    def fetch_state(
        self,
        *,
        action: str,
        payload: dict,
        result: IntegrationResult | None = None,
    ) -> dict[str, Any]:
        metadata = dict(result.metadata) if result is not None else {}
        return {
            "exists": False,
            "action": action,
            **metadata,
        }


@dataclass
class NotionIntegration(DurableIntegrationAdapter):
    name: str = "notion"

    def validate(self, *, action: str, payload: dict) -> None:
        if action not in {"noop", "create_task", "close_task", "publish_report"}:
            raise IntegrationError(f"unsupported action for notion: {action}")
        if action == "create_task":
            if not payload.get("project_id"):
                raise IntegrationError("missing project_id for notion create_task")
            if not payload.get("title"):
                raise IntegrationError("missing title for notion create_task")
        if action == "publish_report":
            if not payload.get("report"):
                raise IntegrationError("missing report payload for notion publish_report")
            if not payload.get("visibility"):
                raise IntegrationError("missing visibility for notion publish_report")

    def execute(self, *, action: str, payload: dict) -> IntegrationResult:
        self.validate(action=action, payload=payload)
        if action == "create_task":
            return IntegrationResult(
                ok=True,
                detail="notion executed: create_task",
                metadata={
                    "external_id": self._task_external_id(payload),
                },
            )
        return IntegrationResult(ok=True, detail=f"notion executed: {action}")

    def _task_external_id(self, payload: dict) -> str:
        execution = payload.get("bro_pm_execution")
        if isinstance(execution, dict):
            if execution.get("audit_event_id"):
                return f"notion-task:{execution['audit_event_id']}"
            if execution.get("idempotency_key"):
                return f"notion-task:{execution['idempotency_key']}"
        title = str(payload.get("title") or "task").strip().lower().replace(" ", "-")
        return f"notion-task:{title}"


@dataclass
class JiraIntegration(DurableIntegrationAdapter):
    name: str = "jira"

    def validate(self, *, action: str, payload: dict) -> None:
        if action not in {"noop", "create_task", "close_task", "approve_action"}:
            raise IntegrationError(f"unsupported action for jira: {action}")

    def execute(self, *, action: str, payload: dict) -> IntegrationResult:
        self.validate(action=action, payload=payload)
        return IntegrationResult(ok=True, detail=f"jira executed: {action}")


@dataclass
class TrelloIntegration(DurableIntegrationAdapter):
    name: str = "trello"

    def validate(self, *, action: str, payload: dict) -> None:
        if action not in {"noop", "create_task", "close_task"}:
            raise IntegrationError(f"unsupported action for trello: {action}")

    def execute(self, *, action: str, payload: dict) -> IntegrationResult:
        self.validate(action=action, payload=payload)
        return IntegrationResult(ok=True, detail=f"trello executed: {action}")


from .yandex_tracker import YandexTrackerIntegration


@dataclass
class TelegramIntegration(DurableIntegrationAdapter):
    name: str = "telegram"

    def validate(self, *, action: str, payload: dict) -> None:
        # Telegram receives notifications and summaries in MVP
        if action not in {"noop", "notify", "announce"}:
            return

    def execute(self, *, action: str, payload: dict) -> IntegrationResult:
        self.validate(action=action, payload=payload)
        return IntegrationResult(ok=True, detail=f"telegram executed: {action}")


@dataclass
class SlackIntegration(DurableIntegrationAdapter):
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
