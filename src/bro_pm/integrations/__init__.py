from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class IntegrationError(RuntimeError):
    pass


class IntegrationResult:
    def __init__(self, *, ok: bool, detail: str = ""):
        self.ok = ok
        self.detail = detail


class Integration(Protocol):
    name: str

    def execute(self, *, action: str, payload: dict) -> IntegrationResult: ...

    def validate(self, *, action: str, payload: dict) -> None: ...


@dataclass
class NotionIntegration:
    name: str = "notion"

    def validate(self, *, action: str, payload: dict) -> None:
        if action not in {"noop", "create_task", "close_task"}:
            raise IntegrationError(f"unsupported action for notion: {action}")

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
    "telegram": TelegramIntegration(),
    "slack": SlackIntegration(),
}
