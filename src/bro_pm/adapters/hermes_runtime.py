from __future__ import annotations

import os
from dataclasses import dataclass

from ..schemas import CommandProposal


@dataclass
class HermesRuntimeResult:
    action: str
    reason: str
    payload: dict
    requires_approval: bool = False


class HermesAdapter:
    """Hermes runtime wrapper.

    MVP uses a deterministic local parser by default. Real Hermes provider integration
    can be injected later without changing backend contracts.
    """

    def __init__(self, *, prefer_remote: bool = False):
        self.prefer_remote = prefer_remote

    def propose(self, actor: str, command_text: str) -> CommandProposal:
        if self.prefer_remote and os.getenv("BRO_PM_HERMES_REMOTE", "false").lower() == "true":
            return self._remote_fallback(actor, command_text)
        return self._deterministic_parser(actor, command_text)

    def _remote_fallback(self, actor: str, command_text: str) -> CommandProposal:
        # Reserved for future integration. Keep explicit failure mode for safety.
        raise RuntimeError("remote Hermes runtime not enabled")

    def _deterministic_parser(self, actor: str, command_text: str) -> CommandProposal:
        text = command_text.strip().lower()
        if text.startswith("pause project "):
            target = text.removeprefix("pause project ").strip()
            return CommandProposal(
                action="pause_project",
                project_id=target,
                reason="parsed command",
                payload={"mode": "pause", "raw_command": command_text},
            )
        if text.startswith("resume project "):
            target = text.removeprefix("resume project ").strip()
            return CommandProposal(
                action="unpause_project",
                project_id=target,
                reason="parsed command",
                payload={"mode": "resume", "raw_command": command_text},
            )
        if text.startswith("create task "):
            title = text.removeprefix("create task ").strip()
            return CommandProposal(
                action="create_task",
                reason="parsed command",
                payload={"title": title, "raw_command": command_text},
            )
        if text.startswith("close task "):
            tid = text.removeprefix("close task ").strip()
            return CommandProposal(
                action="close_task",
                reason="parsed command",
                payload={"target_type": "task", "target_id": tid, "raw_command": command_text},
            )

        # fallback with zero side effects
        return CommandProposal(action="noop", reason="unrecognized command")
