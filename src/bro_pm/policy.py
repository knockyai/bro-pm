from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    reason: str
    requires_approval: bool = False
    safe_pause_blocked: bool = False


class PolicyEngine:
    """Rule-based policy gate for MVP.

    These checks are intentionally strict and explicit to avoid policy being hidden
    in prompts. They run for every command/execution path.
    """

    role_order = ["viewer", "operator", "admin", "owner"]

    def _can_escalate(self, actor_role: str, required_role: str) -> bool:
        return self.role_order.index(actor_role) >= self.role_order.index(required_role)

    def evaluate(self, *, actor_role: str, actor_trusted: bool, action: str, safe_paused: bool) -> PolicyDecision:
        if not actor_trusted:
            return PolicyDecision(False, "untrusted actor blocked", safe_pause_blocked=False)

        if actor_role not in self.role_order:
            return PolicyDecision(False, "unknown actor role")

        privileged = self._can_escalate(actor_role, "admin")

        # safe pause is a hard stop for unsafe operations in MVP.
        # Audit-only draft boss escalations stay permitted for reporting, but still
        # follow role and project-context enforcement elsewhere.
        if safe_paused and action not in {
            "unpause_project",
            "get_status",
            "audit_view",
            "rollback_action",
            "draft_boss_escalation",
        }:
            return PolicyDecision(False, "project is safe-paused", safe_pause_blocked=True)

        if action == "draft_boss_escalation" and not self._can_escalate(actor_role, "operator"):
            return PolicyDecision(False, "requires operator role")

        if action in {"audit_view", "publish_report"} and not self._can_escalate(actor_role, "operator"):
            return PolicyDecision(False, "requires operator role")

        if action in {"delete_project", "set_trust_policy"} and not privileged:
            return PolicyDecision(False, "requires admin or owner role")

        if action == "pause_project" and actor_role == "viewer":
            return PolicyDecision(False, "viewers cannot pause project")

        if action in {"approve_action", "unpause_project"} and actor_role not in {"admin", "owner"}:
            return PolicyDecision(False, "requires admin role")

        if action == "rollback_action" and actor_role not in {"admin", "owner"}:
            return PolicyDecision(False, "rollback requires admin or owner")

        # high-risk command requires approval path in audit logs
        if action in {"close_task", "delete_task", "apply_bulk"}:
            return PolicyDecision(True, "approved with human confirmation", requires_approval=True)

        if action == "draft_boss_escalation":
            return PolicyDecision(True, "escalation draft requires operator confirmation", requires_approval=True)

        return PolicyDecision(True, "policy accepted")
