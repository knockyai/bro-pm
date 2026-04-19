from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from . import models


DEFAULT_POLICY_RULES = {
    "role_order": ["viewer", "operator", "admin", "owner"],
    "safe_pause_allowed_actions": [
        "unpause_project",
        "get_status",
        "audit_view",
        "rollback_action",
        "draft_boss_escalation",
    ],
    "operator_actions": ["audit_view", "publish_report", "draft_boss_escalation"],
    "admin_actions": [
        "delete_project",
        "set_trust_policy",
        "approve_action",
        "unpause_project",
        "rollback_action",
    ],
    "viewer_denied_actions": ["pause_project"],
    "approval_required_actions": ["close_task", "delete_task", "apply_bulk"],
    "approval_reason_by_action": {
        "close_task": "approved with human confirmation",
        "delete_task": "approved with human confirmation",
        "apply_bulk": "approved with human confirmation",
        "draft_boss_escalation": "escalation draft requires operator confirmation",
    },
}


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    reason: str
    requires_approval: bool = False
    safe_pause_blocked: bool = False
    policy_key: str = "default"
    policy_version: int = 1
    policy_rule: str = "default::allow"


@dataclass(frozen=True)
class ActivePolicy:
    policy_key: str
    version: int
    rules: dict


def _normalized_string_list(name: str, value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"policy {name} must be a non-empty list of strings")

    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"policy {name} must contain non-empty strings")
        entry = item.strip()
        if entry not in normalized:
            normalized.append(entry)
    return normalized


def _normalized_reason_map(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError("policy approval_reason_by_action must be a mapping of strings")

    normalized: dict[str, str] = {}
    for key, reason in value.items():
        if not isinstance(key, str) or not key.strip() or not isinstance(reason, str) or not reason.strip():
            raise ValueError("policy approval_reason_by_action must contain non-empty string keys and values")
        normalized[key.strip()] = reason.strip()
    return normalized


class PolicyEngine:
    """Rule-based policy gate backed by a persisted active policy version."""

    def __init__(self, db_session: Session | None = None):
        self.db = db_session

    @staticmethod
    def _decision(
        allowed: bool,
        reason: str,
        active_policy: ActivePolicy,
        *,
        requires_approval: bool = False,
        safe_pause_blocked: bool = False,
        policy_rule: str,
    ) -> PolicyDecision:
        return PolicyDecision(
            allowed=allowed,
            reason=reason,
            requires_approval=requires_approval,
            safe_pause_blocked=safe_pause_blocked,
            policy_key=active_policy.policy_key,
            policy_version=active_policy.version,
            policy_rule=policy_rule,
        )

    def _active_policy(self) -> ActivePolicy:
        if self.db is not None:
            with self.db.no_autoflush:
                rows = (
                    self.db.query(models.PolicyVersion)
                    .filter_by(policy_key="default", is_active=True)
                    .order_by(models.PolicyVersion.version.desc())
                    .all()
                )
            if len(rows) > 1:
                raise RuntimeError("multiple active policy versions detected for default policy")
            if rows:
                record = rows[0]
                rules = self._validate_rules(record.rules_json)
                return ActivePolicy(policy_key=record.policy_key, version=record.version, rules=rules)
            raise RuntimeError("active policy version not found for default policy")

        return ActivePolicy(policy_key="default", version=1, rules=self._validate_rules(DEFAULT_POLICY_RULES))

    @staticmethod
    def _can_escalate(actor_role: str, required_role: str, role_order: list[str]) -> bool:
        return role_order.index(actor_role) >= role_order.index(required_role)

    @staticmethod
    def _admin_denial_reason(action: str) -> str:
        if action == "rollback_action":
            return "rollback requires admin or owner"
        if action in {"approve_action", "unpause_project"}:
            return "requires admin role"
        return "requires admin or owner role"

    def _validate_rules(self, rules: Any) -> dict:
        if not isinstance(rules, dict):
            raise ValueError("policy rules must be a mapping")

        missing = [key for key in DEFAULT_POLICY_RULES if key not in rules]
        if missing:
            raise ValueError(f"policy rules missing required keys: {', '.join(missing)}")

        normalized = {
            "role_order": _normalized_string_list("role_order", rules.get("role_order")),
            "safe_pause_allowed_actions": _normalized_string_list(
                "safe_pause_allowed_actions", rules.get("safe_pause_allowed_actions")
            ),
            "operator_actions": _normalized_string_list("operator_actions", rules.get("operator_actions")),
            "admin_actions": _normalized_string_list("admin_actions", rules.get("admin_actions")),
            "viewer_denied_actions": _normalized_string_list(
                "viewer_denied_actions", rules.get("viewer_denied_actions")
            ),
            "approval_required_actions": _normalized_string_list(
                "approval_required_actions", rules.get("approval_required_actions")
            ),
            "approval_reason_by_action": _normalized_reason_map(rules.get("approval_reason_by_action")),
        }
        if set(normalized["role_order"]) != set(DEFAULT_POLICY_RULES["role_order"]):
            raise ValueError("policy role_order must contain viewer, operator, admin, and owner exactly once")
        return normalized

    def evaluate(self, *, actor_role: str, actor_trusted: bool, action: str, safe_paused: bool) -> PolicyDecision:
        active_policy = self._active_policy()
        rules = active_policy.rules or DEFAULT_POLICY_RULES
        role_order = list(rules.get("role_order") or DEFAULT_POLICY_RULES["role_order"])

        if not actor_trusted:
            return self._decision(
                False,
                "untrusted actor blocked",
                active_policy,
                policy_rule="trust::deny_untrusted_actor",
            )

        if actor_role not in role_order:
            return self._decision(False, "unknown actor role", active_policy, policy_rule="role::unknown_actor")

        privileged = self._can_escalate(actor_role, "admin", role_order)
        safe_pause_allowed_actions = set(rules.get("safe_pause_allowed_actions") or [])
        operator_actions = set(rules.get("operator_actions") or [])
        admin_actions = set(rules.get("admin_actions") or [])
        viewer_denied_actions = set(rules.get("viewer_denied_actions") or [])
        approval_required_actions = set(rules.get("approval_required_actions") or [])
        approval_reason_by_action = dict(rules.get("approval_reason_by_action") or {})

        if safe_paused and action not in safe_pause_allowed_actions:
            return self._decision(
                False,
                "project is safe-paused",
                active_policy,
                safe_pause_blocked=True,
                policy_rule="safe_pause::deny_mutation",
            )

        if action == "draft_boss_escalation" and not self._can_escalate(actor_role, "operator", role_order):
            return self._decision(False, "requires operator role", active_policy, policy_rule="role::operator_required")

        if action in operator_actions and not self._can_escalate(actor_role, "operator", role_order):
            return self._decision(False, "requires operator role", active_policy, policy_rule="role::operator_required")

        if action == "set_trust_policy" and not privileged:
            return self._decision(
                False,
                "requires admin or owner role",
                active_policy,
                policy_rule="control_plane::policy_admin_required",
            )

        if action in admin_actions and not privileged:
            return self._decision(
                False,
                self._admin_denial_reason(action),
                active_policy,
                policy_rule=("role::rollback_admin_required" if action == "rollback_action" else "role::admin_required"),
            )

        if action in viewer_denied_actions and actor_role == "viewer":
            return self._decision(False, "viewers cannot pause project", active_policy, policy_rule="role::viewer_denied")

        if action in approval_required_actions:
            return self._decision(
                True,
                approval_reason_by_action.get(action, "approved with human confirmation"),
                active_policy,
                requires_approval=True,
                policy_rule="approval::human_confirmation_required",
            )

        if action == "draft_boss_escalation":
            return self._decision(
                True,
                approval_reason_by_action.get(action, "escalation draft requires operator confirmation"),
                active_policy,
                requires_approval=True,
                policy_rule="approval::escalation_confirmation_required",
            )

        return self._decision(True, "policy accepted", active_policy, policy_rule="default::allow")

    def activate_policy_version(
        self,
        *,
        actor_role: str,
        actor_trusted: bool,
        policy_key: str,
        version: int,
        rules: dict,
        expected_previous_version: int,
        description: str = "",
    ) -> models.PolicyVersion:
        if self.db is None:
            raise RuntimeError("policy version activation requires a database session")

        decision = self.evaluate(
            actor_role=actor_role,
            actor_trusted=actor_trusted,
            action="set_trust_policy",
            safe_paused=False,
        )
        if not decision.allowed:
            raise ValueError(decision.reason)

        validated_rules = self._validate_rules(rules)
        active_rows = (
            self.db.query(models.PolicyVersion)
            .filter_by(policy_key=policy_key, is_active=True)
            .order_by(models.PolicyVersion.version.desc())
            .all()
        )
        if len(active_rows) > 1:
            raise ValueError("multiple active policy versions detected")

        active_policy = active_rows[0] if active_rows else None
        current_version = active_policy.version if active_policy is not None else None
        if current_version != expected_previous_version:
            raise ValueError("policy version conflict")

        existing = (
            self.db.query(models.PolicyVersion)
            .filter_by(policy_key=policy_key, version=version)
            .one_or_none()
        )
        if existing is not None:
            raise ValueError("policy version conflict")

        if active_policy is not None:
            active_policy.is_active = False

        record = models.PolicyVersion(
            policy_key=policy_key,
            version=version,
            description=description,
            rules_json=validated_rules,
            is_active=True,
            activated_at=datetime.utcnow(),
        )
        self.db.add(record)
        self.db.flush()
        return record
