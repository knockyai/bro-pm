from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy.orm import Session

from .. import models
from ..integrations import INTEGRATIONS
from ..schemas import (
    ProjectReportDecision,
    ProjectReportKpis,
    ProjectReportLinks,
    ProjectReportResponse,
    ProjectReportRisk,
    ReportPublishResult,
)


class ReportingService:
    def __init__(self, db_session: Session):
        self.db = db_session
        self.notion = INTEGRATIONS["notion"]

    def generate_project_report(
        self,
        *,
        project: models.Project,
        actor: str,
        execute_publish: bool = False,
    ) -> ProjectReportResponse:
        visibility = self._normalize_visibility(project.visibility)
        slug = self._normalize_slug(project.slug)
        report_core = self._build_report_core(project=project, visibility=visibility, slug=slug)
        publish_target = f"Bro-PM/Reports/{visibility}/Projects/{slug}"
        publish_payload = {
            "workspace_root": "Bro-PM",
            "parent_page": f"Bro-PM/Reports/{visibility}",
            "project_page": f"Bro-PM/Projects/{visibility}/{slug}",
            "visibility": visibility,
            "report": report_core,
        }

        if execute_publish:
            integration_result = self.notion.execute(action="publish_report", payload=publish_payload)
            publish_status = "executed" if integration_result.ok else "failed"
            publish_detail = integration_result.detail or "notion publish_report execution failed"
            self._record_publish_audit(
                project=project,
                actor=actor,
                visibility=visibility,
                publish_target=publish_target,
                status=publish_status,
                detail=publish_detail,
            )
            publish = ReportPublishResult(
                integration="notion",
                action="publish_report",
                status=publish_status,
                target=publish_target,
                detail=publish_detail,
                visibility=visibility,
            )
            return ProjectReportResponse(
                **report_core,
                publish=publish,
            )

        self.notion.validate(action="publish_report", payload=publish_payload)
        publish = ReportPublishResult(
            integration="notion",
            action="publish_report",
            status="contract_ready",
            target=publish_target,
            detail="Notion-ready publish contract prepared; external publish not executed",
            visibility=visibility,
        )
        return ProjectReportResponse(
            **report_core,
            publish=publish,
        )

    def _build_report_core(self, *, project: models.Project, visibility: str, slug: str) -> dict:
        goals = (
            self.db.query(models.Goal)
            .filter_by(project_id=project.id)
            .order_by(models.Goal.created_at.desc(), models.Goal.id.desc())
            .all()
        )
        tasks = (
            self.db.query(models.Task)
            .filter_by(project_id=project.id)
            .order_by(models.Task.created_at.desc(), models.Task.id.desc())
            .all()
        )
        audit_events = (
            self.db.query(models.AuditEvent)
            .filter_by(project_id=project.id)
            .order_by(models.AuditEvent.created_at.desc(), models.AuditEvent.id.desc())
            .all()
        )
        report_audit_events = [event for event in audit_events if event.action != "publish_report"]

        active_goals = [goal for goal in goals if goal.status.strip().lower() == "active"]
        completed_statuses = {"done", "completed", "closed"}
        completed_tasks = sum(1 for task in tasks if task.status.strip().lower() in completed_statuses)
        open_tasks = len(tasks) - completed_tasks
        latest_signal = report_audit_events[0].action if report_audit_events else "no recent audit signal"
        goal_fragment = active_goals[0].title if active_goals else "no active goal"
        summary = (
            f"{project.name} is tracking {goal_fragment} with {open_tasks} open tasks. "
            f"Latest audit signal: {latest_signal}."
        )

        kpis = ProjectReportKpis(
            total_tasks=len(tasks),
            completed_tasks=completed_tasks,
            open_tasks=open_tasks,
            active_goals=len(active_goals),
            audit_events=len(report_audit_events),
        )

        risks = []
        decisions = []
        action_ids = []
        for event in report_audit_events:
            action_ids.append(event.id)
            payload = self._load_payload(event.payload)
            proposal_payload = payload.get("proposal", {}).get("payload", {})
            policy_reason = payload.get("policy", {}).get("reason")

            if event.action == "draft_boss_escalation":
                risks.append(
                    ProjectReportRisk(
                        kind="boss_escalation",
                        audit_id=event.id,
                        action=event.action,
                        status=event.result,
                        summary=proposal_payload.get("escalation_message") or policy_reason or event.action,
                    )
                )
                continue

            if event.result == "executed":
                decisions.append(
                    ProjectReportDecision(
                        audit_id=event.id,
                        action=event.action,
                        result=event.result,
                        summary=policy_reason or event.action,
                    )
                )

        links = ProjectReportLinks(
            project=f"Bro-PM/Projects/{visibility}/{slug}",
            tasks=f"Bro-PM/Projects/{visibility}/{slug}/Tasks",
            audit_events=f"Bro-PM/Projects/{visibility}/{slug}/Audit",
            report=f"Bro-PM/Reports/{visibility}/Projects/{slug}",
            notion_parent=f"Bro-PM/Reports/{visibility}",
            notion_project=f"Bro-PM/Projects/{visibility}/{slug}",
        )

        return {
            "project_id": project.id,
            "report_type": "project_report",
            "visibility": visibility,
            "summary": summary,
            "kpis": kpis.model_dump(),
            "risks": [risk.model_dump() for risk in risks],
            "decisions": [decision.model_dump() for decision in decisions],
            "action_ids": action_ids,
            "links": links.model_dump(),
        }

    def _record_publish_audit(
        self,
        *,
        project: models.Project,
        actor: str,
        visibility: str,
        publish_target: str,
        status: str,
        detail: str,
    ) -> None:
        payload = {
            "integration": {
                "name": "notion",
                "action": "publish_report",
                "status": status,
                "detail": detail,
            },
            "visibility": visibility,
            "target": publish_target,
            "created_via": "project_report",
            "actor": actor,
        }
        self.db.add(
            models.AuditEvent(
                project_id=project.id,
                actor=actor,
                action="publish_report",
                target_type="report",
                target_id=publish_target,
                payload=json.dumps(payload, ensure_ascii=False),
                result=status,
                created_at=datetime.utcnow(),
            )
        )

    @staticmethod
    def _normalize_visibility(raw_visibility: str | None) -> str:
        normalized = (raw_visibility or "").strip()
        if not normalized:
            raise ValueError("visibility must not be empty")
        if "/" in normalized:
            raise ValueError("visibility must not contain '/'")
        return normalized

    @staticmethod
    def _normalize_slug(raw_slug: str) -> str:
        normalized = (raw_slug or "").strip()
        if not normalized:
            raise ValueError("slug must not be empty")
        if "/" in normalized:
            raise ValueError("slug must not contain '/'")
        return normalized

    @staticmethod
    def _load_payload(raw_payload: str | None) -> dict:
        if not raw_payload:
            return {}
        try:
            loaded = json.loads(raw_payload)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return loaded if isinstance(loaded, dict) else {}
