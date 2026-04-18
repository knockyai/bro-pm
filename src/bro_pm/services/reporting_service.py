from __future__ import annotations

import json
import time
from datetime import datetime, timedelta

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .. import models
from ..integrations import INTEGRATIONS, IntegrationError
from ..schemas import (
    ProjectReportDecision,
    ProjectReportKpis,
    ProjectReportLinks,
    ProjectReportResponse,
    ProjectReportRisk,
    ReportPublishResult,
)


class ReportIdempotencyConflictError(Exception):
    pass


class ReportingService:
    PENDING_PUBLISH_STALE_AFTER = timedelta(minutes=5)
    PENDING_PUBLISH_WAIT_ATTEMPTS = 100
    PENDING_PUBLISH_WAIT_DELAY_SECONDS = 0.05

    def __init__(self, db_session: Session):
        self.db = db_session
        self.notion = INTEGRATIONS["notion"]

    def _build_request_context(
        self,
        *,
        project_id: str,
        actor: str,
        role: str,
        actor_trusted: bool,
        execute_publish: bool,
    ) -> dict:
        return {
            "project_id": project_id,
            "actor": actor,
            "role": role,
            "actor_trusted": actor_trusted,
            "execute_publish": execute_publish,
        }

    def replay_existing_publish_if_available(
        self,
        *,
        project_id: str,
        actor: str,
        role: str,
        actor_trusted: bool,
        idempotency_key: str | None,
        execute_publish: bool,
    ) -> ProjectReportResponse | None:
        if not execute_publish or not idempotency_key:
            return None

        request_context = self._build_request_context(
            project_id=project_id,
            actor=actor,
            role=role,
            actor_trusted=actor_trusted,
            execute_publish=execute_publish,
        )
        existing = self.db.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one_or_none()
        if existing and existing.result == "pending_publish" and not self._is_stale_pending_publish(existing):
            existing = self._wait_for_existing_publish_record(
                idempotency_key,
                wait_for_stable_result=True,
            )
        if not existing:
            return None
        return self._replay_existing_publish(
            existing=existing,
            request_context=request_context,
        )

    def generate_project_report(
        self,
        *,
        project: models.Project,
        actor: str,
        role: str,
        actor_trusted: bool,
        execute_publish: bool = False,
        idempotency_key: str | None = None,
    ) -> ProjectReportResponse:
        request_context = self._build_request_context(
            project_id=project.id,
            actor=actor,
            role=role,
            actor_trusted=actor_trusted,
            execute_publish=execute_publish,
        )

        replayed = self.replay_existing_publish_if_available(
            project_id=project.id,
            actor=actor,
            role=role,
            actor_trusted=actor_trusted,
            idempotency_key=idempotency_key,
            execute_publish=execute_publish,
        )
        if replayed is not None:
            return replayed

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
            reserved_publish_record: models.AuditEvent | None = None
            if idempotency_key:
                reserved_publish_record = models.AuditEvent(
                    project_id=project.id,
                    actor=actor,
                    action="publish_report",
                    target_type="report",
                    target_id=publish_target,
                    payload=json.dumps(
                        self._build_publish_audit_payload(
                            actor=actor,
                            visibility=visibility,
                            publish_target=publish_target,
                            status="pending",
                            detail="report publish execution pending",
                            request_context=request_context,
                            report_core=report_core,
                            replay={"kind": "pending"},
                        ),
                        ensure_ascii=False,
                    ),
                    result="pending_publish",
                    idempotency_key=idempotency_key,
                    created_at=datetime.utcnow(),
                )
                self.db.add(reserved_publish_record)
                try:
                    self.db.commit()
                    self.db.refresh(reserved_publish_record)
                except IntegrityError:
                    self.db.rollback()
                    existing = self._wait_for_existing_publish_record(
                        idempotency_key,
                        wait_for_stable_result=True,
                    )
                    if existing:
                        return self._replay_existing_publish(
                            existing=existing,
                            request_context=request_context,
                        )
                    raise

            try:
                integration_result = self.notion.execute(action="publish_report", payload=publish_payload)
            except IntegrationError as exc:
                publish_detail = str(exc)
                self._persist_publish_audit(
                    project=project,
                    actor=actor,
                    visibility=visibility,
                    publish_target=publish_target,
                    status="failed",
                    detail=publish_detail,
                    request_context=request_context,
                    report_core=report_core,
                    replay={
                        "kind": "error",
                        "detail": publish_detail,
                    },
                    idempotency_key=idempotency_key,
                    existing_record=reserved_publish_record,
                )
                if idempotency_key:
                    self.db.commit()
                else:
                    self.db.flush()
                raise
            except Exception as exc:
                if idempotency_key:
                    publish_detail = str(exc) or type(exc).__name__
                    self._persist_publish_audit(
                        project=project,
                        actor=actor,
                        visibility=visibility,
                        publish_target=publish_target,
                        status="failed",
                        detail=publish_detail,
                        request_context=request_context,
                        report_core=report_core,
                        replay={
                            "kind": "error",
                            "detail": publish_detail,
                        },
                        idempotency_key=idempotency_key,
                        existing_record=reserved_publish_record,
                    )
                    self.db.commit()
                raise

            publish_status = "executed" if integration_result.ok else "failed"
            publish_detail = integration_result.detail or "notion publish_report execution failed"
            publish = ReportPublishResult(
                integration="notion",
                action="publish_report",
                status=publish_status,
                target=publish_target,
                detail=publish_detail,
                visibility=visibility,
            )
            response = ProjectReportResponse(
                **report_core,
                publish=publish,
            )
            self._persist_publish_audit(
                project=project,
                actor=actor,
                visibility=visibility,
                publish_target=publish_target,
                status=publish_status,
                detail=publish_detail,
                request_context=request_context,
                report_core=report_core,
                replay={
                    "kind": "response",
                    "response": response.model_dump(mode="json"),
                },
                idempotency_key=idempotency_key,
                existing_record=reserved_publish_record,
            )
            if idempotency_key:
                self.db.commit()
            else:
                self.db.flush()
            return response

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
        report_audit_events = []
        for event in audit_events:
            payload = self._load_payload(event.payload)
            if event.action == "publish_report":
                continue
            if payload.get("created_via") == "direct_mutation_api":
                continue
            report_audit_events.append(event)

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
            integration_detail = payload.get("integration", {}).get("detail")
            event_detail = payload.get("detail")

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
                        summary=policy_reason or integration_detail or event_detail or event.action,
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

    def _persist_publish_audit(
        self,
        *,
        project: models.Project,
        actor: str,
        visibility: str,
        publish_target: str,
        status: str,
        detail: str,
        request_context: dict,
        report_core: dict,
        replay: dict,
        idempotency_key: str | None,
        existing_record: models.AuditEvent | None,
    ) -> None:
        payload = self._build_publish_audit_payload(
            actor=actor,
            visibility=visibility,
            publish_target=publish_target,
            status=status,
            detail=detail,
            request_context=request_context,
            report_core=report_core,
            replay=replay,
        )
        if existing_record is not None:
            existing_record.payload = json.dumps(payload, ensure_ascii=False)
            existing_record.result = status
            self.db.flush()
            return

        self.db.add(
            models.AuditEvent(
                project_id=project.id,
                actor=actor,
                action="publish_report",
                target_type="report",
                target_id=publish_target,
                payload=json.dumps(payload, ensure_ascii=False),
                result=status,
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow(),
            )
        )

    def _build_publish_audit_payload(
        self,
        *,
        actor: str,
        visibility: str,
        publish_target: str,
        status: str,
        detail: str,
        request_context: dict,
        report_core: dict,
        replay: dict,
    ) -> dict:
        return {
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
            "report": report_core,
            "idempotency": {
                "request": request_context,
                "replay": replay,
            },
        }

    def _wait_for_existing_publish_record(
        self,
        idempotency_key: str | None,
        *,
        attempts: int | None = None,
        delay_seconds: float | None = None,
        wait_for_stable_result: bool = False,
    ) -> models.AuditEvent | None:
        if not idempotency_key:
            return None

        if attempts is None:
            attempts = self.PENDING_PUBLISH_WAIT_ATTEMPTS
        if delay_seconds is None:
            delay_seconds = self.PENDING_PUBLISH_WAIT_DELAY_SECONDS

        existing: models.AuditEvent | None = None
        for attempt in range(attempts):
            existing = self.db.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one_or_none()
            if existing and (
                not wait_for_stable_result
                or existing.result != "pending_publish"
                or self._is_stale_pending_publish(existing)
            ):
                return existing
            if attempt < attempts - 1:
                time.sleep(delay_seconds)
                self.db.expire_all()
        return existing

    def _is_stale_pending_publish(self, existing: models.AuditEvent) -> bool:
        return (
            existing.result == "pending_publish"
            and existing.created_at is not None
            and datetime.utcnow() - existing.created_at >= self.PENDING_PUBLISH_STALE_AFTER
        )

    def _mark_stale_pending_publish_failed(
        self,
        *,
        existing: models.AuditEvent,
        request_context: dict,
    ) -> models.AuditEvent:
        detail = "stale pending publish request requires manual reconciliation before retry"
        payload = self._load_payload(existing.payload)
        payload["integration"] = {
            **(payload.get("integration") or {}),
            "name": "notion",
            "action": "publish_report",
            "status": "failed",
            "detail": detail,
        }
        payload["visibility"] = payload.get("visibility") or "internal"
        payload["target"] = payload.get("target") or existing.target_id
        payload["created_via"] = payload.get("created_via") or "project_report"
        payload["actor"] = payload.get("actor") or existing.actor
        payload["report"] = payload.get("report") or {"project_id": existing.project_id}
        payload["idempotency"] = {
            "request": request_context,
            "replay": {"kind": "error", "detail": detail},
        }
        existing.payload = json.dumps(payload, ensure_ascii=False)
        existing.result = "failed"
        self.db.flush()
        self.db.commit()
        self.db.refresh(existing)
        return existing

    def _replay_existing_publish(
        self,
        *,
        existing: models.AuditEvent,
        request_context: dict,
    ) -> ProjectReportResponse:
        existing_payload = self._load_payload(existing.payload)
        existing_context = existing_payload.get("idempotency", {}).get("request")
        if existing.action != "publish_report" or existing_context != request_context:
            raise ReportIdempotencyConflictError("idempotency key already used for different request context")

        if existing.result == "pending_publish" and self._is_stale_pending_publish(existing):
            existing = self._mark_stale_pending_publish_failed(
                existing=existing,
                request_context=request_context,
            )
            existing_payload = self._load_payload(existing.payload)

        replay = existing_payload.get("idempotency", {}).get("replay") or {}
        if replay.get("kind") == "error":
            raise IntegrationError(
                str(replay.get("detail") or existing_payload.get("integration", {}).get("detail") or "stored publish failed")
            )
        if replay.get("kind") == "response" and isinstance(replay.get("response"), dict):
            return ProjectReportResponse(**replay["response"])
        if existing.result == "pending_publish":
            raise ReportIdempotencyConflictError("idempotent publish request still pending execution")
        raise ReportIdempotencyConflictError("idempotency key already used for unreadable stored publish replay")

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
