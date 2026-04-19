from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError
from sqlalchemy.orm import Session

from ..database import get_db_session
from ..schemas import GoalCreate, ProjectCreate
from ..services.onboarding_service import (
    DEFAULT_COMMUNICATION_INTEGRATIONS,
    DEFAULT_EMPLOYEE_CAPACITY_UNITS,
    InitialGoalInput,
    OnboardingExecutionInput,
    TrackerCredentialInput,
    employee_rows_to_team_entries,
    execute_project_onboarding,
)
from ..services.tracker_credentials import normalize_string_map


router = APIRouter(tags=["ui"], include_in_schema=False)
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))

BOARD_INTEGRATIONS = ("yandex_tracker",)
REPORTING_CADENCES = ("daily", "weekly", "manual")

TRACKER_FIELD_SETS = {
    "yandex_tracker": {
        "config": {"org_id", "queue"},
        "secrets": {"token"},
    },
    "jira": {
        "config": {"base_url", "project_key", "account_email"},
        "secrets": {"api_token"},
    },
    "trello": {
        "config": {"board_id", "list_id"},
        "secrets": {"api_key", "token"},
    },
    "notion": {
        "config": {"workspace_id", "target_id"},
        "secrets": {"integration_token"},
    },
}


@router.get("/", response_class=HTMLResponse)
async def onboarding_page(request: Request) -> HTMLResponse:
    return _render_page(request, form_state=_default_form_state(), status_code=status.HTTP_200_OK)


@router.post("/", response_class=HTMLResponse)
async def submit_onboarding_page(request: Request, db: Session = Depends(get_db_session)) -> HTMLResponse:
    form = await request.form()
    form_state = _build_form_state(form)
    try:
        project = ProjectCreate(
            name=form_state["name"],
            slug=form_state["slug"],
            description=form_state["description"] or None,
            timezone=form_state["timezone"] or None,
            commitment_due_at=_parse_optional_datetime(form_state["commitment_due_at"]),
            created_by=form_state["admin"] or None,
            visibility="internal",
            safe_paused=False,
            metadata={},
        )
        boss = _required_text(form_state["boss"], label="Boss / owner")
        admin = _required_text(form_state["admin"], label="Admin / operator")
        reporting_cadence = _allowed_value(form_state["reporting_cadence"], REPORTING_CADENCES, label="Reporting cadence")
        board_integration = _allowed_value(form_state["board_integration"], BOARD_INTEGRATIONS, label="Board integration")
        employee_rows = _validated_employee_rows(form_state["employees"])
        tracker_credentials = _build_tracker_credentials(board_integration, form_state["tracker"])
        initial_goal = _build_initial_goal(form_state["goal"])

        result = execute_project_onboarding(
            db,
            payload=OnboardingExecutionInput(
                name=project.name,
                slug=project.slug,
                description=project.description,
                timezone=project.timezone,
                commitment_due_at=project.commitment_due_at,
                created_by=project.created_by,
                visibility=project.visibility,
                boss=boss,
                admin=admin,
                reporting_cadence=reporting_cadence,
                communication_integrations=list(DEFAULT_COMMUNICATION_INTEGRATIONS),
                board_integration=board_integration,
                team=employee_rows_to_team_entries(employee_rows),
                metadata={},
                tracker_credentials=tracker_credentials,
                employee_rows=[
                    {
                        "name": row["name"],
                        "function": row["function"],
                        "capacity_hours": DEFAULT_EMPLOYEE_CAPACITY_UNITS,
                    }
                    for row in employee_rows
                ],
                initial_goal=initial_goal,
            ),
        )
        return _render_page(
            request,
            form_state=_default_form_state(),
            status_code=status.HTTP_201_CREATED,
            success_summary={
                "project_id": result.project.id,
                "project_name": result.project.name,
                "project_slug": result.project.slug,
                "board_integration": board_integration,
                "reporting_cadence": reporting_cadence,
                "initial_goal_title": result.initial_goal.title if result.initial_goal is not None else None,
                "launch_due_action_channel": result.launch_due_action.channel if result.launch_due_action is not None else None,
                "launch_due_action_recipient": result.launch_due_action.recipient if result.launch_due_action is not None else None,
            },
        )
    except ValidationError as exc:
        return _render_error_page(
            request,
            form_state=form_state,
            errors=_validation_errors(exc),
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )
    except ValueError as exc:
        return _render_error_page(
            request,
            form_state=form_state,
            errors=[str(exc)],
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )
    except HTTPException as exc:
        return _render_error_page(
            request,
            form_state=form_state,
            errors=_http_exception_errors(exc),
            status_code=exc.status_code,
        )


def _render_page(
    request: Request,
    *,
    form_state: dict,
    status_code: int,
    errors: list[str] | None = None,
    success_summary: dict | None = None,
) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "onboarding.html",
        {
            "board_integrations": BOARD_INTEGRATIONS,
            "default_employee_capacity_units": DEFAULT_EMPLOYEE_CAPACITY_UNITS,
            "errors": errors or [],
            "form": form_state,
            "reporting_cadences": REPORTING_CADENCES,
            "success_summary": success_summary,
        },
        status_code=status_code,
    )


def _render_error_page(
    request: Request,
    *,
    form_state: dict,
    errors: list[str],
    status_code: int,
) -> HTMLResponse:
    return _render_page(
        request,
        form_state=_redacted_form_state(form_state),
        errors=errors,
        status_code=status_code,
    )


def _redacted_form_state(form_state: dict) -> dict:
    redacted = copy.deepcopy(form_state)
    tracker_state = redacted.get("tracker")
    if isinstance(tracker_state, dict):
        for provider, field_sets in TRACKER_FIELD_SETS.items():
            provider_state = tracker_state.get(provider)
            if not isinstance(provider_state, dict):
                continue
            for secret_field in field_sets["secrets"]:
                provider_state[secret_field] = ""
    return redacted


def _http_exception_errors(exc: HTTPException) -> list[str]:
    detail = exc.detail
    if isinstance(detail, str):
        return [detail]
    if isinstance(detail, list):
        errors = [str(item) for item in detail if str(item).strip()]
        return errors or ["Submission failed."]
    if isinstance(detail, dict):
        detail_text = detail.get("detail")
        if isinstance(detail_text, str) and detail_text.strip():
            return [detail_text]
        return [str(detail)]
    return [str(detail) if detail is not None else "Submission failed."]


def _default_form_state() -> dict:
    return {
        "name": "",
        "slug": "",
        "description": "",
        "timezone": "UTC",
        "commitment_due_at": "",
        "boss": "",
        "admin": "",
        "board_integration": "yandex_tracker",
        "reporting_cadence": "weekly",
        "employees": [{"name": "", "function": ""}],
        "tracker": {provider: {} for provider in BOARD_INTEGRATIONS},
        "goal": {
            "title": "",
            "description": "",
            "commitment_due_at": "",
            "auto_decompose": False,
            "max_generated_tasks": "3",
        },
    }


def _build_form_state(form) -> dict:
    state = _default_form_state()
    state.update(
        {
            "name": str(form.get("name", "")).strip(),
            "slug": str(form.get("slug", "")).strip(),
            "description": str(form.get("description", "")).strip(),
            "timezone": str(form.get("timezone", "")).strip(),
            "commitment_due_at": str(form.get("commitment_due_at", "")).strip(),
            "boss": str(form.get("boss", "")).strip(),
            "admin": str(form.get("admin", "")).strip(),
            "board_integration": str(form.get("board_integration", "yandex_tracker")).strip() or "yandex_tracker",
            "reporting_cadence": str(form.get("reporting_cadence", "weekly")).strip() or "weekly",
            "employees": _employee_rows_from_form(form),
            "tracker": {
                provider: {
                    field_name: str(form.get(f"{provider}_{field_name}", "")).strip()
                    for field_name in TRACKER_FIELD_SETS[provider]["config"] | TRACKER_FIELD_SETS[provider]["secrets"]
                }
                for provider in BOARD_INTEGRATIONS
            },
            "goal": {
                "title": str(form.get("goal_title", "")).strip(),
                "description": str(form.get("goal_description", "")).strip(),
                "commitment_due_at": str(form.get("goal_commitment_due_at", "")).strip(),
                "auto_decompose": str(form.get("goal_auto_decompose", "")).strip().lower() == "on",
                "max_generated_tasks": str(form.get("goal_max_generated_tasks", "3")).strip() or "3",
            },
        }
    )
    return state


def _employee_rows_from_form(form) -> list[dict[str, str]]:
    names = [str(value).strip() for value in form.getlist("employee_name")]
    functions = [str(value).strip() for value in form.getlist("employee_function")]
    row_count = max(len(names), len(functions), 1)
    rows: list[dict[str, str]] = []
    for index in range(row_count):
        rows.append(
            {
                "name": names[index] if index < len(names) else "",
                "function": functions[index] if index < len(functions) else "",
            }
        )
    return rows


def _validated_employee_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    filtered_rows = [row for row in rows if row["name"] or row["function"]]
    if not filtered_rows:
        raise ValueError("At least one employee row is required.")
    for row in filtered_rows:
        if not row["name"] or not row["function"]:
            raise ValueError("Each employee row requires both employee name and function.")
    return filtered_rows


def _build_tracker_credentials(provider: str, tracker_state: dict[str, dict[str, str]]) -> TrackerCredentialInput:
    field_set = TRACKER_FIELD_SETS[provider]
    raw_values = tracker_state.get(provider, {})
    config = normalize_string_map(raw_values, allowed_keys=field_set["config"])
    secrets = normalize_string_map(raw_values, allowed_keys=field_set["secrets"])
    missing_fields = sorted((field_set["config"] - set(config)) | (field_set["secrets"] - set(secrets)))
    if missing_fields:
        raise ValueError(f"Missing required {provider} fields: {', '.join(missing_fields)}")
    return TrackerCredentialInput(provider=provider, config=config, secrets=secrets)


def _build_initial_goal(goal_state: dict[str, str | bool]) -> InitialGoalInput | None:
    title = str(goal_state["title"]).strip()
    if not title:
        return None
    goal = GoalCreate(
        title=title,
        description=str(goal_state["description"]).strip() or None,
        status="active",
        commitment_due_at=_parse_optional_datetime(str(goal_state["commitment_due_at"])),
        auto_decompose=bool(goal_state["auto_decompose"]),
        max_generated_tasks=int(str(goal_state["max_generated_tasks"])),
        tasks=[],
    )
    return InitialGoalInput(
        title=goal.title,
        description=goal.description,
        commitment_due_at=goal.commitment_due_at,
        auto_decompose=goal.auto_decompose,
        max_generated_tasks=goal.max_generated_tasks,
    )


def _required_text(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} is required.")
    return normalized


def _allowed_value(value: str, allowed_values: tuple[str, ...], *, label: str) -> str:
    normalized = value.strip().lower()
    if normalized not in allowed_values:
        raise ValueError(f"{label} must be one of: {', '.join(allowed_values)}")
    return normalized


def _parse_optional_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return datetime.fromisoformat(normalized)


def _validation_errors(exc: ValidationError) -> list[str]:
    errors: list[str] = []
    for error in exc.errors():
        field_path = " -> ".join(str(part) for part in error["loc"])
        errors.append(f"{field_path}: {error['msg']}")
    return errors
