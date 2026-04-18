from __future__ import annotations

import importlib
import sys
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def api_client(tmp_path):
    db_path = tmp_path / f"bro_pm_mvp_e2e_{uuid4().hex}.db"
    if db_path.exists():
        db_path.unlink()
    db_url = f"sqlite:///{db_path}"

    for mod_name in (
        "bro_pm.database",
        "bro_pm.api.app",
        "bro_pm.api",
        "bro_pm.api.v1",
        "bro_pm.api.v1.commands",
        "bro_pm.api.v1.projects",
    ):
        sys.modules.pop(mod_name, None)

    api_app = importlib.import_module("bro_pm.api.app")
    with TestClient(api_app.create_app(database_url=db_url)) as client:
        yield client



def _onboarding_payload() -> dict:
    return {
        "name": "Project Nova",
        "slug": f"project-nova-{uuid4().hex[:8]}",
        "description": "project under onboarding-to-report test",
        "timezone": "UTC",
        "boss": "olga",
        "admin": "alice",
        "reporting_cadence": "weekly",
        "communication_integrations": ["slack"],
        "board_integration": "notion",
        "team": [
            {
                "name": "operations",
                "owner": "alice",
                "capacity": 3,
            }
        ],
    }



def _goal_payload() -> dict:
    return {
        "title": "Deliver first onboarding milestone",
        "description": "Decompose the onboarded project into executable work",
        "status": "active",
        "tasks": [
            {
                "title": "Design onboarding plan",
                "description": "Document the first execution plan",
                "status": "todo",
                "priority": "high",
            },
            {
                "title": "Confirm owners",
                "description": "Assign ownership for the first milestone",
                "status": "done",
                "priority": "medium",
            },
        ],
    }


def _mutation_auth_params(*, actor: str = "alice", role: str = "admin") -> dict:
    return {
        "actor": actor,
        "role": role,
    }


def _mutation_auth_headers(*, trusted: bool = True) -> dict[str, str]:
    return {"x-actor-trusted": "true"} if trusted else {}



def _report_payload() -> dict:
    return {
        "actor": "alice",
        "role": "admin",
    }



def test_api_mvp_e2e_onboarding_goal_decomposition_and_project_report(api_client: TestClient):
    onboarding_response = api_client.post("/api/v1/projects/onboard", json=_onboarding_payload())

    assert onboarding_response.status_code == 201
    onboarding = onboarding_response.json()
    project = onboarding["project"]
    assert onboarding["status"] == "active"
    assert onboarding["smoke_check"]["detail"] == "notion executed: create_task"

    goal_response = api_client.post(
        f"/api/v1/projects/{project['id']}/goals",
        params=_mutation_auth_params(),
        headers=_mutation_auth_headers(),
        json=_goal_payload(),
    )

    assert goal_response.status_code == 201
    goal = goal_response.json()
    assert goal["project_id"] == project["id"]
    assert goal["status"] == "active"

    goal_tasks_by_title = {task["title"]: task for task in goal["tasks"]}
    assert set(goal_tasks_by_title) == {"Design onboarding plan", "Confirm owners"}
    assert goal_tasks_by_title["Design onboarding plan"]["status"] == "todo"
    assert goal_tasks_by_title["Confirm owners"]["status"] == "done"

    tasks_response = api_client.get(f"/api/v1/projects/{project['id']}/tasks")

    assert tasks_response.status_code == 200
    tasks = tasks_response.json()
    assert len(tasks) == 2
    assert {task["project_id"] for task in tasks} == {project["id"]}
    assert {task["goal_id"] for task in tasks} == {goal["id"]}
    tasks_by_title = {task["title"]: task for task in tasks}
    assert set(tasks_by_title) == set(goal_tasks_by_title)
    assert tasks_by_title["Design onboarding plan"]["status"] == "todo"
    assert tasks_by_title["Confirm owners"]["status"] == "done"

    report_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_payload(),
    )

    assert report_response.status_code == 200
    report = report_response.json()
    assert report["project_id"] == project["id"]
    assert report["report_type"] == "project_report"
    assert report["visibility"] == "internal"
    assert "Project Nova" in report["summary"]
    assert "Deliver first onboarding milestone" in report["summary"]
    assert "1 open tasks" in report["summary"]
    assert report["kpis"] == {
        "total_tasks": 2,
        "completed_tasks": 1,
        "open_tasks": 1,
        "active_goals": 1,
        "audit_events": 1,
    }
    assert report["risks"] == []
    assert report["decisions"] == [
        {
            "audit_id": report["action_ids"][0],
            "action": "onboard_project",
            "result": "executed",
            "summary": "notion executed: create_task",
            "reason": "notion executed: create_task",
            "mode": None,
            "trace_label": None,
            "lineage": "mode=unknown -> audit=onboard_project:executed",
        }
    ]
    assert report["publish"]["status"] == "contract_ready"
    assert report["publish"]["action"] == "publish_report"
    assert report["publish"]["target"].endswith(project["slug"])
