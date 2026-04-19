from __future__ import annotations

import io
import json
from urllib.error import HTTPError
from uuid import uuid4

import pytest

from bro_pm import models
from bro_pm.config import Settings
from bro_pm.database import init_db
from bro_pm.integrations import IntegrationError, IntegrationResult, YandexTrackerIntegration


class _FakeHTTPResponse:
    def __init__(self, payload: dict, *, status: int = 201):
        self.status = status
        self._payload = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> _FakeHTTPResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def _settings(**overrides) -> Settings:
    values = {
        "yandex_tracker_api_base": "https://tracker.example.test/v2",
        "yandex_tracker_backend": "native",
        "yandex_tracker_token": "secret-token",
        "yandex_tracker_auth_prefix": "OAuth",
        "yandex_tracker_org_header_name": "X-Org-ID",
        "yandex_tracker_org_id": "org-77",
        "yandex_tracker_default_queue": "BROPM",
        "yandex_tracker_mcp_command": "uvx",
        "yandex_tracker_mcp_args_json": '["tracker-mcp"]',
        "yandex_tracker_mcp_env_json": '{"YANDEX_TRACKER_TOKEN": "secret-token"}',
        "yandex_tracker_mcp_cwd": "/tmp/yandex-tracker-mcp",
        "yandex_tracker_mcp_tool_name": "issue_create",
        "yandex_tracker_mcp_timeout_seconds": 45,
    }
    values.update(overrides)
    return Settings(**values)


@pytest.mark.parametrize(
    ("settings_overrides", "payload", "expected_message"),
    [
        ({"yandex_tracker_token": None}, {"project_id": "project-1", "title": "Ship feature"}, "missing yandex_tracker token"),
        ({"yandex_tracker_org_id": None}, {"project_id": "project-1", "title": "Ship feature"}, "missing yandex_tracker org id"),
        (
            {"yandex_tracker_default_queue": None},
            {"project_id": "project-1", "title": "Ship feature"},
            "missing yandex_tracker queue",
        ),
    ],
)
def test_yandex_tracker_validate_requires_token_org_and_queue(settings_overrides, payload, expected_message):
    integration = YandexTrackerIntegration(settings=_settings(**settings_overrides))

    with pytest.raises(IntegrationError, match=expected_message):
        integration.validate(action="create_task", payload=payload)


def test_yandex_tracker_execute_constructs_request_headers_and_normalizes_success():
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout: int = 0):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["headers"] = {key.lower(): value for key, value in request.header_items()}
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _FakeHTTPResponse({"id": "42", "key": "BROPM-42"})

    integration = YandexTrackerIntegration(settings=_settings(), urlopen=fake_urlopen)

    result = integration.execute(
        action="create_task",
        payload={
            "project_id": "project-1",
            "title": "Sync release checklist",
            "description": "Prepare the release train.",
            "project_metadata": {
                "integrations": {
                    "yandex_tracker": {
                        "queue": "OPS",
                    }
                }
            },
        },
    )

    assert captured["url"] == "https://tracker.example.test/v2/issues/"
    assert captured["timeout"] == 30
    assert captured["headers"] == {
        "authorization": "OAuth secret-token",
        "content-type": "application/json; charset=utf-8",
        "accept": "application/json",
        "x-org-id": "org-77",
    }
    assert captured["body"] == {
        "queue": "OPS",
        "summary": "Sync release checklist",
        "description": "Prepare the release train.",
    }
    assert result.ok is True
    assert result.detail == "yandex_tracker created task BROPM-42 (id: 42)"
    assert result.metadata == {
        "issue_key": "BROPM-42",
        "issue_id": "42",
        "queue": "OPS",
    }


def test_yandex_tracker_execute_wraps_http_failures_as_integration_error():
    def fake_urlopen(request, timeout: int = 0):
        raise HTTPError(
            url=request.full_url,
            code=403,
            msg="Forbidden",
            hdrs=None,
            fp=io.BytesIO(b'{"errors": ["quota exceeded"]}'),
        )

    integration = YandexTrackerIntegration(settings=_settings(), urlopen=fake_urlopen)

    with pytest.raises(IntegrationError, match="yandex_tracker create_task failed with HTTP 403: quota exceeded"):
        integration.execute(
            action="create_task",
            payload={
                "project_id": "project-1",
                "title": "Sync release checklist",
            },
        )


def test_yandex_tracker_execute_loads_non_secret_config_from_store_and_uses_runtime_token(tmp_path):
    db_path = tmp_path / "yandex_tracker_credentials.db"
    db_url = f"sqlite:///{db_path}"
    init_db(db_url)
    from bro_pm.database import SessionLocal

    session = SessionLocal()
    project_id = ""
    try:
        project = models.Project(
            name=f"Tracker Project {uuid4().hex[:8]}",
            slug=f"tracker-project-{uuid4().hex[:8]}",
            safe_paused=False,
            visibility="internal",
        )
        session.add(project)
        session.flush()
        project_id = project.id
        session.add(
            models.TrackerCredential(
                project_id=project_id,
                provider="yandex_tracker",
                config_json={"org_id": "org-from-db", "queue": "OPS"},
                secret_json={"token": "token-from-db"},
            )
        )
        session.commit()
    finally:
        session.close()

    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout: int = 0):
        captured["headers"] = {key.lower(): value for key, value in request.header_items()}
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _FakeHTTPResponse({"id": "42", "key": "OPS-42"})

    integration = YandexTrackerIntegration(
        settings=_settings(yandex_tracker_token="runtime-token", yandex_tracker_org_id=None, yandex_tracker_default_queue=None),
        urlopen=fake_urlopen,
    )

    result = integration.execute(
        action="create_task",
        payload={
            "project_id": project_id,
            "title": "Stored credential task",
        },
    )

    assert captured["headers"]["authorization"] == "OAuth runtime-token"
    assert captured["headers"]["x-org-id"] == "org-from-db"
    assert captured["body"] == {
        "queue": "OPS",
        "summary": "Stored credential task",
    }
    assert result.detail == "yandex_tracker created task OPS-42 (id: 42)"


def test_yandex_tracker_execute_uses_runtime_token_when_stored_secret_is_masked(tmp_path):
    db_path = tmp_path / "yandex_tracker_masked_credentials.db"
    db_url = f"sqlite:///{db_path}"
    init_db(db_url)
    from bro_pm.database import SessionLocal

    session = SessionLocal()
    project_id = ""
    try:
        project = models.Project(
            name=f"Tracker Project {uuid4().hex[:8]}",
            slug=f"tracker-project-masked-{uuid4().hex[:8]}",
            safe_paused=False,
            visibility="internal",
        )
        session.add(project)
        session.flush()
        project_id = project.id
        session.add(
            models.TrackerCredential(
                project_id=project_id,
                provider="yandex_tracker",
                config_json={"org_id": "org-from-db", "queue": "OPS"},
                secret_json={"token": "[redacted]"},
            )
        )
        session.commit()
    finally:
        session.close()

    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout: int = 0):
        captured["headers"] = {key.lower(): value for key, value in request.header_items()}
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _FakeHTTPResponse({"id": "42", "key": "OPS-42"})

    integration = YandexTrackerIntegration(
        settings=_settings(yandex_tracker_token="runtime-token", yandex_tracker_org_id=None, yandex_tracker_default_queue=None),
        urlopen=fake_urlopen,
    )

    result = integration.execute(
        action="create_task",
        payload={
            "project_id": project_id,
            "title": "Stored masked credential task",
        },
    )

    assert captured["headers"]["authorization"] == "OAuth runtime-token"
    assert captured["headers"]["x-org-id"] == "org-from-db"
    assert captured["body"] == {
        "queue": "OPS",
        "summary": "Stored masked credential task",
    }
    assert result.detail == "yandex_tracker created task OPS-42 (id: 42)"


def test_yandex_tracker_execute_uses_native_backend_by_default():
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout: int = 0):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _FakeHTTPResponse({"id": "42", "key": "BROPM-42"})

    def forbidden_mcp_runner(**kwargs):
        raise AssertionError("mcp runner must not be used when backend defaults to native")

    integration = YandexTrackerIntegration(
        settings=_settings(),
        urlopen=fake_urlopen,
        mcp_tool_runner=forbidden_mcp_runner,
    )

    result = integration.execute(
        action="create_task",
        payload={
            "project_id": "project-1",
            "title": "Default native backend",
        },
    )

    assert captured["url"] == "https://tracker.example.test/v2/issues/"
    assert captured["timeout"] == 30
    assert captured["body"] == {
        "queue": "BROPM",
        "summary": "Default native backend",
    }
    assert result.ok is True
    assert result.detail == "yandex_tracker created task BROPM-42 (id: 42)"


@pytest.mark.parametrize(
    ("settings_overrides", "expected_message"),
    [
        ({"yandex_tracker_mcp_command": None, "yandex_tracker_backend": "mcp"}, "missing yandex_tracker MCP command"),
        ({"yandex_tracker_mcp_tool_name": None, "yandex_tracker_backend": "mcp"}, "missing yandex_tracker MCP tool name"),
    ],
)
def test_yandex_tracker_validate_requires_mcp_command_and_tool_name(settings_overrides, expected_message):
    integration = YandexTrackerIntegration(settings=_settings(**settings_overrides))

    with pytest.raises(IntegrationError, match=expected_message):
        integration.validate(
            action="create_task",
            payload={
                "project_id": "project-1",
                "title": "Ship feature",
            },
        )


def test_yandex_tracker_execute_project_metadata_can_override_backend_to_mcp_and_queue():
    captured: dict[str, object] = {}

    def forbidden_urlopen(request, timeout: int = 0):
        raise AssertionError("native HTTP path must not run when project metadata selects MCP backend")

    def fake_mcp_runner(**kwargs):
        captured.update(kwargs)
        return {
            "isError": False,
            "structuredContent": {"id": "77", "key": "OPS-77"},
            "content": [{"type": "text", "text": "created issue OPS-77"}],
        }

    integration = YandexTrackerIntegration(
        settings=_settings(),
        urlopen=forbidden_urlopen,
        mcp_tool_runner=fake_mcp_runner,
    )

    result = integration.execute(
        action="create_task",
        payload={
            "project_id": "project-1",
            "title": "MCP override task",
            "description": "Use stdio MCP instead of HTTP.",
            "project_metadata": {
                "integrations": {
                    "yandex_tracker": {
                        "backend": "mcp",
                        "queue": "OPS",
                    }
                }
            },
        },
    )

    assert captured == {
        "command": "uvx",
        "args": ["tracker-mcp"],
        "env": {"YANDEX_TRACKER_TOKEN": "secret-token"},
        "cwd": "/tmp/yandex-tracker-mcp",
        "tool_name": "issue_create",
        "tool_arguments": {
            "queue": "OPS",
            "summary": "MCP override task",
            "description": "Use stdio MCP instead of HTTP.",
        },
        "timeout_seconds": 45,
    }
    assert result.ok is True
    assert result.detail == "yandex_tracker created task OPS-77 (id: 77)"
    assert result.metadata == {
        "issue_key": "OPS-77",
        "issue_id": "77",
        "queue": "OPS",
    }


def test_yandex_tracker_execute_mcp_normalizes_success_from_structured_content():
    integration = YandexTrackerIntegration(
        settings=_settings(yandex_tracker_backend="mcp"),
        mcp_tool_runner=lambda **kwargs: {
            "isError": False,
            "structuredContent": {"issueKey": "OPS-88", "issueId": "88"},
            "content": [{"type": "text", "text": "created issue OPS-88"}],
        },
    )

    result = integration.execute(
        action="create_task",
        payload={
            "project_id": "project-1",
            "title": "Normalize MCP success",
            "queue": "OPS",
        },
    )

    assert result.ok is True
    assert result.detail == "yandex_tracker created task OPS-88 (id: 88)"
    assert result.metadata == {
        "issue_key": "OPS-88",
        "issue_id": "88",
        "queue": "OPS",
    }


def test_yandex_tracker_execute_mcp_maps_error_result_to_integration_error():
    integration = YandexTrackerIntegration(
        settings=_settings(yandex_tracker_backend="mcp"),
        mcp_tool_runner=lambda **kwargs: {
            "isError": True,
            "content": [{"type": "text", "text": "quota exceeded"}],
        },
    )

    with pytest.raises(IntegrationError, match="yandex_tracker create_task failed via MCP: quota exceeded"):
        integration.execute(
            action="create_task",
            payload={
                "project_id": "project-1",
                "title": "Fail MCP execution",
                "queue": "OPS",
            },
        )


def test_yandex_tracker_verify_action_result_fetches_issue_state_and_confirms_summary():
    requests: list[tuple[str, str, dict[str, str]]] = []

    def fake_urlopen(request, timeout: int = 0):
        requests.append((request.get_method(), request.full_url, {key.lower(): value for key, value in request.header_items()}))
        if request.get_method() == "POST":
            return _FakeHTTPResponse({"id": "42", "key": "OPS-42"})
        if request.get_method() == "GET":
            return _FakeHTTPResponse({"id": "42", "key": "OPS-42", "summary": "Verify tracker task", "queue": "OPS"}, status=200)
        raise AssertionError(f"unexpected method {request.get_method()}")

    integration = YandexTrackerIntegration(settings=_settings(), urlopen=fake_urlopen)
    payload = {
        "project_id": "project-1",
        "title": "Verify tracker task",
        "queue": "OPS",
    }

    result = integration.execute(action="create_task", payload=payload)
    verification = integration.verify_action_result(action="create_task", payload=payload, result=result)

    assert integration.supports_verification(action="create_task", payload=payload) is True
    assert verification.ok is True
    assert verification.metadata["issue_key"] == "OPS-42"
    assert verification.metadata["state"] == {
        "exists": True,
        "issue_key": "OPS-42",
        "issue_id": "42",
        "summary": "Verify tracker task",
        "queue": "OPS",
    }
    assert requests[0][0:2] == ("POST", "https://tracker.example.test/v2/issues/")
    assert requests[1][0:2] == ("GET", "https://tracker.example.test/v2/issues/OPS-42")


def test_yandex_tracker_fetch_state_returns_missing_for_404():
    def fake_urlopen(request, timeout: int = 0):
        if request.get_method() == "GET":
            raise HTTPError(url=request.full_url, code=404, msg="Not found", hdrs=None, fp=io.BytesIO(b'{"error": "not found"}'))
        raise AssertionError("fetch_state should only issue GET requests")

    integration = YandexTrackerIntegration(settings=_settings(), urlopen=fake_urlopen)
    payload = {
        "project_id": "project-1",
        "title": "Missing tracker task",
        "queue": "OPS",
    }
    result = IntegrationResult(ok=True, detail="yandex_tracker created task OPS-404", metadata={"issue_key": "OPS-404", "issue_id": "404", "queue": "OPS"})

    state = integration.fetch_state(action="create_task", payload=payload, result=result)
    verification = integration.verify_action_result(action="create_task", payload=payload, result=result)

    assert state == {
        "exists": False,
        "issue_key": "OPS-404",
        "issue_id": "404",
        "queue": "OPS",
    }
    assert verification.ok is False
    assert verification.detail == "yandex_tracker create_task verification failed: issue not found"


def test_yandex_tracker_verify_action_result_fails_when_remote_queue_is_missing():
    requests: list[str] = []

    def fake_urlopen(request, timeout: int = 0):
        requests.append(request.get_method())
        if request.get_method() == "POST":
            return _FakeHTTPResponse({"id": "42", "key": "OPS-42"})
        if request.get_method() == "GET":
            return _FakeHTTPResponse({"id": "42", "key": "OPS-42", "summary": "Verify tracker task"}, status=200)
        raise AssertionError(f"unexpected method {request.get_method()}")

    integration = YandexTrackerIntegration(settings=_settings(), urlopen=fake_urlopen)
    payload = {
        "project_id": "project-1",
        "title": "Verify tracker task",
        "queue": "OPS",
    }

    result = integration.execute(action="create_task", payload=payload)
    verification = integration.verify_action_result(action="create_task", payload=payload, result=result)

    assert requests == ["POST", "GET"]
    assert verification.ok is False
    assert verification.detail == "yandex_tracker create_task verification failed: queue missing from fetched state"
