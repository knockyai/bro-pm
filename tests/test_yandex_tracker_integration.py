from __future__ import annotations

import io
import json
from urllib.error import HTTPError

import pytest

from bro_pm.config import Settings
from bro_pm.integrations import IntegrationError, YandexTrackerIntegration


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
        "yandex_tracker_token": "secret-token",
        "yandex_tracker_auth_prefix": "OAuth",
        "yandex_tracker_org_header_name": "X-Org-ID",
        "yandex_tracker_org_id": "org-77",
        "yandex_tracker_default_queue": "BROPM",
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
