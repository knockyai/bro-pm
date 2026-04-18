from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from ..config import Settings, settings as runtime_settings
from ..services.tracker_credentials import load_tracker_credentials
from . import IntegrationError, IntegrationResult

McpToolRunner = Callable[..., dict[str, Any]]


@dataclass
class YandexTrackerIntegration:
    name: str = "yandex_tracker"
    settings: Settings = field(default_factory=lambda: runtime_settings)
    urlopen: Callable[..., Any] = field(default_factory=lambda: urllib.request.urlopen, repr=False)
    timeout_seconds: int = 30
    mcp_tool_runner: McpToolRunner = field(default_factory=lambda: _run_mcp_tool_stdio, repr=False)

    def validate(self, *, action: str, payload: dict) -> None:
        if action not in {"noop", "create_task", "close_task"}:
            raise IntegrationError(f"unsupported action for yandex_tracker: {action}")
        if action != "create_task":
            return
        if not self._normalized_text(payload.get("project_id")):
            raise IntegrationError("missing project_id for yandex_tracker create_task")
        if not self._normalized_text(payload.get("title")):
            raise IntegrationError("missing title for yandex_tracker create_task")

        backend = self._resolve_backend(payload)
        if backend == "native":
            self._validated_native_context(payload)
            return
        if backend == "mcp":
            self._validated_mcp_context(payload)
            return
        raise IntegrationError(f"unsupported yandex_tracker backend: {backend}")

    def execute(self, *, action: str, payload: dict) -> IntegrationResult:
        self.validate(action=action, payload=payload)
        if action != "create_task":
            return IntegrationResult(ok=True, detail=f"yandex_tracker executed: {action}")

        backend = self._resolve_backend(payload)
        if backend == "native":
            return self._execute_native(payload)
        if backend == "mcp":
            return self._execute_mcp(payload)
        raise IntegrationError(f"unsupported yandex_tracker backend: {backend}")

    def _execute_native(self, payload: dict) -> IntegrationResult:
        context = self._validated_native_context(payload)
        request_payload = {
            "queue": context["queue"],
            "summary": context["title"],
        }
        if context["description"]:
            request_payload["description"] = context["description"]

        request = urllib.request.Request(
            f"{context['api_base'].rstrip('/')}/issues/",
            data=json.dumps(request_payload).encode("utf-8"),
            headers={
                "Authorization": context["authorization"],
                context["org_header_name"]: context["org_id"],
                "Content-Type": "application/json; charset=utf-8",
                "Accept": "application/json",
            },
            method="POST",
        )

        try:
            with self.urlopen(request, timeout=self.timeout_seconds) as response:
                response_payload = self._load_json_bytes(response.read())
        except urllib.error.HTTPError as exc:
            error_detail = self._extract_error_detail(self._load_json_bytes(exc.read()), fallback=str(exc.reason or exc.msg))
            raise IntegrationError(f"yandex_tracker create_task failed with HTTP {exc.code}: {error_detail}") from exc
        except urllib.error.URLError as exc:
            raise IntegrationError(f"yandex_tracker create_task failed: {exc.reason}") from exc
        except OSError as exc:
            raise IntegrationError(f"yandex_tracker create_task failed: {exc}") from exc

        issue_key = self._extract_issue_key(response_payload)
        issue_id = self._extract_issue_id(response_payload)
        return IntegrationResult(
            ok=True,
            detail=self._success_detail(issue_key=issue_key, issue_id=issue_id),
            metadata=self._success_metadata(queue=context["queue"], issue_key=issue_key, issue_id=issue_id),
        )

    def _execute_mcp(self, payload: dict) -> IntegrationResult:
        context = self._validated_mcp_context(payload)
        tool_arguments = {
            "queue": context["queue"],
            "summary": context["title"],
        }
        if context["description"]:
            tool_arguments["description"] = context["description"]

        try:
            result = self.mcp_tool_runner(
                command=context["command"],
                args=context["args"],
                env=context["env"],
                cwd=context["cwd"],
                tool_name=context["tool_name"],
                tool_arguments=tool_arguments,
                timeout_seconds=context["timeout_seconds"],
            )
        except IntegrationError:
            raise
        except OSError as exc:
            raise IntegrationError(f"yandex_tracker create_task failed via MCP: {exc}") from exc
        except Exception as exc:  # pragma: no cover - defensive wrapper for SDK/runtime exceptions
            raise IntegrationError(f"yandex_tracker create_task failed via MCP: {exc}") from exc

        normalized_result = self._coerce_mapping(result)
        if normalized_result.get("isError"):
            detail = self._extract_mcp_error_detail(normalized_result)
            raise IntegrationError(f"yandex_tracker create_task failed via MCP: {detail}")

        issue_key = self._extract_issue_key(normalized_result)
        issue_id = self._extract_issue_id(normalized_result)
        return IntegrationResult(
            ok=True,
            detail=self._success_detail(issue_key=issue_key, issue_id=issue_id),
            metadata=self._success_metadata(queue=context["queue"], issue_key=issue_key, issue_id=issue_id),
        )

    def _validated_native_context(self, payload: dict) -> dict[str, str | None]:
        api_base = self._required_setting(
            self._credential_value(payload, config_key="api_base", fallback=self.settings.yandex_tracker_api_base),
            "missing yandex_tracker api base",
        )
        token = self._required_setting(
            self._credential_value(payload, secret_key="token", fallback=self.settings.yandex_tracker_token),
            "missing yandex_tracker token",
        )
        auth_prefix = self._required_setting(
            self._credential_value(payload, config_key="auth_prefix", fallback=self.settings.yandex_tracker_auth_prefix),
            "missing yandex_tracker auth prefix",
        )
        org_header_name = self._required_setting(
            self._credential_value(
                payload,
                config_key="org_header_name",
                fallback=self.settings.yandex_tracker_org_header_name,
            ),
            "missing yandex_tracker org header name",
        )
        org_id = self._required_setting(
            self._credential_value(payload, config_key="org_id", fallback=self.settings.yandex_tracker_org_id),
            "missing yandex_tracker org id",
        )
        common = self._validated_common_context(payload)
        return {
            "api_base": api_base,
            "authorization": f"{auth_prefix} {token}",
            "org_header_name": org_header_name,
            "org_id": org_id,
            **common,
        }

    def _validated_mcp_context(self, payload: dict) -> dict[str, Any]:
        command = self._required_setting(self.settings.yandex_tracker_mcp_command, "missing yandex_tracker MCP command")
        tool_name = self._required_setting(self.settings.yandex_tracker_mcp_tool_name, "missing yandex_tracker MCP tool name")
        timeout_seconds = self.settings.yandex_tracker_mcp_timeout_seconds
        if timeout_seconds <= 0:
            raise IntegrationError("invalid yandex_tracker MCP timeout seconds")
        common = self._validated_common_context(payload)
        return {
            "command": command,
            "args": self._parse_json_string_list(
                self.settings.yandex_tracker_mcp_args_json,
                setting_name="yandex_tracker MCP args JSON",
            ),
            "env": self._parse_json_string_mapping(
                self.settings.yandex_tracker_mcp_env_json,
                setting_name="yandex_tracker MCP env JSON",
            ),
            "cwd": self._normalized_text(self.settings.yandex_tracker_mcp_cwd),
            "tool_name": tool_name,
            "timeout_seconds": timeout_seconds,
            **common,
        }

    def _validated_common_context(self, payload: dict) -> dict[str, str | None]:
        queue = self._resolve_queue(payload)
        if not queue:
            raise IntegrationError("missing yandex_tracker queue")
        return {
            "queue": queue,
            "title": self._normalized_text(payload.get("title")),
            "description": self._normalized_text(payload.get("description")),
        }

    def _resolve_backend(self, payload: dict) -> str:
        project_metadata = payload.get("project_metadata")
        if isinstance(project_metadata, dict):
            integrations = project_metadata.get("integrations")
            if isinstance(integrations, dict):
                yandex_metadata = integrations.get("yandex_tracker")
                if isinstance(yandex_metadata, dict):
                    metadata_backend = self._normalized_text(yandex_metadata.get("backend"))
                    if metadata_backend:
                        return metadata_backend.lower()
        return self._normalized_text(self.settings.yandex_tracker_backend) or "native"

    def _resolve_queue(self, payload: dict) -> str | None:
        direct_queue = self._normalized_text(payload.get("queue"))
        if direct_queue:
            return direct_queue
        project_metadata = payload.get("project_metadata")
        if isinstance(project_metadata, dict):
            integrations = project_metadata.get("integrations")
            if isinstance(integrations, dict):
                yandex_metadata = integrations.get("yandex_tracker")
                if isinstance(yandex_metadata, dict):
                    metadata_queue = self._normalized_text(yandex_metadata.get("queue"))
                    if metadata_queue:
                        return metadata_queue
        credential_queue = self._credential_value(payload, config_key="queue")
        if credential_queue:
            return credential_queue
        return self._normalized_text(self.settings.yandex_tracker_default_queue)

    def _credential_value(
        self,
        payload: dict,
        *,
        config_key: str | None = None,
        secret_key: str | None = None,
        fallback: Any = None,
    ) -> str | None:
        direct_credentials = payload.get("tracker_credentials")
        if isinstance(direct_credentials, dict):
            if config_key:
                config = direct_credentials.get("config")
                if isinstance(config, dict):
                    value = self._normalized_text(config.get(config_key))
                    if value:
                        return value
            if secret_key:
                secrets = direct_credentials.get("secrets")
                if isinstance(secrets, dict):
                    value = self._normalized_text(secrets.get(secret_key))
                    if value:
                        return value

        project_id = self._normalized_text(payload.get("project_id"))
        if project_id:
            stored = load_tracker_credentials(project_id=project_id, provider="yandex_tracker")
            if stored is not None:
                if config_key:
                    value = self._normalized_text(stored.config.get(config_key))
                    if value:
                        return value

        return self._normalized_text(fallback)

    @staticmethod
    def _normalized_text(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        normalized = value.strip()
        return normalized or None

    def _required_setting(self, value: Any, error_message: str) -> str:
        normalized = self._normalized_text(value)
        if not normalized:
            raise IntegrationError(error_message)
        return normalized

    def _parse_json_string_list(self, value: str | None, *, setting_name: str) -> list[str]:
        if value is None:
            return []
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise IntegrationError(f"invalid {setting_name}: {exc.msg}") from exc
        if not isinstance(parsed, list) or any(not isinstance(item, str) for item in parsed):
            raise IntegrationError(f"invalid {setting_name}: expected JSON array of strings")
        return parsed

    def _parse_json_string_mapping(self, value: str | None, *, setting_name: str) -> dict[str, str] | None:
        if value is None:
            return None
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise IntegrationError(f"invalid {setting_name}: {exc.msg}") from exc
        if not isinstance(parsed, dict) or any(not isinstance(key, str) or not isinstance(item, str) for key, item in parsed.items()):
            raise IntegrationError(f"invalid {setting_name}: expected JSON object of string pairs")
        return parsed

    @staticmethod
    def _load_json_bytes(raw_value: bytes) -> dict[str, Any] | list[Any] | str:
        text = raw_value.decode("utf-8", errors="replace").strip()
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    @staticmethod
    def _extract_error_detail(payload: dict[str, Any] | list[Any] | str, *, fallback: str) -> str:
        if isinstance(payload, dict):
            errors = payload.get("errors")
            if isinstance(errors, list):
                parts = [str(item).strip() for item in errors if str(item).strip()]
                if parts:
                    return "; ".join(parts)
            for key in ("error", "message", "description"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        if isinstance(payload, str) and payload.strip():
            return payload.strip()
        return fallback.strip() or "request failed"

    def _extract_mcp_error_detail(self, payload: dict[str, Any]) -> str:
        structured = payload.get("structuredContent")
        if isinstance(structured, dict):
            detail = self._extract_error_detail(structured, fallback="")
            if detail:
                return detail
        content_detail = self._extract_text_content(payload.get("content"))
        if content_detail:
            return content_detail
        return "tool call reported error"

    def _extract_issue_key(self, payload: Any) -> str | None:
        return self._extract_nested_value(payload, ("issue_key", "issueKey", "key"))

    def _extract_issue_id(self, payload: Any) -> str | None:
        return self._extract_nested_value(payload, ("issue_id", "issueId", "id"))

    def _extract_nested_value(self, payload: Any, keys: tuple[str, ...]) -> str | None:
        mapping = self._coerce_mapping(payload)
        for key in keys:
            value = self._normalized_text(mapping.get(key))
            if value:
                return value
        structured = mapping.get("structuredContent")
        if isinstance(structured, dict):
            for key in keys:
                value = self._normalized_text(structured.get(key))
                if value:
                    return value
        text_content = self._extract_text_content(mapping.get("content"))
        if text_content:
            parsed = self._parse_json_text(text_content)
            if isinstance(parsed, dict):
                for key in keys:
                    value = self._normalized_text(parsed.get(key))
                    if value:
                        return value
        return None

    @staticmethod
    def _extract_text_content(content: Any) -> str | None:
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    text = item.get("text")
                    if isinstance(text, str) and text.strip():
                        parts.append(text.strip())
            if parts:
                return "\n".join(parts)
        return None

    @staticmethod
    def _parse_json_text(value: str) -> dict[str, Any] | list[Any] | str:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    @staticmethod
    def _coerce_mapping(payload: Any) -> dict[str, Any]:
        if isinstance(payload, dict):
            return payload
        model_dump = getattr(payload, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump(by_alias=True, exclude_none=True)
            if isinstance(dumped, dict):
                return dumped
        return {}

    @staticmethod
    def _success_metadata(*, queue: str, issue_key: str | None, issue_id: str | None) -> dict[str, str]:
        metadata = {"queue": queue}
        if issue_key:
            metadata["issue_key"] = issue_key
        if issue_id:
            metadata["issue_id"] = issue_id
        return metadata

    @staticmethod
    def _success_detail(*, issue_key: str | None, issue_id: str | None) -> str:
        if issue_key and issue_id:
            return f"yandex_tracker created task {issue_key} (id: {issue_id})"
        if issue_key:
            return f"yandex_tracker created task {issue_key}"
        if issue_id:
            return f"yandex_tracker created task id={issue_id}"
        return "yandex_tracker created task"


def _run_mcp_tool_stdio(
    *,
    command: str,
    args: list[str],
    env: dict[str, str] | None,
    cwd: str | None,
    tool_name: str,
    tool_arguments: dict[str, Any],
    timeout_seconds: int,
) -> dict[str, Any]:
    async def _call_tool() -> dict[str, Any]:
        server = StdioServerParameters(command=command, args=args, env=env, cwd=cwd)
        async with stdio_client(server) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                result = await session.call_tool(
                    tool_name,
                    arguments=tool_arguments,
                    read_timeout_seconds=timedelta(seconds=timeout_seconds),
                )
                return result.model_dump(by_alias=True, exclude_none=True)

    return asyncio.run(_call_tool())
