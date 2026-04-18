from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import Any
from urllib.parse import urlencode

import httpx
import fastapi.testclient
import starlette.testclient


class _AsyncCompatTestClient:
    __test__ = False
    def __init__(
        self,
        app,
        base_url: str = "http://testserver",
        headers: dict[str, str] | None = None,
        follow_redirects: bool = True,
        raise_server_exceptions: bool = True,
        **_: Any,
    ) -> None:
        self.app = app
        self.base_url = base_url
        self.headers = headers or {}
        self.follow_redirects = follow_redirects
        self.raise_server_exceptions = raise_server_exceptions
        self._runner: asyncio.Runner | None = None
        self._stack: AsyncExitStack | None = None
        self._client: httpx.AsyncClient | None = None

    def __enter__(self) -> "_AsyncCompatTestClient":
        self._ensure_started()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if self._runner is None or self._stack is None:
            return
        self._runner.run(self._stack.aclose())
        self._runner.close()
        self._runner = None
        self._stack = None
        self._client = None

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        self._ensure_started()
        assert self._runner is not None
        assert self._client is not None
        data = kwargs.pop("data", None)
        if data is not None:
            kwargs.setdefault("headers", {})
            kwargs["headers"] = dict(kwargs["headers"])
            kwargs["headers"].setdefault("content-type", "application/x-www-form-urlencoded")
            kwargs["content"] = urlencode(data, doseq=True)
        return self._runner.run(self._client.request(method, url, **kwargs))

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("PUT", url, **kwargs)

    def patch(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("PATCH", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("DELETE", url, **kwargs)

    def _ensure_started(self) -> None:
        if self._runner is not None:
            return
        self._runner = asyncio.Runner()
        self._stack = AsyncExitStack()
        self._runner.run(self._startup())

    async def _startup(self) -> None:
        assert self._stack is not None
        await self._stack.enter_async_context(self.app.router.lifespan_context(self.app))
        transport = httpx.ASGITransport(app=self.app, raise_app_exceptions=self.raise_server_exceptions)
        self._client = await self._stack.enter_async_context(
            httpx.AsyncClient(
                transport=transport,
                base_url=self.base_url,
                headers=self.headers,
                follow_redirects=self.follow_redirects,
            )
        )


fastapi.testclient.TestClient = _AsyncCompatTestClient
starlette.testclient.TestClient = _AsyncCompatTestClient
