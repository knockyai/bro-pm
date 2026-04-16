from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .v1.commands import router as command_router
from .v1.projects import router as project_router
from ..database import init_db
from ..config import settings


def create_app(*, database_url: str | None = None) -> FastAPI:
    """Create a minimally wired FastAPI app instance for API use and tests."""

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        init_db(database_url)
        yield

    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.include_router(project_router, prefix="/api/v1")
    app.include_router(command_router, prefix="/api/v1")
    return app


app = create_app()
