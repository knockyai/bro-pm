from __future__ import annotations

from asyncio import Task
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ..config import settings
from ..database import SessionLocal, init_db
from ..services.report_scheduler import start_polling_task, stop_polling_task
from .v1.commands import router as command_router
from .v1.projects import router as project_router


def _start_report_scheduler_task(*, poll_interval_seconds: float, session_factory) -> Task[None]:
    return start_polling_task(session_factory=session_factory, poll_interval_seconds=poll_interval_seconds)


def create_app(
    *,
    database_url: str | None = None,
    enable_scheduler: bool | None = None,
    scheduler_poll_interval_seconds: float | None = None,
) -> FastAPI:
    """Create a minimally wired FastAPI app instance for API use and tests."""

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        init_db(database_url)
        scheduler_task: Task[None] | None = None
        if enable_scheduler is None:
            scheduler_enabled = settings.timer_actions_enabled and database_url is None
        else:
            scheduler_enabled = enable_scheduler
        if scheduler_enabled:
            scheduler_task = _start_report_scheduler_task(
                poll_interval_seconds=(
                    settings.timer_actions_poll_interval_seconds
                    if scheduler_poll_interval_seconds is None
                    else scheduler_poll_interval_seconds
                ),
                session_factory=SessionLocal,
            )
        yield
        await stop_polling_task(scheduler_task)

    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.include_router(project_router, prefix="/api/v1")
    app.include_router(command_router, prefix="/api/v1")
    return app


app = create_app(enable_scheduler=settings.timer_actions_enabled)
