from __future__ import annotations

from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from .config import settings
from .models import Base


_ENGINE_OPTIONS = {"echo": False, "future": True}

_engine = create_engine(settings.database_url, **_ENGINE_OPTIONS)
SessionLocal = sessionmaker(bind=_engine, class_=Session, autocommit=False, autoflush=False, future=True)



def _initialize_engine(database_url: str) -> None:
    """(Re)initialize the SQLAlchemy engine and session factory for tests/runtime."""

    global _engine
    global SessionLocal

    _engine.dispose()
    _engine = create_engine(database_url, **_ENGINE_OPTIONS)
    SessionLocal.configure(bind=_engine)


def init_db(database_url: str | None = None) -> None:
    """Initialize or reinitialize DB schema for a concrete database URL."""

    if database_url is not None and database_url != settings.database_url:
        _initialize_engine(database_url)
    Base.metadata.create_all(bind=_engine)


def get_db_session() -> Iterator[Session]:
    """FastAPI dependency: yields a SQLAlchemy session with transaction handling."""

    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class Database:
    """Compatibility wrapper for non-HTTP usage paths."""

    def __init__(self, session_factory: sessionmaker | None = None) -> None:
        self.session_factory = session_factory or SessionLocal

    def session(self) -> Session:
        return self.session_factory()


# create schema on import so local smoke tests and CLI usage work immediately
init_db()
