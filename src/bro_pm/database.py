from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from .config import settings
from .models import Base


# Lazily initialized singleton to keep connection configuration stable.
_engine = create_engine(settings.database_url, echo=False, future=True)
SessionLocal = sessionmaker(bind=_engine, class_=Session, autocommit=False, autoflush=False, future=True)


# create schema on import so local smoke tests can run immediately
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

    def __init__(self) -> None:
        self.session_factory = SessionLocal

    def session(self) -> Session:
        return self.session_factory()
