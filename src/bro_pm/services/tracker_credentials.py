from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any

from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from .. import models

TRACKER_SECRET_REDACTION = "[redacted]"


@dataclass(frozen=True)
class StoredTrackerCredentials:
    provider: str
    config: dict[str, str]
    secrets: dict[str, str]


def normalize_string_map(raw_value: Any, *, allowed_keys: set[str]) -> dict[str, str]:
    if not isinstance(raw_value, dict):
        return {}
    normalized: dict[str, str] = {}
    for key in allowed_keys:
        value = raw_value.get(key)
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                normalized[key] = stripped
    return normalized


def upsert_tracker_credentials(
    db: Session,
    *,
    project_id: str,
    provider: str,
    config: dict[str, str],
    secrets: dict[str, str],
) -> models.TrackerCredential:
    credential = (
        db.query(models.TrackerCredential)
        .filter_by(project_id=project_id, provider=provider)
        .one_or_none()
    )
    if credential is None:
        credential = models.TrackerCredential(project_id=project_id, provider=provider)
        db.add(credential)

    credential.config_json = dict(config)
    credential.secret_json = {key: TRACKER_SECRET_REDACTION for key in secrets}
    db.flush()
    return credential


def load_tracker_credentials(
    *,
    project_id: str,
    provider: str,
    session: Session | None = None,
) -> StoredTrackerCredentials | None:
    owns_session = session is None
    database_module = importlib.import_module("bro_pm.database")
    db = session or database_module.SessionLocal()
    try:
        try:
            credential = (
                db.query(models.TrackerCredential)
                .filter_by(project_id=project_id, provider=provider)
                .one_or_none()
            )
        except OperationalError:
            return None
        if credential is None:
            return None
        return StoredTrackerCredentials(
            provider=credential.provider,
            config=normalize_string_map(credential.config_json, allowed_keys=set((credential.config_json or {}).keys())),
            secrets=normalize_string_map(credential.secret_json, allowed_keys=set((credential.secret_json or {}).keys())),
        )
    finally:
        if owns_session:
            db.close()
