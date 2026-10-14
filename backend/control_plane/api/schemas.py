"""Validated request bodies and explicit response contracts."""

from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints, field_validator


ApplicationName = Annotated[
    str, StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=100)
]


class ApplicationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: ApplicationName
    configuration: dict[str, JsonValue] = Field(default_factory=dict)


class DeploymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    specification: dict[str, JsonValue] = Field(min_length=1)


class StoredRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def restore_utc(cls, value: datetime) -> datetime:
        # SQLite stores naive timestamps; all timestamps are written as UTC.
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


class ApplicationRead(StoredRecord):
    name: str
    configuration: dict[str, JsonValue]


class DeploymentRead(StoredRecord):
    application_id: int
    specification: dict[str, JsonValue]
    runtime_status: Literal["pending", "starting", "healthy", "failed", "stopped"]
