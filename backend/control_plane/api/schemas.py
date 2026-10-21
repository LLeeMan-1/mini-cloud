"""Validated request bodies and explicit response contracts."""

from datetime import datetime, timezone
import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, StringConstraints, field_validator, model_validator


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


class WorkerRegister(BaseModel):
    model_config = ConfigDict(extra="forbid")

    identity: ApplicationName


class WorkerRead(BaseModel):
    id: int
    identity: str
    registered_at: AwareDatetime
    last_heartbeat_at: AwareDatetime
    status: Literal["available", "unavailable"]


JobState = Literal["queued", "assigned", "claimed", "completed", "failed"]


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    deployment_id: int
    worker_id: int | None
    kind: Literal["prepare_deployment"]
    state: JobState
    created_at: AwareDatetime
    assigned_at: AwareDatetime | None
    claimed_at: AwareDatetime | None
    finished_at: AwareDatetime | None
    claim_id: UUID | None
    result: dict[str, JsonValue] | None
    error: str | None


class JobAssignment(BaseModel):
    id: int
    deployment_id: int
    kind: Literal["prepare_deployment"]
    state: Literal["assigned"]
    specification: dict[str, JsonValue]


class JobClaim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_id: UUID


class JobReport(JobClaim):
    outcome: Literal["completed", "failed"]
    result: dict[str, JsonValue] | None = None
    error: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2000)] | None = None

    @model_validator(mode="after")
    def coherent_outcome(self):
        if self.outcome == "completed" and self.error is not None:
            raise ValueError("Completed reports cannot contain an error")
        if self.outcome == "failed" and (self.error is None or self.result is not None):
            raise ValueError("Failed reports require an error and cannot contain a result")
        if self.result is not None and len(json.dumps(self.result).encode("utf-8")) > 16384:
            raise ValueError("Result must fit within 16 KiB of JSON")
        return self


class SchedulingAssignment(BaseModel):
    job_id: int
    worker_id: int


class SchedulingResult(BaseModel):
    assigned_count: int
    assignments: list[SchedulingAssignment]
