"""Validated request bodies and explicit response contracts."""

from datetime import datetime, timezone
import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, StringConstraints, field_validator, model_validator


ApplicationName = Annotated[
    str, StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=100)
]
JobKind = Literal["prepare_deployment", "start_instance"]
InstanceState = Literal["pending", "starting", "healthy", "failed", "stopped"]
ReplicaCount = Annotated[int, Field(strict=True, ge=0, le=100)]


class ApplicationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: ApplicationName
    configuration: dict[str, JsonValue] = Field(default_factory=dict)


class DeploymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    specification: dict[str, JsonValue] = Field(min_length=1)
    desired_replicas: ReplicaCount = 1


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
    capabilities: list[JobKind] = Field(default_factory=lambda: ["prepare_deployment"], max_length=2)


class WorkerRead(BaseModel):
    id: int
    identity: str
    registered_at: AwareDatetime
    last_heartbeat_at: AwareDatetime
    status: Literal["available", "unavailable"]
    capabilities: list[JobKind]


JobState = Literal["queued", "assigned", "claimed", "completed", "failed"]


class InstanceCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")


class InstanceStop(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: Annotated[int, Field(strict=True, gt=0)]


class InstanceRead(BaseModel):
    id: UUID
    deployment_id: int
    # Old durable report receipts omit this field. Do not insert it on replay.
    replica_index: Annotated[int, Field(ge=0)] | None = None
    start_job_id: int
    worker_id: int | None
    last_attempt_id: int | None
    state: InstanceState
    version: int
    created_at: AwareDatetime
    updated_at: AwareDatetime
    last_observed_at: AwareDatetime | None
    healthy_at: AwareDatetime | None
    stopped_at: AwareDatetime | None
    state_source: Literal["control_plane", "worker_report", "manual"]
    state_reason: str
    simulated: Literal[True]
    worker_available: bool | None
    startup_job_state: JobState
    current_worker_id: int | None
    current_attempt_number: int | None
    ownership_valid: bool


class InstanceOutcome(BaseModel):
    model_config = ConfigDict(extra="forbid")
    instance_id: UUID
    expected_version: Annotated[int, Field(strict=True, gt=0)]
    state: Literal["healthy", "failed"]
    phase: Literal["readiness_passed", "startup_failed", "readiness_failed"]

    @model_validator(mode="after")
    def valid_phase(self):
        if (self.state == "healthy") != (self.phase == "readiness_passed"):
            raise ValueError("Readiness success must report healthy; failure must report failed")
        return self


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    deployment_id: int
    worker_id: int | None
    kind: JobKind
    state: JobState
    created_at: AwareDatetime
    assigned_at: AwareDatetime | None
    claimed_at: AwareDatetime | None
    finished_at: AwareDatetime | None
    claim_id: UUID | None
    result: dict[str, JsonValue] | None
    error: str | None
    attempt_count: int
    max_attempts: int
    next_run_at: AwareDatetime
    assignment_expires_at: AwareDatetime | None
    lease_expires_at: AwareDatetime | None
    last_failure_code: str | None


class ClaimRead(JobRead):
    server_time: AwareDatetime
    renewal_interval: float
    instance: InstanceRead | None = None


class ReportRead(JobRead):
    acknowledged_attempt_number: int
    attempt_outcome: Literal["completed", "failed"]
    instance: InstanceRead | None = None


class AttemptRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    number: int
    worker_id: int
    state: Literal["assigned", "claimed", "completed", "failed", "expired"]
    claim_id: UUID | None
    assigned_at: AwareDatetime
    claimed_at: AwareDatetime | None
    finished_at: AwareDatetime | None
    failure_code: str | None
    retryable: bool
    result: dict[str, JsonValue] | None
    error: str | None


class JobAssignment(BaseModel):
    id: int
    deployment_id: int
    kind: JobKind
    state: Literal["assigned"]
    specification: dict[str, JsonValue]
    attempt_number: int
    assignment_expires_at: AwareDatetime
    instance_id: UUID | None = None
    instance_version: int | None = None


class JobClaim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_id: UUID
    attempt_number: Annotated[int, Field(strict=True, gt=0)]


class JobRenew(JobClaim):
    renewal_id: UUID


class RenewalRead(BaseModel):
    job_id: int
    worker_id: int
    attempt_number: int
    claim_id: UUID
    renewal_id: UUID
    lease_expires_at: AwareDatetime
    server_time: AwareDatetime
    renewal_interval: float


class JobReport(JobClaim):
    outcome: Literal["completed", "failed"]
    result: dict[str, JsonValue] | None = None
    retryable: bool = Field(default=False, strict=True)
    instance_outcome: InstanceOutcome | None = None
    error: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2000)] | None = None

    @model_validator(mode="after")
    def coherent_outcome(self):
        if self.instance_outcome is not None and (
            (self.outcome == "completed") != (self.instance_outcome.state == "healthy")
        ):
            raise ValueError("Job and instance outcomes must agree")
        if self.outcome == "completed" and self.error is not None:
            raise ValueError("Completed reports cannot contain an error")
        if self.outcome == "completed" and self.retryable:
            raise ValueError("Only failures may be retryable")
        if self.outcome == "failed" and (self.error is None or self.result is not None):
            raise ValueError("Failed reports require an error and cannot contain a result")
        if self.result is not None and len(json.dumps(self.result).encode("utf-8")) > 16384:
            raise ValueError("Result must fit within 16 KiB of JSON")
        return self


class SchedulingAssignment(BaseModel):
    job_id: int
    worker_id: int
    attempt_number: int


class RecoveryItem(BaseModel):
    job_id: int
    attempt_number: int
    state: JobState
    failure_code: str


class RecoveryResult(BaseModel):
    recovered_count: int
    recovered: list[RecoveryItem]


class SchedulingResult(BaseModel):
    assigned_count: int
    assignments: list[SchedulingAssignment]


class ReplicaTargetUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    desired_replicas: ReplicaCount
    expected_version: Annotated[int, Field(strict=True, gt=0)]


class ReplicaTargetRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    deployment_id: int
    desired_replicas: int
    version: int
    created_at: AwareDatetime
    updated_at: AwareDatetime


class ReplicaSlotRead(BaseModel):
    replica_index: int
    instance_id: UUID
    start_job_id: int


class BlockedReplica(ReplicaSlotRead):
    reason: Literal["stopped", "terminal_startup_failure"]


class ReplicaSummary(ReplicaTargetRead):
    materialized_replicas: int
    missing_indexes: list[int]
    lifecycle_counts: dict[InstanceState, int]
    retrying_replicas: int
    expired_ownership_replicas: int
    unavailable_worker_replicas: int
    blocked: list[BlockedReplica]


class ReconciliationRead(BaseModel):
    deployment_id: int
    target_version: int
    desired_replicas: int
    created_count: int
    created: list[ReplicaSlotRead]
    blocked: list[BlockedReplica]
