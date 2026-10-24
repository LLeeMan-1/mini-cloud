"""Persistent entities; specifications and runtime state are separate columns."""

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import Boolean, CheckConstraint, DDL, DateTime, ForeignKey, ForeignKeyConstraint, Index, JSON, String, UniqueConstraint, event, text
from sqlalchemy.types import TypeDecorator
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Application(Base):
    __tablename__ = "applications"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(default=utc_now)
    configuration: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class Deployment(Base):
    __tablename__ = "deployments"
    __table_args__ = (
        CheckConstraint(
            "runtime_status IN ('pending', 'starting', 'healthy', 'failed', 'stopped')",
            name="valid_runtime_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    application_id: Mapped[int] = mapped_column(ForeignKey("applications.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(default=utc_now)
    specification: Mapped[dict[str, Any]] = mapped_column(JSON)
    runtime_status: Mapped[str] = mapped_column(String(20), default="pending")


class UTCDateTime(TypeDecorator):
    """SQLite stores UTC without an offset; Python always receives aware UTC."""

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Timestamp must be timezone-aware")
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    def process_result_value(self, value, dialect):
        return value.replace(tzinfo=timezone.utc) if value is not None else None


class DeploymentTarget(Base):
    __tablename__ = "deployment_targets"
    __table_args__ = (
        CheckConstraint("typeof(desired_replicas) = 'integer' AND desired_replicas BETWEEN 0 AND 100",
                        name="bounded_replica_target"),
        CheckConstraint("version > 0", name="positive_target_version"),
    )

    deployment_id: Mapped[int] = mapped_column(ForeignKey("deployments.id"), primary_key=True)
    desired_replicas: Mapped[int] = mapped_column(default=1)
    version: Mapped[int] = mapped_column(default=1)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)


event.listen(DeploymentTarget.__table__, "after_create", DDL("""
    CREATE TRIGGER targets_valid_update BEFORE UPDATE ON deployment_targets
    WHEN NEW.deployment_id != OLD.deployment_id OR NEW.created_at != OLD.created_at
      OR NEW.version != OLD.version + 1 OR NEW.desired_replicas <= OLD.desired_replicas
    BEGIN SELECT RAISE(ABORT, 'invalid replica target mutation'); END
""").execute_if(dialect="sqlite"))


class Worker(Base):
    __tablename__ = "workers"

    id: Mapped[int] = mapped_column(primary_key=True)
    identity: Mapped[str] = mapped_column(String(100), unique=True)
    registered_at: Mapped[datetime] = mapped_column(UTCDateTime())
    last_heartbeat_at: Mapped[datetime] = mapped_column(UTCDateTime())
    capabilities: Mapped[list[str]] = mapped_column(
        JSON, default=lambda: ["prepare_deployment"], server_default=text("'[\"prepare_deployment\"]'")
    )


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint("kind IN ('prepare_deployment', 'start_instance')", name="valid_job_kind"),
        Index("one_prepare_job_per_deployment", "deployment_id", unique=True,
              sqlite_where=text("kind = 'prepare_deployment'")),
        CheckConstraint("state IN ('queued', 'assigned', 'claimed', 'completed', 'failed')",
                        name="valid_job_state"),
        CheckConstraint("""
            (state = 'queued' AND worker_id IS NULL AND assigned_at IS NULL
                AND claim_id IS NULL AND claimed_at IS NULL AND finished_at IS NULL)
            OR (state = 'assigned' AND worker_id IS NOT NULL AND assigned_at IS NOT NULL
                AND claim_id IS NULL AND claimed_at IS NULL AND finished_at IS NULL)
            OR (state = 'claimed' AND worker_id IS NOT NULL AND assigned_at IS NOT NULL
                AND claim_id IS NOT NULL AND claimed_at IS NOT NULL AND finished_at IS NULL)
            OR (state = 'completed' AND worker_id IS NOT NULL
                AND assigned_at IS NOT NULL AND claim_id IS NOT NULL
                AND claimed_at IS NOT NULL AND finished_at IS NOT NULL)
            OR (state = 'failed' AND finished_at IS NOT NULL)
        """, name="coherent_job_ownership"),
        CheckConstraint("attempt_count >= 0 AND max_attempts > 0 AND attempt_count <= max_attempts",
                        name="bounded_job_attempts"),
        CheckConstraint("""
            (state = 'assigned' AND assignment_expires_at IS NOT NULL AND lease_expires_at IS NULL)
            OR (state = 'claimed' AND assignment_expires_at IS NULL AND lease_expires_at IS NOT NULL)
            OR (state IN ('queued', 'completed', 'failed') AND assignment_expires_at IS NULL AND lease_expires_at IS NULL)
        """, name="coherent_job_deadlines"),
        CheckConstraint("error IS NULL OR state = 'failed'", name="job_error_only_on_failure"),
        CheckConstraint("state != 'failed' OR error IS NOT NULL", name="failed_job_has_error"),
        CheckConstraint("result IS NULL OR state = 'completed'", name="job_result_only_on_completion"),
        Index("one_active_job_per_worker", "worker_id", unique=True,
              sqlite_where=text("state IN ('assigned', 'claimed')")),
        Index("jobs_queue_order", "state", "created_at", "id"),
        Index("jobs_ready", "state", "next_run_at"),
        Index("jobs_assignment_expiry", "state", "assignment_expires_at"),
        Index("jobs_lease_expiry", "state", "lease_expires_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    deployment_id: Mapped[int] = mapped_column(ForeignKey("deployments.id"))
    worker_id: Mapped[int | None] = mapped_column(ForeignKey("workers.id"), default=None)
    kind: Mapped[str] = mapped_column(String(30), default="prepare_deployment")
    state: Mapped[str] = mapped_column(String(20), default="queued")
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    assigned_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    claimed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    claim_id: Mapped[str | None] = mapped_column(String(36), unique=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON(none_as_null=True))
    error: Mapped[str | None] = mapped_column(String(2000))
    attempt_count: Mapped[int] = mapped_column(default=0)
    max_attempts: Mapped[int] = mapped_column(default=3)
    next_run_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    assignment_expires_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    lease_expires_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    last_failure_code: Mapped[str | None] = mapped_column(String(50))


class JobAttempt(Base):
    __tablename__ = "job_attempts"
    __table_args__ = (
        UniqueConstraint("job_id", "number", name="unique_job_attempt"),
        CheckConstraint("number > 0", name="positive_attempt_number"),
        CheckConstraint("state IN ('assigned','claimed','completed','failed','expired')", name="valid_attempt_state"),
        CheckConstraint("""
            (state = 'assigned' AND claim_id IS NULL AND claimed_at IS NULL AND finished_at IS NULL)
            OR (state = 'claimed' AND claim_id IS NOT NULL AND claimed_at IS NOT NULL AND finished_at IS NULL)
            OR (state IN ('completed','failed') AND claim_id IS NOT NULL AND claimed_at IS NOT NULL AND finished_at IS NOT NULL)
            OR (state = 'expired' AND finished_at IS NOT NULL)
        """, name="coherent_attempt_state"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"), index=True)
    number: Mapped[int]
    worker_id: Mapped[int] = mapped_column(ForeignKey("workers.id"))
    state: Mapped[str] = mapped_column(String(20))
    claim_id: Mapped[str | None] = mapped_column(String(36), unique=True)
    assigned_at: Mapped[datetime] = mapped_column(UTCDateTime())
    claimed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    failure_code: Mapped[str | None] = mapped_column(String(50))
    retryable: Mapped[bool] = mapped_column(Boolean, default=False)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON(none_as_null=True))
    error: Mapped[str | None] = mapped_column(String(2000))
    report_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON(none_as_null=True))
    report_ack: Mapped[dict[str, Any] | None] = mapped_column(JSON(none_as_null=True))


class JobRenewal(Base):
    """Durable receipts prevent delayed renewal retries extending leases again."""
    __tablename__ = "job_renewals"
    __table_args__ = (UniqueConstraint("attempt_id", "renewal_id"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    attempt_id: Mapped[int] = mapped_column(ForeignKey("job_attempts.id"), index=True)
    renewal_id: Mapped[str] = mapped_column(String(36))
    granted_at: Mapped[datetime] = mapped_column(UTCDateTime())
    lease_expires_at: Mapped[datetime] = mapped_column(UTCDateTime())


class ReplicaSlot(Base):
    __tablename__ = "replica_slots"
    __table_args__ = (
        ForeignKeyConstraint(["deployment_id", "replica_index", "current_generation"],
                             ["instances.deployment_id", "instances.replica_index", "instances.generation"],
                             deferrable=True, initially="DEFERRED", name="current_slot_incarnation"),
        CheckConstraint("typeof(replica_index)='integer' AND replica_index >= 0", name="slot_index_valid"),
        CheckConstraint("typeof(current_generation)='integer' AND current_generation > 0", name="slot_generation_valid"),
        CheckConstraint("version > 0", name="slot_version_positive"),
        CheckConstraint("replacement_limit = 1 AND automatic_replacements BETWEEN 0 AND replacement_limit",
                        name="bounded_automatic_replacements"),
    )

    deployment_id: Mapped[int] = mapped_column(ForeignKey("deployments.id"), primary_key=True)
    replica_index: Mapped[int] = mapped_column(primary_key=True)
    current_generation: Mapped[int] = mapped_column(default=1)
    version: Mapped[int] = mapped_column(default=1)
    automatic_replacements: Mapped[int] = mapped_column(default=0)
    replacement_limit: Mapped[int] = mapped_column(default=1)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)


event.listen(ReplicaSlot.__table__, "after_create", DDL("""
    CREATE TRIGGER slots_valid_update BEFORE UPDATE ON replica_slots
    WHEN NEW.deployment_id != OLD.deployment_id OR NEW.replica_index != OLD.replica_index
      OR NEW.created_at != OLD.created_at OR NEW.replacement_limit != OLD.replacement_limit
      OR NEW.version != OLD.version + 1 OR NEW.current_generation != OLD.current_generation + 1
      OR NEW.automatic_replacements NOT IN (OLD.automatic_replacements, OLD.automatic_replacements + 1)
    BEGIN SELECT RAISE(ABORT, 'invalid slot mutation'); END
""").execute_if(dialect="sqlite"))


class Instance(Base):
    __tablename__ = "instances"
    __table_args__ = (
        UniqueConstraint("deployment_id", "replica_index", "generation", name="unique_replica_generation"),
        ForeignKeyConstraint(["deployment_id", "replica_index"],
                             ["replica_slots.deployment_id", "replica_slots.replica_index"]),
        CheckConstraint("typeof(generation)='integer' AND generation > 0", name="valid_instance_generation"),
        CheckConstraint("creation_reason IN ('initial','automatic_replacement','administrative_replacement')",
                        name="valid_creation_reason"),
        CheckConstraint("typeof(replica_index) = 'integer' AND replica_index >= 0", name="valid_replica_index"),
        CheckConstraint("state IN ('pending','starting','healthy','failed','stopped')", name="valid_instance_state"),
        CheckConstraint("version > 0", name="positive_instance_version"),
        CheckConstraint("state_source IN ('control_plane','worker_report','manual')", name="valid_instance_source"),
        CheckConstraint("state != 'healthy' OR (worker_id IS NOT NULL AND last_attempt_id IS NOT NULL AND healthy_at IS NOT NULL AND last_observed_at IS NOT NULL)", name="healthy_instance_observed"),
        CheckConstraint("(state = 'stopped') = (stopped_at IS NOT NULL)", name="coherent_instance_stop"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    deployment_id: Mapped[int] = mapped_column(ForeignKey("deployments.id"))
    replica_index: Mapped[int] = mapped_column(default=0)
    generation: Mapped[int] = mapped_column(default=1)
    creation_reason: Mapped[str] = mapped_column(String(40), default="initial")
    start_job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"), unique=True)
    worker_id: Mapped[int | None] = mapped_column(ForeignKey("workers.id"))
    last_attempt_id: Mapped[int | None] = mapped_column(ForeignKey("job_attempts.id"))
    state: Mapped[str] = mapped_column(String(20), default="pending")
    version: Mapped[int] = mapped_column(default=1)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    last_observed_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    healthy_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    stopped_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    state_source: Mapped[str] = mapped_column(String(20), default="control_plane")
    state_reason: Mapped[str] = mapped_column(String(100), default="created")


event.listen(Instance.__table__, "after_create", DDL("""
    CREATE TRIGGER instances_valid_transition BEFORE UPDATE ON instances
    WHEN NEW.id != OLD.id OR NEW.deployment_id != OLD.deployment_id OR NEW.start_job_id != OLD.start_job_id
      OR NEW.replica_index != OLD.replica_index
      OR NEW.generation != OLD.generation OR NEW.creation_reason != OLD.creation_reason
      OR NEW.created_at != OLD.created_at OR NEW.version != OLD.version + 1
      OR (NEW.state != OLD.state AND NOT (
        (OLD.state='pending' AND NEW.state IN ('starting','failed')) OR
        (OLD.state='starting' AND NEW.state IN ('healthy','failed')) OR
        (OLD.state='failed' AND NEW.state IN ('starting','stopped')) OR
        (OLD.state='healthy' AND NEW.state='stopped')
      )) OR OLD.state='stopped'
    BEGIN SELECT RAISE(ABORT, 'invalid instance mutation'); END
""").execute_if(dialect="sqlite"))


class ReplacementReceipt(Base):
    __tablename__ = "replacement_receipts"

    request_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    previous_instance_id: Mapped[str] = mapped_column(ForeignKey("instances.id"))
    new_instance_id: Mapped[str] = mapped_column(ForeignKey("instances.id"), unique=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    acknowledgment: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)


event.listen(ReplacementReceipt.__table__, "after_create", DDL("""
    CREATE TRIGGER replacement_receipts_immutable BEFORE UPDATE ON replacement_receipts
    BEGIN SELECT RAISE(ABORT, 'replacement receipt is immutable'); END
""").execute_if(dialect="sqlite"))

event.listen(ReplacementReceipt.__table__, "after_create", DDL("""
    CREATE TRIGGER replacement_receipts_no_delete BEFORE DELETE ON replacement_receipts
    BEGIN SELECT RAISE(ABORT, 'replacement receipt is immutable'); END
""").execute_if(dialect="sqlite"))


event.listen(
    Job.__table__, "after_create", DDL("""
        CREATE TRIGGER jobs_valid_transition
        BEFORE UPDATE OF state ON jobs
        WHEN NEW.state IS NOT OLD.state AND NOT (
            (OLD.state = 'queued' AND NEW.state = 'assigned') OR
            (OLD.state = 'assigned' AND NEW.state IN ('claimed', 'queued', 'failed')) OR
            (OLD.state = 'claimed' AND NEW.state IN ('completed', 'failed', 'queued'))
        )
        BEGIN
            SELECT RAISE(ABORT, 'invalid job state transition');
        END
    """).execute_if(dialect="sqlite"),
)

event.listen(JobAttempt.__table__, "after_create", DDL("""
    CREATE TRIGGER attempts_valid_transition BEFORE UPDATE ON job_attempts
    WHEN NEW.job_id != OLD.job_id OR NEW.number != OLD.number OR NEW.worker_id != OLD.worker_id
      OR NEW.assigned_at != OLD.assigned_at
      OR (OLD.claim_id IS NOT NULL AND NEW.claim_id IS NOT OLD.claim_id)
      OR (OLD.state IN ('completed','failed','expired'))
      OR (NEW.state != OLD.state AND NOT (
        (OLD.state='assigned' AND NEW.state IN ('claimed','expired')) OR
        (OLD.state='claimed' AND NEW.state IN ('completed','failed','expired'))
      ))
    BEGIN SELECT RAISE(ABORT, 'invalid attempt mutation'); END
""").execute_if(dialect="sqlite"))


# Enforce immutability even for direct SQL updates, while allowing status updates.
event.listen(
    Deployment.__table__,
    "after_create",
    DDL("""
        CREATE TRIGGER deployments_specification_immutable
        BEFORE UPDATE OF specification ON deployments
        WHEN NEW.specification IS NOT OLD.specification
        BEGIN
            SELECT RAISE(ABORT, 'deployment specification is immutable');
        END
    """).execute_if(dialect="sqlite"),
)
