"""Persistent entities; specifications and runtime state are separate columns."""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import CheckConstraint, DDL, DateTime, ForeignKey, Index, JSON, String, event, text
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


class Worker(Base):
    __tablename__ = "workers"

    id: Mapped[int] = mapped_column(primary_key=True)
    identity: Mapped[str] = mapped_column(String(100), unique=True)
    registered_at: Mapped[datetime] = mapped_column(UTCDateTime())
    last_heartbeat_at: Mapped[datetime] = mapped_column(UTCDateTime())


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint("kind = 'prepare_deployment'", name="valid_job_kind"),
        CheckConstraint("state IN ('queued', 'assigned', 'claimed', 'completed', 'failed')",
                        name="valid_job_state"),
        CheckConstraint("""
            (state = 'queued' AND worker_id IS NULL AND assigned_at IS NULL
                AND claim_id IS NULL AND claimed_at IS NULL AND finished_at IS NULL)
            OR (state = 'assigned' AND worker_id IS NOT NULL AND assigned_at IS NOT NULL
                AND claim_id IS NULL AND claimed_at IS NULL AND finished_at IS NULL)
            OR (state = 'claimed' AND worker_id IS NOT NULL AND assigned_at IS NOT NULL
                AND claim_id IS NOT NULL AND claimed_at IS NOT NULL AND finished_at IS NULL)
            OR (state IN ('completed', 'failed') AND worker_id IS NOT NULL
                AND assigned_at IS NOT NULL AND claim_id IS NOT NULL
                AND claimed_at IS NOT NULL AND finished_at IS NOT NULL)
        """, name="coherent_job_ownership"),
        CheckConstraint("error IS NULL OR state = 'failed'", name="job_error_only_on_failure"),
        CheckConstraint("state != 'failed' OR error IS NOT NULL", name="failed_job_has_error"),
        CheckConstraint("result IS NULL OR state = 'completed'", name="job_result_only_on_completion"),
        Index("one_active_job_per_worker", "worker_id", unique=True,
              sqlite_where=text("state IN ('assigned', 'claimed')")),
        Index("jobs_queue_order", "state", "created_at", "id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    deployment_id: Mapped[int] = mapped_column(ForeignKey("deployments.id"), unique=True)
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


event.listen(
    Job.__table__, "after_create", DDL("""
        CREATE TRIGGER jobs_valid_transition
        BEFORE UPDATE OF state ON jobs
        WHEN NEW.state IS NOT OLD.state AND NOT (
            (OLD.state = 'queued' AND NEW.state = 'assigned') OR
            (OLD.state = 'assigned' AND NEW.state = 'claimed') OR
            (OLD.state = 'claimed' AND NEW.state IN ('completed', 'failed'))
        )
        BEGIN
            SELECT RAISE(ABORT, 'invalid job state transition');
        END
    """).execute_if(dialect="sqlite"),
)


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
