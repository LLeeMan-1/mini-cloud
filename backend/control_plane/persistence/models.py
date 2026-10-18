"""Persistent entities; specifications and runtime state are separate columns."""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import CheckConstraint, DDL, DateTime, ForeignKey, JSON, String, event
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
