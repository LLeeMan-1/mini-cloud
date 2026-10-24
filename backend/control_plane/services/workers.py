"""Worker presence operations; availability is derived without background writes."""

from datetime import datetime

from sqlalchemy import bindparam, func, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from backend.control_plane.persistence.models import Worker


class WorkerNotFound(Exception):
    pass


def register_worker(session: Session, identity: str, now: datetime, capabilities=None) -> Worker:
    # The unique key and atomic upsert also handle concurrent registration retries.
    statement = insert(Worker).values(
        identity=identity, registered_at=now, last_heartbeat_at=now,
        capabilities=["prepare_deployment"] if capabilities is None else sorted(set(capabilities)),
    )
    statement = statement.on_conflict_do_update(
        index_elements=[Worker.identity],
        set_={"last_heartbeat_at": func.max(
            Worker.last_heartbeat_at, statement.excluded.last_heartbeat_at
        ), "capabilities": statement.excluded.capabilities},
    ).returning(Worker)
    worker = session.scalars(statement).one()
    session.commit()
    session.refresh(worker)
    return worker


def heartbeat(session: Session, worker_id: int, now: datetime) -> Worker:
    worker = session.scalars(
        update(Worker).where(Worker.id == worker_id).values(
            last_heartbeat_at=func.max(
                Worker.last_heartbeat_at,
                bindparam("heartbeat_time", now, type_=Worker.last_heartbeat_at.type),
            )
        ).returning(Worker)
    ).one_or_none()
    if worker is None:
        raise WorkerNotFound(worker_id)
    session.commit()
    session.refresh(worker)
    return worker


def list_workers(session: Session) -> list[Worker]:
    return list(session.scalars(select(Worker).order_by(Worker.id)))


def worker_view(worker: Worker, now: datetime, timeout: float) -> dict:
    expired = (now - worker.last_heartbeat_at).total_seconds() > timeout
    return {
        "id": worker.id,
        "identity": worker.identity,
        "registered_at": worker.registered_at,
        "last_heartbeat_at": worker.last_heartbeat_at,
        "status": "unavailable" if expired else "available",
        "capabilities": worker.capabilities,
    }
