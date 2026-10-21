"""Explicit, bounded FIFO scheduling. No background scheduler is started."""

from datetime import timedelta

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from backend.control_plane.persistence.models import Job, Worker, utc_now
from backend.control_plane.services.jobs import Clock, write_transaction


def schedule_jobs(
    session: Session, timeout: float, limit: int = 100, clock: Clock = utc_now,
) -> list[dict[str, int]]:
    assignments = []
    with write_transaction(session):
        # Availability, capacity, and assignment share the same locked snapshot.
        now = clock()
        busy = select(Job.worker_id).where(Job.state.in_(("assigned", "claimed")))
        workers = list(session.scalars(select(Worker.id).where(
            Worker.last_heartbeat_at >= now - timedelta(seconds=timeout),
            ~Worker.id.in_(busy),
        ).order_by(Worker.id).limit(limit)))
        queued = list(session.scalars(select(Job.id).where(Job.state == "queued").order_by(
            Job.created_at, Job.id
        ).limit(len(workers))))
        for job_id, worker_id in zip(queued, workers):
            updated = session.scalar(update(Job).where(Job.id == job_id, Job.state == "queued").values(
                state="assigned", worker_id=worker_id, assigned_at=now
            ).returning(Job.id))
            if updated is not None:
                assignments.append({"job_id": job_id, "worker_id": worker_id})
    return assignments
