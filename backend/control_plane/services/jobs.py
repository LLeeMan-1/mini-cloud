"""Durable job ownership and simulated outcomes; no application execution."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select, text, update
from sqlalchemy.orm import Session

from backend.control_plane.persistence.models import Deployment, Job, Worker, utc_now


Clock = Callable[[], datetime]


class RecordNotFound(Exception):
    pass


class JobConflict(Exception):
    pass


@contextmanager
def write_transaction(session: Session) -> Iterator[None]:
    """Acquire SQLite's write reservation before any decision-making reads."""
    try:
        session.execute(text("BEGIN IMMEDIATE"))
        yield
        session.commit()
    except BaseException:
        session.rollback()
        raise


def require_worker(session: Session, worker_id: int) -> Worker:
    worker = session.get(Worker, worker_id)
    if worker is None:
        raise RecordNotFound("Worker not found")
    return worker


def get_job(session: Session, job_id: int) -> Job:
    job = session.get(Job, job_id)
    if job is None:
        raise RecordNotFound("Job not found")
    return job


def list_jobs(
    session: Session, *, state: str | None = None, deployment_id: int | None = None,
    worker_id: int | None = None, limit: int = 100, offset: int = 0,
) -> list[Job]:
    query = select(Job)
    for column, value in ((Job.state, state), (Job.deployment_id, deployment_id), (Job.worker_id, worker_id)):
        if value is not None:
            query = query.where(column == value)
    return list(session.scalars(query.order_by(Job.created_at, Job.id).limit(limit).offset(offset)))


def poll_jobs(session: Session, worker_id: int) -> list[dict[str, Any]]:
    require_worker(session, worker_id)
    rows = session.execute(
        select(Job, Deployment.specification).join(Deployment).where(
            Job.worker_id == worker_id, Job.state == "assigned"
        ).order_by(Job.created_at, Job.id)
    )
    return [{"id": job.id, "deployment_id": job.deployment_id, "kind": job.kind,
             "state": job.state, "specification": specification} for job, specification in rows]


def claim_job(
    session: Session, worker_id: int, job_id: int, claim_id: str, timeout: float,
    clock: Clock = utc_now,
) -> Job:
    with write_transaction(session):
        worker = require_worker(session, worker_id)
        job = get_job(session, job_id)
        if job.worker_id != worker_id:
            raise JobConflict("Job is not assigned to this worker")
        if job.claim_id == claim_id and job.state in ("claimed", "completed", "failed"):
            return job
        if job.state != "assigned":
            raise JobConflict("Only assigned jobs can be claimed")
        now = clock()
        if worker.last_heartbeat_at < now - timedelta(seconds=timeout):
            raise JobConflict("Worker heartbeat has expired")
        if session.scalar(select(Job.id).where(Job.claim_id == claim_id)) is not None:
            raise JobConflict("Claim ID already belongs to another job")
        updated = session.scalar(update(Job).where(
            Job.id == job_id, Job.worker_id == worker_id, Job.state == "assigned"
        ).values(state="claimed", claim_id=claim_id, claimed_at=now).returning(Job.id))
        if updated is None:
            raise JobConflict("Job could not be claimed")
    return get_job(session, job_id)


def report_job(
    session: Session, worker_id: int, job_id: int, claim_id: str, outcome: str,
    result: dict[str, Any] | None, error: str | None, clock: Clock = utc_now,
) -> Job:
    with write_transaction(session):
        require_worker(session, worker_id)
        job = get_job(session, job_id)
        if job.worker_id != worker_id or job.claim_id != claim_id:
            raise JobConflict("Worker or claim ID does not own this job")
        if job.state in ("completed", "failed"):
            if (job.state, job.result, job.error) != (outcome, result, error):
                raise JobConflict("Report contradicts the recorded outcome")
            return job
        if job.state != "claimed":
            raise JobConflict("Only claimed jobs can be reported")
        updated = session.scalar(update(Job).where(
            Job.id == job_id, Job.worker_id == worker_id,
            Job.claim_id == claim_id, Job.state == "claimed",
        ).values(state=outcome, result=result, error=error, finished_at=clock()).returning(Job.id))
        if updated is None:
            raise JobConflict("Job could not be reported")
    return get_job(session, job_id)
