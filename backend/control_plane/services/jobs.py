"""Transactional ownership, expiry, and immutable attempt acknowledgments."""
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
import json

from sqlalchemy import or_, select, text, update
from sqlalchemy.orm import Session

from backend.control_plane.persistence.models import Deployment, Job, JobAttempt, JobRenewal, Worker, utc_now
from backend.control_plane.job_policy import JobPolicy

Clock = Callable[[], datetime]


class RecordNotFound(Exception):
    pass


class JobConflict(Exception):
    def __init__(self, message, code="invalid_transition"):
        super().__init__(message)
        self.code = code


@contextmanager
def write_transaction(session: Session) -> Iterator[None]:
    try:
        session.execute(text("BEGIN IMMEDIATE"))
        yield
        session.commit()
    except BaseException:
        session.rollback()
        raise


def require_worker(session, worker_id):
    worker = session.get(Worker, worker_id)
    if worker is None:
        raise RecordNotFound("Worker not found")
    return worker


def get_job(session, job_id):
    job = session.get(Job, job_id)
    if job is None:
        raise RecordNotFound("Job not found")
    return job


def snapshot(job):
    return {column.name: (value.isoformat() if isinstance(value, datetime) else value)
            for column in Job.__table__.columns
            for value in (getattr(job, column.name),)}


def change_job(session, job, **values):
    changed = session.scalar(update(Job).where(
        Job.id == job.id, Job.state == job.state, Job.attempt_count == job.attempt_count
    ).values(**values).returning(Job.id))
    if changed is None:
        raise JobConflict("Job changed during operation", "stale_attempt")
    session.refresh(job)


def list_jobs(session, *, state=None, deployment_id=None, worker_id=None, limit=100, offset=0):
    query = select(Job)
    for column, value in ((Job.state, state), (Job.deployment_id, deployment_id), (Job.worker_id, worker_id)):
        if value is not None:
            query = query.where(column == value)
    return list(session.scalars(query.order_by(Job.created_at, Job.id).limit(limit).offset(offset)))


def list_attempts(session, job_id):
    get_job(session, job_id)
    return list(session.scalars(select(JobAttempt).where(JobAttempt.job_id == job_id).order_by(JobAttempt.number)))


def poll_jobs(session, worker_id, clock=utc_now):
    require_worker(session, worker_id)
    rows = session.execute(select(Job, Deployment.specification).join(Deployment).where(
        Job.worker_id == worker_id, Job.state == "assigned", Job.assignment_expires_at > clock()
    ).order_by(Job.created_at, Job.id))
    return [{"id": job.id, "deployment_id": job.deployment_id, "kind": job.kind,
             "state": job.state, "specification": spec, "attempt_number": job.attempt_count,
             "assignment_expires_at": job.assignment_expires_at} for job, spec in rows]


def attempt_for(session, job_id, number):
    attempt = session.scalar(select(JobAttempt).where(JobAttempt.job_id == job_id, JobAttempt.number == number))
    if attempt is None:
        raise JobConflict("Assignment generation does not exist", "stale_attempt")
    return attempt


def check_owner(job, attempt, worker_id, claim_id, now):
    if (attempt.worker_id != worker_id or attempt.claim_id != claim_id
            or job.attempt_count != attempt.number or job.worker_id != worker_id
            or job.claim_id != claim_id):
        raise JobConflict("Worker, attempt, or claim ID does not own this job", "stale_attempt")
    if job.state != "claimed" or attempt.state != "claimed":
        raise JobConflict("Attempt is not actively claimed", "invalid_transition")
    if now >= job.lease_expires_at:
        raise JobConflict("Execution lease expired", "lease_expired")


def claim_job(session, worker_id, job_id, claim_id, timeout, clock=utc_now, *, attempt_number, policy=None):
    policy = policy or JobPolicy()
    with write_transaction(session):
        worker = require_worker(session, worker_id)
        job = get_job(session, job_id)
        now = clock()
        attempt = attempt_for(session, job_id, attempt_number)
        if job.attempt_count != attempt_number or job.worker_id != worker_id or attempt.worker_id != worker_id:
            raise JobConflict("Assignment belongs to another generation or worker", "stale_attempt")
        if job.state == "claimed" and job.claim_id == claim_id:
            check_owner(job, attempt, worker_id, claim_id, now)
        else:
            if job.state != "assigned":
                raise JobConflict("Only assigned jobs can be claimed")
            if now >= job.assignment_expires_at:
                raise JobConflict("Assignment expired", "assignment_expired")
            if worker.last_heartbeat_at < now - timedelta(seconds=timeout):
                raise JobConflict("Worker heartbeat has expired", "worker_unavailable")
            if session.scalar(select(JobAttempt.id).where(JobAttempt.claim_id == claim_id)) is not None:
                raise JobConflict("Claim ID already belongs to an attempt", "claim_id_conflict")
            change_job(session, job, state="claimed", claim_id=claim_id, claimed_at=now,
                       assignment_expires_at=None, lease_expires_at=now + timedelta(seconds=policy.lease_seconds))
            attempt.state = "claimed"
            attempt.claim_id = claim_id
            attempt.claimed_at = now
        response = {**snapshot(job), "server_time": now, "renewal_interval": policy.renewal_seconds}
    return response


def renew_job(session, worker_id, job_id, number, claim_id, renewal_id, clock=utc_now, policy=None):
    policy = policy or JobPolicy()
    with write_transaction(session):
        require_worker(session, worker_id)
        job = get_job(session, job_id)
        attempt = attempt_for(session, job_id, number)
        now = clock()
        check_owner(job, attempt, worker_id, claim_id, now)
        receipt = session.scalar(select(JobRenewal).where(
            JobRenewal.attempt_id == attempt.id, JobRenewal.renewal_id == renewal_id))
        if receipt is None:
            deadline = max(job.lease_expires_at, now + timedelta(seconds=policy.lease_seconds))
            change_job(session, job, lease_expires_at=deadline)
            receipt = JobRenewal(attempt_id=attempt.id, renewal_id=renewal_id,
                                 granted_at=now, lease_expires_at=deadline)
            session.add(receipt)
        response = {"job_id": job_id, "attempt_number": number, "worker_id": worker_id,
                    "claim_id": claim_id, "renewal_id": renewal_id,
                    "lease_expires_at": receipt.lease_expires_at, "server_time": now,
                    "renewal_interval": policy.renewal_seconds}
    return response


def close_job(session, job, now, policy, code, error, retryable):
    retry = retryable and job.attempt_count < job.max_attempts
    common = dict(assignment_expires_at=None, lease_expires_at=None,
                  last_failure_code=code if retry or not retryable else "attempts_exhausted")
    if retry:
        change_job(session, job, state="queued", worker_id=None, assigned_at=None,
                   claimed_at=None, claim_id=None, finished_at=None, result=None, error=None,
                   next_run_at=now + timedelta(seconds=policy.retry_delay(job.attempt_count)), **common)
    else:
        change_job(session, job, state="failed", finished_at=now, result=None, error=error, **common)


def report_job(session, worker_id, job_id, claim_id, outcome, result, error, clock=utc_now,
               *, attempt_number, retryable=False, policy=None):
    policy = policy or JobPolicy()
    payload = {"outcome": outcome, "result": result, "error": error, "retryable": retryable}
    with write_transaction(session):
        require_worker(session, worker_id)
        job = get_job(session, job_id)
        attempt = attempt_for(session, job_id, attempt_number)
        if attempt.worker_id != worker_id or attempt.claim_id != claim_id:
            raise JobConflict("Worker or claim ID does not own the attempt", "stale_attempt")
        if attempt.report_ack is not None:
            if json.dumps(attempt.report_payload, sort_keys=True) != json.dumps(payload, sort_keys=True):
                raise JobConflict("Report contradicts accepted outcome", "report_conflict")
            return attempt.report_ack
        now = clock()
        check_owner(job, attempt, worker_id, claim_id, now)
        if outcome == "completed":
            change_job(session, job, state="completed", finished_at=now, result=result,
                       error=None, assignment_expires_at=None, lease_expires_at=None)
            code = None
        else:
            code = "retryable_failure" if retryable else "permanent_failure"
            close_job(session, job, now, policy, code, error, retryable)
        ack = {**snapshot(job), "acknowledged_attempt_number": attempt_number, "attempt_outcome": outcome}
        # Set all terminal history fields before the single flush; history is immutable afterwards.
        attempt.state = outcome
        attempt.finished_at = now
        attempt.result = result
        attempt.error = error
        attempt.retryable = retryable
        attempt.failure_code = code
        attempt.report_payload = payload
        attempt.report_ack = ack
    return ack


def recover_in_transaction(session, now, policy, limit=100):
    expired = list(session.scalars(select(Job).where(or_(
        (Job.state == "assigned") & (Job.assignment_expires_at <= now),
        (Job.state == "claimed") & (Job.lease_expires_at <= now),
    )).order_by(Job.id).limit(limit)))
    recovered = []
    for job in expired:
        attempt = attempt_for(session, job.id, job.attempt_count)
        code = "assignment_expired" if job.state == "assigned" else "lease_expired"
        number = job.attempt_count
        close_job(session, job, now, policy, code, code.replace("_", " "), True)
        attempt.state = "expired"
        attempt.finished_at = now
        attempt.failure_code = code
        attempt.error = code.replace("_", " ")
        attempt.retryable = True
        recovered.append({"job_id": job.id, "attempt_number": number, "state": job.state,
                          "failure_code": code})
    session.flush()
    return recovered


def recover_jobs(session, limit=100, clock=utc_now, policy=None):
    with write_transaction(session):
        recovered = recover_in_transaction(session, clock(), policy or JobPolicy(), limit)
    return {"recovered_count": len(recovered), "recovered": recovered}
