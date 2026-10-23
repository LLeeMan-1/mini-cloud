"""Explicit bounded recovery and deterministic scheduling in one write transaction."""
from datetime import timedelta
from sqlalchemy import select
from backend.control_plane.persistence.models import Job, JobAttempt, Worker, utc_now
from backend.control_plane.services.jobs import write_transaction, recover_in_transaction, change_job
from backend.control_plane.job_policy import JobPolicy


def schedule_jobs(session, timeout, limit=100, clock=utc_now, policy=None):
    policy = policy or JobPolicy()
    assignments = []
    with write_transaction(session):
        now = clock()
        recover_in_transaction(session, now, policy, limit)
        busy = select(Job.worker_id).where(Job.state.in_(("assigned", "claimed")))
        workers = list(session.scalars(select(Worker.id).where(
            Worker.last_heartbeat_at >= now - timedelta(seconds=timeout), ~Worker.id.in_(busy)
        ).order_by(Worker.id).limit(limit)))
        queued = list(session.scalars(select(Job).where(
            Job.state == "queued", Job.next_run_at <= now, Job.attempt_count < Job.max_attempts
        ).order_by(Job.next_run_at, Job.created_at, Job.id).limit(len(workers))))
        for job, worker_id in zip(queued, workers):
            number = job.attempt_count + 1
            change_job(session, job, state="assigned", worker_id=worker_id, assigned_at=now,
                       attempt_count=number, assignment_expires_at=now + timedelta(seconds=policy.assignment_seconds))
            session.add(JobAttempt(job_id=job.id, number=number, worker_id=worker_id,
                                   state="assigned", assigned_at=now))
            assignments.append({"job_id": job.id, "worker_id": worker_id, "attempt_number": number})
    return assignments
