"""Explicit bounded recovery and capability-aware FIFO scheduling."""
from datetime import timedelta
from sqlalchemy import select, tuple_
from backend.control_plane.persistence.models import Job, JobAttempt, Worker, utc_now
from backend.control_plane.services.jobs import write_transaction, recover_in_transaction, change_job
from backend.control_plane.services import instances
from backend.control_plane.job_policy import JobPolicy


def schedule_jobs(session, timeout, limit=100, clock=utc_now, policy=None):
    policy = policy or JobPolicy()
    assignments = []
    with write_transaction(session):
        now = clock()
        recover_in_transaction(session, now, policy, limit)
        busy = select(Job.worker_id).where(Job.state.in_(("assigned", "claimed")))
        workers = list(session.scalars(select(Worker).where(
            Worker.last_heartbeat_at >= now - timedelta(seconds=timeout), ~Worker.id.in_(busy)
        ).order_by(Worker.id)))
        cursor = None
        while workers and len(assignments) < limit:
            supported = {kind for worker in workers for kind in worker.capabilities}
            if not supported:
                break
            query = select(Job).where(Job.state == "queued", Job.next_run_at <= now,
                                       Job.attempt_count < Job.max_attempts, Job.kind.in_(supported))
            if cursor is not None:
                query = query.where(tuple_(Job.next_run_at, Job.created_at, Job.id) > cursor)
            queued = list(session.scalars(query.order_by(Job.next_run_at, Job.created_at, Job.id).limit(100)))
            if not queued:
                break
            for job in queued:
                cursor = (job.next_run_at, job.created_at, job.id)
                worker = next((node for node in workers if job.kind in node.capabilities), None)
                if worker is None:
                    continue
                number = job.attempt_count + 1
                change_job(session, job, state="assigned", worker_id=worker.id, assigned_at=now,
                           attempt_count=number, assignment_expires_at=now + timedelta(seconds=policy.assignment_seconds))
                attempt = JobAttempt(job_id=job.id, number=number, worker_id=worker.id,
                                     state="assigned", assigned_at=now)
                session.add(attempt)
                session.flush()
                if job.kind == "start_instance":
                    instances.record_placement(session, job, attempt, now)
                assignments.append({"job_id": job.id, "worker_id": worker.id, "attempt_number": number})
                workers.remove(worker)
                if not workers or len(assignments) == limit:
                    break
    return assignments
