"""Explicit simulated instances. Helpers participate in the caller's transaction."""

from sqlalchemy import select, update

from backend.control_plane.persistence.models import Instance, Job, Worker, utc_now


def find_for_job(session, job):
    from backend.control_plane.services.jobs import JobConflict
    instance = session.scalar(select(Instance).where(Instance.start_job_id == job.id))
    if instance is None or instance.deployment_id != job.deployment_id:
        raise JobConflict("Startup job has no matching instance", "instance_mismatch")
    return instance


def get_instance(session, instance_id):
    from backend.control_plane.services.jobs import RecordNotFound
    instance = session.get(Instance, instance_id)
    if instance is None:
        raise RecordNotFound("Instance not found")
    return instance


def list_instances(session, *, deployment_id=None, worker_id=None, state=None, limit=100, offset=0):
    query = select(Instance)
    for column, value in ((Instance.deployment_id, deployment_id), (Instance.worker_id, worker_id), (Instance.state, state)):
        if value is not None:
            query = query.where(column == value)
    return list(session.scalars(query.order_by(Instance.created_at, Instance.id).limit(limit).offset(offset)))


def change_instance(session, instance, now, *, expected_version=None, **values):
    from backend.control_plane.services.jobs import JobConflict
    version = instance.version if expected_version is None else expected_version
    changed = session.scalar(update(Instance).where(
        Instance.id == instance.id, Instance.version == version,
        Instance.state == instance.state,
        Instance.last_attempt_id.is_(None) if instance.last_attempt_id is None
        else Instance.last_attempt_id == instance.last_attempt_id,
    ).values(**values, version=version + 1, updated_at=now).returning(Instance.id))
    if changed is None:
        raise JobConflict("Instance changed since it was inspected", "instance_version_conflict")
    session.refresh(instance)


def instance_view(session, instance, now, timeout):
    job = session.get(Job, instance.start_job_id)
    worker = session.get(Worker, instance.worker_id) if instance.worker_id is not None else None
    active = job.state in ("assigned", "claimed")
    result = {column.name: getattr(instance, column.name) for column in Instance.__table__.columns}
    result.update(
        simulated=True,
        worker_available=None if worker is None else (now - worker.last_heartbeat_at).total_seconds() <= timeout,
        startup_job_state=job.state,
        current_worker_id=job.worker_id if active else None,
        current_attempt_number=job.attempt_count if active else None,
        ownership_valid=job.state == "claimed" and now < job.lease_expires_at,
    )
    return result


def create_instance(session, deployment_id, policy, clock=utc_now):
    from backend.control_plane.services.jobs import JobConflict, write_transaction
    from backend.control_plane.services.replicas import get_target, inspect_slots
    with write_transaction(session):
        target = get_target(session, deployment_id)
        inspect_slots(session, target)
        if target.desired_replicas == 0:
            raise JobConflict("Deployment has no desired replica", "no_desired_replica")
        instance, created = ensure_slot(session, deployment_id, 0, policy, clock())
    return instance, created


def ensure_slot(session, deployment_id, replica_index, policy, now):
    """Caller must hold the write transaction and validate target/slot integrity."""
    instance = session.scalar(select(Instance).where(
        Instance.deployment_id == deployment_id, Instance.replica_index == replica_index))
    if instance is not None:
        return instance, False
    job = Job(deployment_id=deployment_id, kind="start_instance", state="queued",
              max_attempts=policy.max_attempts, created_at=now, next_run_at=now)
    session.add(job)
    session.flush()
    instance = Instance(deployment_id=deployment_id, replica_index=replica_index, start_job_id=job.id,
                        created_at=now, updated_at=now)
    session.add(instance)
    session.flush()
    return instance, True


def stop_instance(session, instance_id, expected_version, clock=utc_now):
    from backend.control_plane.services.jobs import JobConflict, write_transaction
    with write_transaction(session):
        instance = get_instance(session, instance_id)
        if instance.state == "stopped":
            return instance
        job = session.get(Job, instance.start_job_id)
        if job.state not in ("completed", "failed") or instance.state not in ("healthy", "failed"):
            raise JobConflict("Stop requires terminal startup work and a healthy or failed instance", "instance_stop_conflict")
        now = clock()
        change_instance(session, instance, now, expected_version=expected_version,
                        state="stopped", state_source="manual", state_reason="simulated_stop", stopped_at=now)
    return instance


def record_placement(session, job, attempt, now):
    instance = find_for_job(session, job)
    change_instance(session, instance, now, worker_id=attempt.worker_id, last_attempt_id=attempt.id,
                    state_source="control_plane", state_reason="assigned")


def record_claim(session, job, attempt, now):
    from backend.control_plane.services.jobs import JobConflict
    instance = find_for_job(session, job)
    if instance.last_attempt_id != attempt.id or instance.worker_id != attempt.worker_id:
        raise JobConflict("Instance placement does not match the claim", "instance_mismatch")
    if instance.state not in ("pending", "failed"):
        raise JobConflict("Instance cannot start from this state", "instance_state_conflict")
    change_instance(session, instance, now, state="starting", state_source="control_plane",
                    state_reason="startup_claimed")


def record_failure(session, job, attempt, now, reason, claimed):
    from backend.control_plane.services.jobs import JobConflict
    # Assignment expiry alone does not imply startup was attempted.
    instance = find_for_job(session, job)
    if instance.last_attempt_id != attempt.id:
        raise JobConflict("Recovery attempt does not match instance placement", "instance_mismatch")
    state = "failed" if claimed or job.state == "failed" else instance.state
    change_instance(session, instance, now, state=state, state_source="control_plane", state_reason=reason)


def record_outcome(session, job, attempt, observation, now):
    from backend.control_plane.services.jobs import JobConflict
    instance = find_for_job(session, job)
    if (observation["instance_id"] != instance.id or instance.worker_id != attempt.worker_id
            or instance.last_attempt_id != attempt.id or instance.state != "starting"):
        raise JobConflict("Observation does not match current instance ownership", "instance_mismatch")
    values = dict(state=observation["state"], last_observed_at=now,
                  state_source="worker_report", state_reason=observation["phase"])
    if observation["state"] == "healthy":
        values["healthy_at"] = now
    change_instance(session, instance, now, expected_version=observation["expected_version"], **values)
    return instance
