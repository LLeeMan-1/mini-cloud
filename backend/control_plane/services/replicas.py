"""Explicit slot reconciliation and bounded replacement; recovery stays separate."""

from sqlalchemy import select, text, update

from backend.control_plane.persistence.models import Deployment, DeploymentTarget, Instance, Job, JobAttempt, ReplicaSlot, Worker, utc_now
from backend.control_plane.services import instances
from backend.control_plane.services.jobs import JobConflict, RecordNotFound, write_transaction
from backend.control_plane.services.replacements import classify_replacement, replace_current


def get_target(session, deployment_id):
    if session.get(Deployment, deployment_id) is None:
        raise RecordNotFound("Deployment not found")
    target = session.get(DeploymentTarget, deployment_id)
    if target is None:
        raise JobConflict("Deployment target is missing", "replica_integrity_conflict")
    return target


def inspect_slots(session, target):
    """Fail closed before creating work if persistent relationships disagree."""
    incarnations = list(session.scalars(select(Instance).where(
        Instance.deployment_id == target.deployment_id).order_by(Instance.replica_index, Instance.generation)))
    logical_slots = list(session.scalars(select(ReplicaSlot).where(
        ReplicaSlot.deployment_id == target.deployment_id).order_by(ReplicaSlot.replica_index)))
    by_index = {slot.replica_index: slot for slot in logical_slots}
    jobs = {job.id: job for job in session.scalars(select(Job).where(
        Job.deployment_id == target.deployment_id, Job.kind == "start_instance"))}
    allowed = {"pending": {"queued", "assigned"}, "starting": {"claimed"},
               "healthy": {"completed"}, "failed": {"queued", "assigned", "failed"},
               "stopped": {"completed", "failed"}}
    invalid = set(jobs) != {instance.start_job_id for instance in incarnations}
    for slot in logical_slots:
        generations = [instance.generation for instance in incarnations if instance.replica_index == slot.replica_index]
        if generations != list(range(1, slot.current_generation + 1)):
            invalid = True
    for instance in incarnations:
        job = jobs.get(instance.start_job_id)
        slot = by_index.get(instance.replica_index)
        if (slot is None or instance.replica_index >= target.desired_replicas or job is None
                or job.state not in allowed[instance.state]):
            invalid = True
            continue
        if instance.generation != slot.current_generation and (job.state not in ("completed", "failed")
                                                               or instance.state not in ("failed", "stopped")):
            invalid = True
        history = list(session.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id).order_by(JobAttempt.number)))
        if [attempt.number for attempt in history] != list(range(1, job.attempt_count + 1)):
            invalid = True
        if any(attempt.state in ("assigned", "claimed") for attempt in history[:-1]):
            invalid = True
        if any(attempt.state in ("completed", "failed")
               and (attempt.report_payload is None or attempt.report_ack is None) for attempt in history):
            invalid = True
        if job.attempt_count > 0:
            attempt = session.get(JobAttempt, instance.last_attempt_id) if instance.last_attempt_id else None
            if (attempt is None or attempt.job_id != job.id or attempt.number != job.attempt_count
                    or instance.worker_id != attempt.worker_id):
                invalid = True
            elif job.state in ("assigned", "claimed", "completed"):
                if (attempt.worker_id != job.worker_id or attempt.state != job.state
                        or attempt.claim_id != job.claim_id):
                    invalid = True
            else:
                if attempt.state not in ("failed", "expired"):
                    invalid = True
                if job.state == "failed" and (attempt.worker_id != job.worker_id or attempt.claim_id != job.claim_id):
                    invalid = True
        elif (job.state != "queued" or instance.worker_id is not None
              or instance.last_attempt_id is not None):
            invalid = True
    if invalid:
        raise JobConflict("Replica slots and startup jobs are inconsistent", "replica_integrity_conflict")
    slots = [instance for instance in incarnations if instance.generation == by_index[instance.replica_index].current_generation]
    return slots, {instance.start_job_id: jobs[instance.start_job_id] for instance in slots}


def slot_view(instance):
    return {"replica_index": instance.replica_index, "instance_id": instance.id,
            "start_job_id": instance.start_job_id, "generation": instance.generation}


def replacement_views(session, slots, jobs, now):
    views = []
    for instance in slots:
        slot = session.get(ReplicaSlot, (instance.deployment_id, instance.replica_index))
        views.append({**slot_view(instance), "slot_version": slot.version,
                      "automatic_replacements": slot.automatic_replacements, "replacement_limit": slot.replacement_limit,
                      "replacement": classify_replacement(session, slot, instance, jobs[instance.start_job_id], now)})
    return views


def update_target(session, deployment_id, desired_replicas, expected_version, clock=utc_now):
    with write_transaction(session):
        target = get_target(session, deployment_id)
        if target.version != expected_version:
            raise JobConflict("Replica target changed since inspection", "target_version_conflict")
        if desired_replicas < target.desired_replicas:
            raise JobConflict("Scale-down is deferred to B2", "scale_down_not_supported")
        if desired_replicas != target.desired_replicas:
            changed = session.scalar(update(DeploymentTarget).where(
                DeploymentTarget.deployment_id == deployment_id,
                DeploymentTarget.version == expected_version,
            ).values(desired_replicas=desired_replicas, version=expected_version + 1,
                     updated_at=clock()).returning(DeploymentTarget.deployment_id))
            if changed is None:
                raise JobConflict("Replica target changed since inspection", "target_version_conflict")
            session.refresh(target)
    return target


def get_summary(session, deployment_id, timeout, clock=utc_now):
    # Explicit read transaction keeps multi-query inspection on one SQLite snapshot.
    with session.begin():
        session.execute(text("BEGIN"))
        target = get_target(session, deployment_id)
        slots, jobs = inspect_slots(session, target)
        now = clock()
        workers = {worker.id: worker for worker in session.scalars(select(Worker).where(
            Worker.id.in_([instance.worker_id for instance in slots if instance.worker_id is not None])))}
        indexes = {instance.replica_index for instance in slots}
        views = replacement_views(session, slots, jobs, now)
        result = {column.name: getattr(target, column.name) for column in DeploymentTarget.__table__.columns}
        result.update(
            materialized_replicas=len(slots),
            missing_indexes=[index for index in range(target.desired_replicas) if index not in indexes],
            lifecycle_counts={state: sum(instance.state == state for instance in slots)
                              for state in ("pending", "starting", "healthy", "failed", "stopped")},
            retrying_replicas=sum(job.state == "queued" and job.attempt_count > 0 for job in jobs.values()),
            expired_ownership_replicas=sum(
                (job.state == "assigned" and now >= job.assignment_expires_at)
                or (job.state == "claimed" and now >= job.lease_expires_at) for job in jobs.values()),
            unavailable_worker_replicas=sum(
                instance.worker_id is not None
                and (now - workers[instance.worker_id].last_heartbeat_at).total_seconds() > timeout
                for instance in slots),
            slots=views,
            blocked=[{**slot_view(instance), "reason": view["replacement"]["reason"]}
                     for instance, view in zip(slots, views) if view["replacement"]["status"] == "blocked"],
            replacement_eligible=[view for view in views if view["replacement"]["status"] in ("eligible", "cooldown")],
        )
    return result


def reconcile_deployment(session, deployment_id, policy, clock=utc_now):
    with write_transaction(session):
        target = get_target(session, deployment_id)
        slots, jobs = inspect_slots(session, target)
        occupied = {instance.replica_index for instance in slots}
        created = []
        replaced = []
        now = clock()
        views = replacement_views(session, slots, jobs, now)
        for instance, view in zip(slots, views):
            if view["replacement"]["status"] == "eligible":
                slot = session.get(ReplicaSlot, (deployment_id, instance.replica_index))
                replaced.append(replace_current(session, slot, instance, policy, now, automatic=True))
        for index in range(target.desired_replicas):
            if index not in occupied:
                instance, _ = instances.ensure_slot(session, deployment_id, index, policy, now)
                created.append(slot_view(instance))
        result = dict(deployment_id=deployment_id, target_version=target.version,
                      desired_replicas=target.desired_replicas, created_count=len(created),
                      created=created, replaced_count=len(replaced), replaced=replaced,
                      blocked=[{**slot_view(instance), "reason": view["replacement"]["reason"]}
                               for instance, view in zip(slots, views) if view["replacement"]["status"] == "blocked"],
                      replacement_eligible=[view for view in views if view["replacement"]["status"] == "cooldown"])
    return result
