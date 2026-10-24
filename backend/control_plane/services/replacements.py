"""Conservative simulation-only replacement policy and durable admin receipts."""

from datetime import timedelta

from sqlalchemy import select, update

from backend.control_plane.persistence.models import Job, JobAttempt, ReplicaSlot, ReplacementReceipt, utc_now
from backend.control_plane.services import instances
from backend.control_plane.services.jobs import JobConflict, write_transaction


COOLDOWN_SECONDS = 30


def classify_replacement(session, slot, instance, job, now):
    """Shared read-only policy. Never treat heartbeat expiry as termination."""
    def result(status, reason, eligible_at=None):
        return dict(status=status, reason=reason,
                    eligible_at=eligible_at.isoformat() if eligible_at is not None else None, simulated=True)
    if slot is None:
        raise JobConflict("Instance has no logical slot", "replica_integrity_conflict")
    if slot.current_generation != instance.generation:
        return result("retained", "historical_generation")
    if job.state in ("queued", "assigned", "claimed"):
        return result("retained", "startup_retrying" if job.state == "queued" and job.attempt_count else "startup_active")
    if instance.state == "stopped":
        return result("blocked", "stopped")
    if instance.state == "healthy":
        return result("retained", "healthy")
    attempts = list(session.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id).order_by(JobAttempt.number)))
    if any(attempt.state == "expired" and attempt.claim_id is not None for attempt in attempts):
        return result("blocked", "uncertain_execution")
    if job.last_failure_code == "permanent_failure":
        return result("blocked", "permanent_startup_failure")
    safe = (job.state == "failed" and job.last_failure_code == "attempts_exhausted"
            and len(attempts) == job.max_attempts == job.attempt_count
            and all((attempt.state == "expired" and attempt.claim_id is None)
                    or (attempt.state == "failed" and attempt.retryable
                        and ((attempt.report_payload or {}).get("instance_outcome") or {}).get("state") == "failed")
                    for attempt in attempts))
    if not safe:
        return result("blocked", "terminal_startup_failure")
    if slot.automatic_replacements >= slot.replacement_limit:
        return result("blocked", "automatic_budget_exhausted")
    eligible_at = job.finished_at + timedelta(seconds=COOLDOWN_SECONDS)
    if now < eligible_at:
        return result("cooldown", "replacement_cooldown", eligible_at)
    return result("eligible", "retry_attempts_exhausted", eligible_at)


def replace_current(session, slot, previous, policy, now, *, automatic):
    """Caller holds BEGIN IMMEDIATE and validates policy/state before calling."""
    old_version, old_generation = slot.version, slot.current_generation
    instance = instances.create_incarnation(session, slot, old_generation + 1,
        "automatic_replacement" if automatic else "administrative_replacement", policy, now)
    changed = session.scalar(update(ReplicaSlot).where(
        ReplicaSlot.deployment_id == slot.deployment_id, ReplicaSlot.replica_index == slot.replica_index,
        ReplicaSlot.version == old_version, ReplicaSlot.current_generation == previous.generation,
    ).values(current_generation=old_generation + 1, version=old_version + 1,
             automatic_replacements=slot.automatic_replacements + int(automatic), updated_at=now)
      .returning(ReplicaSlot.deployment_id))
    if changed is None:
        raise JobConflict("Slot changed during replacement", "slot_version_conflict")
    session.refresh(slot)
    return dict(deployment_id=slot.deployment_id, replica_index=slot.replica_index,
                previous_instance_id=previous.id, previous_generation=previous.generation,
                instance_id=instance.id, generation=instance.generation, start_job_id=instance.start_job_id,
                slot_version=slot.version, creation_reason=instance.creation_reason, simulated=True)


def administrative_replace(session, instance_id, payload, policy, clock=utc_now):
    from backend.control_plane.services.replicas import get_target, inspect_slots
    with write_transaction(session):
        request_id = payload["request_id"]
        accepted_payload = {**payload, "instance_id": instance_id}
        receipt = session.get(ReplacementReceipt, request_id)
        if receipt is not None:
            if receipt.payload != accepted_payload:
                raise JobConflict("Request ID contradicts an accepted replacement", "replacement_request_conflict")
            return receipt.acknowledgment
        previous = instances.get_instance(session, instance_id)
        target = get_target(session, previous.deployment_id)
        inspect_slots(session, target)
        slot = session.get(ReplicaSlot, (previous.deployment_id, previous.replica_index))
        if slot.current_generation != previous.generation:
            raise JobConflict("Instance generation is no longer current", "stale_generation")
        if slot.version != payload["expected_slot_version"]:
            raise JobConflict("Slot changed since inspection", "slot_version_conflict")
        job = session.get(Job, previous.start_job_id)
        if previous.state not in ("failed", "stopped") or job.state not in ("completed", "failed"):
            raise JobConflict("Replacement requires terminal failed or stopped startup work", "replacement_state_conflict")
        ack = {"request_id": request_id, **replace_current(session, slot, previous, policy, clock(), automatic=False)}
        session.add(ReplacementReceipt(request_id=request_id, previous_instance_id=previous.id,
            new_instance_id=ack["instance_id"], payload=accepted_payload, acknowledgment=ack, created_at=clock()))
    return ack
