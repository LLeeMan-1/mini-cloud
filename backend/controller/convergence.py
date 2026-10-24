"""Synchronous HTTP orchestration only. Durable decisions belong to the API."""

from collections import Counter
from dataclasses import dataclass
import logging
import math
from threading import Event
from time import monotonic

from backend.controller.client import AvailabilityError, DeploymentError, FatalError, ProtocolError


logger = logging.getLogger(__name__)


@dataclass
class CycleResult:
    cycle_id: int
    outcome: str = "success"
    deployments_seen: int = 0
    deployments_reconciled: int = 0
    deployments_failed: int = 0
    jobs_recovered: int = 0
    slots_created: int = 0
    instances_replaced: int = 0
    blocked_replicas: int = 0
    jobs_assigned: int = 0
    duration_seconds: float = 0
    global_failure: bool = False
    retry_after: float = 0
    fatal: bool = False


class StopRequested(Exception):
    pass


def run_cycle(client, *, page_size=100, stop_event=None, clock=monotonic, cycle_id=1):
    if type(page_size) is not int or not 1 <= page_size <= 100:
        raise ValueError("Page size must be an integer from 1 to 100")
    stop = stop_event if stop_event is not None else Event()
    result = CycleResult(cycle_id)
    start = clock()
    logger.info("event=cycle_start cycle_id=%s", cycle_id)

    def check_stop():
        if stop.is_set():
            raise StopRequested

    def log_error(error, deployment_id=None):
        mutation_result = ("not_applicable" if error.operation == "discover" else "rejected"
                           if error.status in (400, 401, 403, 404, 409, 422) else "unknown")
        logger.warning("event=request_failed cycle_id=%s operation=%s deployment_id=%s status=%s code=%s mutation_result=%s",
                       cycle_id, error.operation, deployment_id, error.status, error.code,
                       mutation_result)

    try:
        check_stop()
        recovered = client.recover()
        result.jobs_recovered = recovered["recovered_count"]
        logger.info("event=recovery_complete cycle_id=%s recovered_count=%s", cycle_id, result.jobs_recovered)
        after_id, boundary, consecutive_server_errors = 0, None, 0
        while True:
            check_stop()
            page = client.page(page_size, after_id, boundary)
            boundary = page["snapshot_max_id"]
            result.deployments_seen += len(page["items"])
            for item in page["items"]:
                check_stop()
                deployment_id = item["id"]
                try:
                    data = client.reconcile(deployment_id)
                except (AvailabilityError, FatalError) as error:
                    result.deployments_failed += 1
                    error.deployment_id = deployment_id
                    raise
                except (DeploymentError, ProtocolError) as error:
                    result.deployments_failed += 1
                    result.outcome = "degraded"
                    log_error(error, deployment_id)
                    consecutive_server_errors = consecutive_server_errors + 1 if error.status and error.status >= 500 else 0
                    if consecutive_server_errors >= 3:
                        raise AvailabilityError("reconcile", "consecutive_server_errors", error.status) from None
                    continue
                consecutive_server_errors = 0
                result.deployments_reconciled += 1
                result.slots_created += data["created_count"]
                result.instances_replaced += data["replaced_count"]
                result.blocked_replicas += len(data["blocked"])
                level = logging.INFO if data["created_count"] or data["replaced_count"] else logging.DEBUG
                logger.log(level, "event=reconcile_complete cycle_id=%s deployment_id=%s created_count=%s replaced_count=%s",
                           cycle_id, deployment_id, data["created_count"], data["replaced_count"])
                for reason, count in sorted(Counter(row["reason"] for row in data["blocked"]).items()):
                    logger.info("event=deployment_blocked cycle_id=%s deployment_id=%s reason=%s count=%s",
                                cycle_id, deployment_id, reason, count)
            if page["next_after_id"] is None:
                break
            after_id = page["next_after_id"]
        check_stop()
        result.jobs_assigned = client.schedule()["assigned_count"]
        logger.info("event=scheduling_complete cycle_id=%s assigned_count=%s", cycle_id, result.jobs_assigned)
    except StopRequested:
        result.outcome = "aborted"
        logger.info("event=cycle_interrupted cycle_id=%s reason=shutdown", cycle_id)
    except FatalError as error:
        result.outcome, result.fatal = "aborted", True
        log_error(error, getattr(error, "deployment_id", None))
    except (AvailabilityError, ProtocolError) as error:
        result.outcome, result.global_failure = "aborted", True
        result.retry_after = error.retry_after
        log_error(error, getattr(error, "deployment_id", None))
    finally:
        result.duration_seconds = max(0, clock() - start)
        logger.info("event=cycle_end cycle_id=%s outcome=%s duration_seconds=%.3f deployments_seen=%s deployments_reconciled=%s deployments_failed=%s jobs_recovered=%s slots_created=%s instances_replaced=%s blocked_replicas=%s jobs_assigned=%s",
                    cycle_id, result.outcome, result.duration_seconds, result.deployments_seen,
                    result.deployments_reconciled, result.deployments_failed, result.jobs_recovered,
                    result.slots_created, result.instances_replaced, result.blocked_replicas, result.jobs_assigned)
    return result


def run_controller(client, *, interval=5, page_size=100, once=False, stop_event=None, clock=monotonic):
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError("Interval must be finite and positive")
    stop = stop_event if stop_event is not None else Event()
    streak, cycle_id = 0, 0
    try:
        while not stop.is_set():
            cycle_id += 1
            result = run_cycle(client, page_size=page_size, stop_event=stop, clock=clock, cycle_id=cycle_id)
            if result.fatal:
                return 1
            if stop.is_set():
                return 0
            if once:
                return {"success": 0, "degraded": 1, "aborted": 2}[result.outcome]
            streak = min(streak + 1, 6) if result.global_failure else 0
            backoff = min(30, 2 ** (streak - 1)) if streak else 0
            delay = max(interval, backoff, result.retry_after)
            deadline = clock() + delay
            logger.info("event=cycle_wait cycle_id=%s next_delay_seconds=%s", cycle_id, delay)
            while not stop.is_set():
                remaining = deadline - clock()
                if remaining <= 0 or stop.wait(remaining):
                    break
    finally:
        logger.info("event=controller_stopped cycles=%s", cycle_id)
    return 0
