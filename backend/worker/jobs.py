"""HTTP-only simulated work. Specifications are never interpreted or executed."""

import logging
from time import monotonic
from uuid import uuid4

import httpx


logger = logging.getLogger(__name__)


class JobPoller:
    def __init__(self):
        self.pending = None

    def step(self, client: httpx.Client, worker_id: int) -> bool:
        """One request per step. Return True when another stage is ready now."""
        if self.pending is None:
            response = client.get(f"/workers/{worker_id}/jobs")
            response.raise_for_status()
            assignments = response.json()
            if not isinstance(assignments, list):
                raise ValueError("Assignments must be a list")
            if not assignments:
                return False
            assignment = assignments[0]
            if (not isinstance(assignment, dict) or type(assignment.get("id")) is not int
                    or assignment["id"] <= 0 or assignment.get("kind") != "prepare_deployment"):
                raise ValueError("Invalid simulated assignment")
            self.pending = {"id": assignment["id"], "claim_id": str(uuid4()), "phase": "claim"}
            return True
        job = self.pending
        prefix = f"/workers/{worker_id}/jobs/{job['id']}"
        if job["phase"] == "claim":
            response = client.post(prefix + "/claim", json={"claim_id": job["claim_id"]})
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError("Claim response must be an object")
            state = data.get("state")
            if state in ("completed", "failed"):
                self.pending = None
                return False
            if state != "claimed":
                raise ValueError("Claim response must confirm ownership")
            job["phase"] = "report"
            return True
        response = client.post(prefix + "/report", json={
            "claim_id": job["claim_id"], "outcome": "completed", "result": {"simulated": True}
        })
        response.raise_for_status()
        logger.info("Completed simulated job %s", job["id"])
        self.pending = None
        return False


def retryable(error: Exception) -> bool:
    return not isinstance(error, httpx.HTTPStatusError) or (
        error.response.status_code >= 500 or error.response.status_code in (408, 429)
    )


def run_job_agent(client, identity, interval, stop, *, clock=monotonic):
    """Independent heartbeat/poll deadlines and backoffs in one lightweight loop.

    Network calls are synchronous and timeout-bounded; a request can delay another
    operation, but a polling backoff never suspends the heartbeat deadline.
    Pending claim/report state survives transient failures within this process.
    """
    worker_id = None
    poller = JobPoller()
    heartbeat_due = poll_due = clock()
    heartbeat_retry = poll_retry = 1.0
    while not stop.is_set():
        if clock() >= heartbeat_due:
            try:
                if worker_id is None:
                    response = client.post("/workers/register", json={"identity": identity.strip()})
                    response.raise_for_status()
                    data = response.json()
                    if not isinstance(data, dict) or type(data.get("id")) is not int or data["id"] <= 0:
                        raise ValueError("Registration response must contain a positive integer ID")
                    worker_id = data["id"]
                    logger.info("Registered worker %s as ID %s", identity, worker_id)
                    poll_due = clock()
                else:
                    response = client.post(f"/workers/{worker_id}/heartbeat")
                    response.raise_for_status()
                heartbeat_retry = 1.0
                heartbeat_due = clock() + interval
            except (httpx.HTTPError, ValueError) as error:
                if (worker_id is not None and isinstance(error, httpx.HTTPStatusError)
                        and error.response.status_code == 404):
                    worker_id = None
                    poller.pending = None
                    heartbeat_due = clock()
                    continue
                if not retryable(error):
                    raise
                logger.warning("Registration/heartbeat failed: %s; retry in %ss", error, heartbeat_retry)
                heartbeat_due = clock() + heartbeat_retry
                heartbeat_retry = min(heartbeat_retry * 2, 30)
        if worker_id is not None and clock() >= poll_due:
            try:
                more = poller.step(client, worker_id)
                poll_retry = 1.0
                poll_due = clock() if more else clock() + interval
            except (httpx.HTTPError, ValueError) as error:
                if isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 404:
                    worker_id = None
                    poller.pending = None
                    heartbeat_due = clock()
                    continue
                if isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 409:
                    logger.warning("Job ownership conflict; leaving job unchanged: %s", error)
                    poller.pending = None
                    poll_due = clock() + interval
                else:
                    if not retryable(error):
                        raise
                    logger.warning("Job request failed: %s; retry in %ss", error, poll_retry)
                    poll_due = clock() + poll_retry
                    poll_retry = min(poll_retry * 2, 30)
        deadline = min(heartbeat_due, poll_due) if worker_id is not None else heartbeat_due
        if stop.wait(max(0, deadline - clock())):
            break
