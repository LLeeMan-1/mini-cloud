"""HTTP-only simulated work. Specifications are never interpreted or executed."""

import logging
from time import monotonic
from uuid import uuid4

import httpx


logger = logging.getLogger(__name__)


class OwnershipLost(ValueError):
    pass


class JobPoller:
    def __init__(self, clock=monotonic):
        self.pending = None
        self.clock = clock

    def abandon_expired(self):
        if self.pending and self.pending.get("local_deadline", float("inf")) <= self.clock():
            logger.warning("Local lease deadline passed; discarding simulated work")
            self.pending = None
            return True
        return False

    def grant(self, data, request_start):
        from datetime import datetime
        if not isinstance(data, dict):
            raise ValueError("Lease grant must be an object")
        deadline = datetime.fromisoformat(data["lease_expires_at"])
        server_time = datetime.fromisoformat(data["server_time"])
        if deadline.utcoffset() is None or server_time.utcoffset() is None:
            raise ValueError("Lease grant must use aware timestamps")
        remaining = (deadline - server_time).total_seconds()
        safety = min(1.0, max(0.01, remaining / 10))
        local_deadline = request_start + remaining - safety
        if remaining <= 0 or self.clock() >= local_deadline:
            self.pending = None
            raise OwnershipLost("No safely confirmed lease remains")
        interval = float(data["renewal_interval"])
        if not 0 < interval < float("inf"):
            raise ValueError("Invalid renewal interval")
        self.pending["local_deadline"] = max(self.pending.get("local_deadline", 0), local_deadline)
        self.pending["renew_due"] = min(self.clock() + interval,
                                        self.clock() + (self.pending["local_deadline"] - self.clock()) / 2)

    def renew(self, client, worker_id):
        if self.abandon_expired():
            raise OwnershipLost("Lease expired locally")
        job = self.pending
        if "renewal_id" not in job:
            job["renewal_id"] = str(uuid4())
        body = {"claim_id": job["claim_id"], "attempt_number": job["attempt_number"],
                "renewal_id": job["renewal_id"]}
        start = self.clock()
        response = client.post(f"/workers/{worker_id}/jobs/{job['id']}/renew", json=body)
        response.raise_for_status()
        try:
            self.grant(response.json(), start)
        except (KeyError, TypeError) as error:
            raise ValueError("Invalid renewal grant") from error
        del job["renewal_id"]

    def step(self, client: httpx.Client, worker_id: int) -> bool:
        if self.abandon_expired():
            raise OwnershipLost("Lease expired locally")
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
                    or assignment["id"] <= 0 or assignment.get("kind") != "prepare_deployment"
                    or type(assignment.get("attempt_number")) is not int or assignment["attempt_number"] <= 0):
                raise ValueError("Invalid simulated assignment")
            self.pending = {"id": assignment["id"], "attempt_number": assignment["attempt_number"],
                            "claim_id": str(uuid4()), "phase": "claim"}
            return True
        job = self.pending
        prefix = f"/workers/{worker_id}/jobs/{job['id']}"
        body = {"claim_id": job["claim_id"], "attempt_number": job["attempt_number"]}
        if job["phase"] == "claim":
            start = self.clock()
            response = client.post(prefix + "/claim", json=body)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or data.get("state") != "claimed":
                raise ValueError("Claim response must confirm ownership")
            try:
                self.grant(data, start)
            except (KeyError, TypeError) as error:
                raise ValueError("Invalid claim grant") from error
            job["phase"] = "report"
            return True
        response = client.post(prefix + "/report", json={
            **body, "outcome": "completed", "result": {"simulated": True}
        })
        response.raise_for_status()
        logger.info("Completed simulated job %s attempt %s", job["id"], job["attempt_number"])
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
    poller = JobPoller(clock)
    heartbeat_due = poll_due = clock()
    heartbeat_retry = poll_retry = renewal_retry = 1.0
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
        if poller.abandon_expired():
            poll_due = clock() + interval
        if worker_id is not None and poller.pending and clock() >= poller.pending.get("renew_due", float("inf")):
            try:
                poller.renew(client, worker_id)
                renewal_retry = 1.0
            except OwnershipLost:
                poller.pending = None
                poll_due = clock() + interval
            except (httpx.HTTPError, ValueError) as error:
                if isinstance(error, httpx.HTTPStatusError) and error.response.status_code in (404, 409):
                    logger.warning("Lease rejected; abandoning attempt: %s", error)
                    poller.pending = None
                    poll_due = clock() + interval
                else:
                    if not retryable(error):
                        raise
                    logger.warning("Lease renewal uncertain: %s", error)
                    if poller.pending:
                        poller.pending["renew_due"] = min(clock() + renewal_retry, poller.pending["local_deadline"])
                    renewal_retry = min(renewal_retry * 2, 30)
        if worker_id is not None and clock() >= poll_due:
            try:
                more = poller.step(client, worker_id)
                if more and poller.pending and poller.pending["phase"] == "report":
                    # A fresh claim gets its own renewal retry budget.
                    renewal_retry = 1.0
                poll_retry = 1.0
                poll_due = clock() if more else clock() + interval
            except OwnershipLost:
                poller.pending = None
                poll_due = clock() + interval
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
        if poller.pending:
            deadline = min(deadline, poller.pending.get("renew_due", float("inf")),
                           poller.pending.get("local_deadline", float("inf")))
        if stop.wait(max(0, deadline - clock())):
            break
