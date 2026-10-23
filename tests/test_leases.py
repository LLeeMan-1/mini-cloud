from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.control_plane.api.jobs import get_clock
from backend.control_plane.main import create_app
from backend.control_plane.persistence.models import Job, JobAttempt, utc_now
from backend.control_plane.job_policy import JobPolicy


@pytest.fixture
def system(client):
    now = [datetime(2026, 10, 22, tzinfo=timezone.utc)]
    client.app.dependency_overrides[utc_now] = lambda: now[0]
    client.app.dependency_overrides[get_clock] = lambda: lambda: now[0]
    application = client.post("/applications", json={"name": "leases"}).json()
    deployment = client.post(f"/applications/{application['id']}/deployments", json={"specification": {"simulated": True}}).json()
    job_id = client.get("/jobs").json()[0]["id"]
    node = client.post("/workers/register", json={"identity": "first"}).json()
    assert client.post("/scheduling/run").json()["assigned_count"] == 1
    return client, now, job_id, node["id"], deployment


def endpoint(system, action, worker=None):
    _, _, job, node, _ = system
    return f"/workers/{node if worker is None else worker}/jobs/{job}/{action}"


def current(system):
    return system[0].get(f"/jobs/{system[2]}").json()


def claim(system, number=1, token=None, worker=None):
    body = {"attempt_number": number, "claim_id": token or str(uuid4())}
    response = system[0].post(endpoint(system, "claim", worker), json=body)
    assert response.status_code == 200, response.text
    return body, response.json()


def report(system, body, outcome="completed", retryable=False, worker=None):
    payload = {**body, "outcome": outcome, "retryable": retryable}
    payload.update({"error": "simulated transient failure"} if outcome == "failed" else {"result": {"simulated": True}})
    return system[0].post(endpoint(system, "report", worker), json=payload), payload


@pytest.mark.parametrize("seconds,expected", [(14.999999, 200), (15, 409), (15.000001, 409)])
def test_assignment_deadline_boundary(system, seconds, expected):
    client, now, *_ = system
    now[0] += timedelta(seconds=seconds)
    response = client.post(endpoint(system, "claim"), json={"attempt_number": 1, "claim_id": str(uuid4())})
    assert response.status_code == expected
    if expected == 409:
        assert response.json()["detail"]["code"] == "assignment_expired"
        assert client.get(f"/workers/{system[3]}/jobs").json() == []


@pytest.mark.parametrize("seconds,expected", [(29.999999, 200), (30, 409), (30.000001, 409)])
@pytest.mark.parametrize("operation", ["claim", "renew", "report"])
def test_execution_lease_boundary(system, seconds, expected, operation):
    body, _ = claim(system)
    client, now, *_ = system
    now[0] += timedelta(seconds=seconds)
    if operation == "renew":
        body = {**body, "renewal_id": str(uuid4())}
    if operation == "report":
        body = {**body, "outcome": "completed"}
    response = client.post(endpoint(system, operation), json=body)
    assert response.status_code == expected
    if expected == 409:
        assert response.json()["detail"]["code"] == "lease_expired"


def test_idempotent_claim_and_renewals_never_extend_twice_or_shorten(system):
    client, now, *_ = system
    body, original = claim(system)
    now[0] += timedelta(seconds=5)
    assert client.post(endpoint(system, "claim"), json=body).json()["lease_expires_at"] == original["lease_expires_at"]
    renewal = {**body, "renewal_id": str(uuid4())}
    first = client.post(endpoint(system, "renew"), json=renewal)
    assert first.status_code == 200
    first_deadline = first.json()["lease_expires_at"]
    now[0] += timedelta(seconds=5)
    second = client.post(endpoint(system, "renew"), json={**body, "renewal_id": str(uuid4())})
    assert second.status_code == 200
    second_deadline = second.json()["lease_expires_at"]
    assert second_deadline > first_deadline
    replay = client.post(endpoint(system, "renew"), json=renewal)
    assert replay.json()["lease_expires_at"] == first_deadline
    assert current(system)["lease_expires_at"] == second_deadline


@pytest.mark.parametrize("claimed", [False, True])
def test_recovery_releases_capacity_and_retries_only_after_delay(system, claimed):
    client, now, job, node, _ = system
    if claimed:
        claim(system)
    now[0] += timedelta(seconds=30 if claimed else 15)
    recovered = client.post("/jobs/recover").json()
    assert recovered["recovered_count"] == 1
    queued = current(system)
    assert queued["state"] == "queued" and queued["worker_id"] is None
    assert queued["attempt_count"] == 1
    assert client.post("/jobs/recover").json()["recovered_count"] == 0
    client.post(f"/workers/{node}/heartbeat")
    assert client.post("/scheduling/run").json()["assigned_count"] == 0
    now[0] += timedelta(seconds=5)
    assert client.post("/scheduling/run").json()["assignments"] == [{"job_id": job, "worker_id": node, "attempt_number": 2}]
    attempts = client.get(f"/jobs/{job}/attempts").json()
    assert [attempt["state"] for attempt in attempts] == ["expired", "assigned"]
    assert attempts[0]["failure_code"] == ("lease_expired" if claimed else "assignment_expired")


def test_stale_attempts_rejected_even_after_same_worker_reassignment(system):
    body, _ = claim(system)
    client, now, _, node, _ = system
    now[0] += timedelta(seconds=30)
    client.post("/jobs/recover")
    now[0] += timedelta(seconds=5)
    client.post(f"/workers/{node}/heartbeat")
    client.post("/scheduling/run")
    new_body, _ = claim(system, number=2)
    before = current(system)
    for operation, payload in (
        ("claim", body), ("renew", {**body, "renewal_id": str(uuid4())}),
        ("report", {**body, "outcome": "completed"}),
    ):
        response = client.post(endpoint(system, operation), json=payload)
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "stale_attempt"
    assert current(system) == before
    assert report(system, new_body)[0].status_code == 200


def test_another_worker_can_finish_after_lease_expiry(system):
    old_body, _ = claim(system)
    client, now, job, _, deployment = system
    now[0] += timedelta(seconds=30)
    client.post("/jobs/recover")
    now[0] += timedelta(seconds=5)
    other = client.post("/workers/register", json={"identity": "second"}).json()["id"]
    assert client.post("/scheduling/run").json()["assigned_count"] == 1
    assert client.post(endpoint(system, "report"), json={**old_body, "outcome": "completed"}).status_code == 409
    body, _ = claim(system, number=2, worker=other)
    assert report(system, body, worker=other)[0].status_code == 200
    assert current(system)["state"] == "completed"
    assert client.get("/deployments").json() == [deployment]


@pytest.mark.parametrize("failure_type", ["assignment", "lease", "reported"])
def test_retry_exhaustion_and_delays(system, failure_type):
    client, now, _, node, _ = system
    for number in range(1, 4):
        body = None
        if failure_type != "assignment":
            body, _ = claim(system, number=number)
        if failure_type == "reported":
            assert report(system, body, "failed", True)[0].status_code == 200
        else:
            now[0] += timedelta(seconds=15 if failure_type == "assignment" else 30)
            assert client.post("/jobs/recover").json()["recovered_count"] == 1
        assert current(system)["attempt_count"] == number
        if number < 3:
            assert current(system)["state"] == "queued"
            due = datetime.fromisoformat(current(system)["next_run_at"])
            assert (due - now[0]).total_seconds() == (5 if number == 1 else 10)
            now[0] = due
            client.post(f"/workers/{node}/heartbeat")
            assert client.post("/scheduling/run").json()["assigned_count"] == 1
    assert current(system)["state"] == "failed"
    assert current(system)["last_failure_code"] == "attempts_exhausted"
    assert client.post("/scheduling/run").json()["assigned_count"] == 0
    assert len(client.get(f"/jobs/{system[2]}/attempts").json()) == 3


def test_permanent_failure_never_retries_and_history_is_immutable(system):
    body, _ = claim(system)
    client, now, job, _, _ = system
    assert report(system, body, "failed")[0].status_code == 200
    now[0] += timedelta(hours=1)
    assert client.post("/scheduling/run").json()["assigned_count"] == 0
    assert current(system)["last_failure_code"] == "permanent_failure"
    with Session(client.app.state.engine) as session:
        attempt = session.scalar(select(JobAttempt).where(JobAttempt.job_id == job))
        attempt.error = "rewrite history"
        with pytest.raises(IntegrityError, match="invalid attempt mutation"):
            session.commit()


def test_historical_report_ack_survives_new_attempt_and_restart(system, tmp_path):
    client, now, job, node, _ = system
    body, _ = claim(system)
    accepted, payload = report(system, body, "failed", True)
    assert accepted.status_code == 200
    receipt = accepted.json()
    assert receipt["state"] == "queued" and receipt["attempt_outcome"] == "failed"
    now[0] += timedelta(seconds=5)
    client.post("/scheduling/run")
    new_body, _ = claim(system, number=2)
    before = current(system)
    assert client.post(endpoint(system, "report"), json=payload).json() == receipt
    assert current(system) == before
    with TestClient(create_app(client.app.state.engine.url.database)) as restarted:
        assert restarted.post(endpoint(system, "report"), json=payload).json() == receipt
    assert client.post(endpoint(system, "report"), json={**payload, "error": "different"}).status_code == 409
    assert report(system, new_body)[0].status_code == 200


def test_concurrent_recovery_and_scheduling_do_not_duplicate_attempts(system):
    client, now, job, node, _ = system
    now[0] += timedelta(seconds=15)
    with ThreadPoolExecutor(max_workers=6) as executor:
        responses = list(executor.map(lambda i: client.post("/jobs/recover" if i % 2 else "/scheduling/run"), range(6)))
    assert all(response.status_code == 200 for response in responses)
    assert current(system)["attempt_count"] == 1
    now[0] += timedelta(seconds=5)
    client.post(f"/workers/{node}/heartbeat")
    with ThreadPoolExecutor(max_workers=4) as executor:
        runs = list(executor.map(lambda _: client.post("/scheduling/run").json(), range(4)))
    assert sum(run["assigned_count"] for run in runs) == 1
    assert len(client.get(f"/jobs/{job}/attempts").json()) == 2


@pytest.mark.parametrize("at_expiry", [False, True])
@pytest.mark.parametrize("operation", ["renew", "report"])
def test_worker_and_recovery_race(system, at_expiry, operation):
    body, _ = claim(system)
    client, now, *_ = system
    now[0] += timedelta(seconds=30 if at_expiry else 29)
    payload = {**body, "renewal_id": str(uuid4())} if operation == "renew" else {**body, "outcome": "completed"}
    with ThreadPoolExecutor(max_workers=2) as executor:
        work = executor.submit(client.post, endpoint(system, operation), json=payload)
        recovery = executor.submit(client.post, "/jobs/recover")
        response, recovered = work.result(), recovery.result()
    assert recovered.status_code == 200
    assert response.status_code == (409 if at_expiry else 200)
    assert current(system)["state"] == ("queued" if at_expiry else "claimed" if operation == "renew" else "completed")


def test_concurrent_renewal_retries_grant_once(system):
    body, _ = claim(system)
    client, now, *_ = system
    now[0] += timedelta(seconds=10)
    renewal = {**body, "renewal_id": str(uuid4())}
    with ThreadPoolExecutor(max_workers=4) as executor:
        responses = list(executor.map(lambda _: client.post(endpoint(system, "renew"), json=renewal), range(4)))
    assert all(response.status_code == 200 for response in responses)
    assert len({response.json()["lease_expires_at"] for response in responses}) == 1


def test_recovery_rollback_preserves_active_attempt(system):
    client, now, *_ = system
    now[0] += timedelta(seconds=15)
    before = current(system)

    def fail(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE job_attempts"):
            raise RuntimeError("Injected recovery failure")

    event.listen(client.app.state.engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="Injected recovery"):
            client.post("/jobs/recover")
    finally:
        event.remove(client.app.state.engine, "before_cursor_execute", fail)
    assert current(system) == before
    assert client.post("/jobs/recover").json()["recovered_count"] == 1


def test_new_protocol_requires_attempt_number(system):
    assert system[0].post(endpoint(system, "claim"), json={"claim_id": str(uuid4())}).status_code == 422


def test_policy_validation_and_backoff():
    policy = JobPolicy()
    assert [policy.retry_delay(i) for i in range(1, 7)] == [5, 10, 20, 40, 60, 60]
    for values in ({"lease_seconds": 0}, {"renewal_seconds": 30}, {"max_attempts": 1.5}, {"retry_seconds": float("inf")}):
        with pytest.raises(ValueError):
            JobPolicy(**values)


def test_report_idempotency_preserves_json_types(system):
    body, _ = claim(system)
    client = system[0]
    payload = {**body, "outcome": "completed", "result": {"flag": True}}
    assert client.post(endpoint(system, "report"), json=payload).status_code == 200
    assert client.post(endpoint(system, "report"), json={**payload, "result": {"flag": 1}}).status_code == 409
