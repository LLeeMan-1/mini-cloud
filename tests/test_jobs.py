from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.control_plane.api.jobs import get_clock
from backend.control_plane.main import create_app
from backend.control_plane.persistence.models import Job, utc_now
from backend.control_plane.services import scheduling


@pytest.fixture
def clock(client):
    time = [datetime(2026, 10, 20, tzinfo=timezone.utc)]
    client.app.dependency_overrides[utc_now] = lambda: time[0]
    client.app.dependency_overrides[get_clock] = lambda: lambda: time[0]
    return time


def deployment(client):
    application = client.post("/applications", json={"name": "demo"}).json()
    response = client.post(f"/applications/{application['id']}/deployments",
                           json={"specification": {"image": "never-executed:v1"}})
    assert response.status_code == 201
    return response.json()


def worker(client, identity="worker-1"):
    response = client.post("/workers/register", json={"identity": identity})
    assert response.status_code == 200
    return response.json()


def get_job(client, deployment_id):
    response = client.get("/jobs", params={"deployment_id": deployment_id})
    assert response.status_code == 200
    return response.json()[0]


def assigned(client):
    dep = deployment(client)
    node = worker(client)
    assert client.post("/scheduling/run").json()["assigned_count"] == 1
    job = get_job(client, dep["id"])
    prefix = f"/workers/{node['id']}/jobs/{job['id']}"
    return dep, node, job, prefix


def test_creation_poll_claim_complete_and_idempotent_retries(client, clock):
    dep, node, job, prefix = assigned(client)
    assert job["state"] == "assigned"
    assert job["claim_id"] is None
    assignments = client.get(f"/workers/{node['id']}/jobs").json()
    assert assignments == [{"id": job["id"], "deployment_id": dep["id"],
                            "kind": "prepare_deployment", "state": "assigned",
                            "specification": dep["specification"], "attempt_number": 1,
                            "assignment_expires_at": job["assignment_expires_at"]}]
    token = str(uuid4())
    claim = client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": token})
    assert claim.status_code == 200
    assert claim.json()["state"] == "claimed"
    assert client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": token}).json() == claim.json()
    assert client.get(f"/workers/{node['id']}/jobs").json() == []
    report = {"attempt_number": 1, "claim_id": token, "outcome": "completed", "result": {"simulated": True}}
    completed = client.post(prefix + "/report", json=report)
    assert completed.status_code == 200
    assert completed.json()["state"] == "completed"
    assert client.post(prefix + "/report", json=report).json() == completed.json()
    assert client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": token}).status_code == 409
    assert client.get("/deployments").json() == [dep]
    assert client.get(f"/jobs/{job['id']}").json() == {k: v for k, v in completed.json().items() if k not in ("acknowledged_attempt_number", "attempt_outcome")}
    for field in ("created_at", "assigned_at", "claimed_at", "finished_at"):
        assert datetime.fromisoformat(completed.json()[field]).tzinfo is not None


def test_new_deployment_has_exactly_one_queued_job(client):
    dep = deployment(client)
    job = get_job(client, dep["id"])
    assert job["state"] == "queued"
    assert job["worker_id"] is None
    assert job["result"] is None and job["error"] is None
    with Session(client.app.state.engine) as session:
        session.add(Job(deployment_id=dep["id"]))
        with pytest.raises(IntegrityError, match="UNIQUE"):
            session.commit()


def test_deployment_and_job_creation_roll_back_together(client):
    app = client.post("/applications", json={"name": "atomic"}).json()

    def fail_job(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO jobs"):
            raise RuntimeError("Simulated job persistence failure")

    event.listen(client.app.state.engine, "before_cursor_execute", fail_job)
    try:
        with pytest.raises(RuntimeError, match="Simulated job"):
            client.post(f"/applications/{app['id']}/deployments", json={"specification": {"test": True}})
    finally:
        event.remove(client.app.state.engine, "before_cursor_execute", fail_job)
    assert client.get("/deployments").json() == []
    assert client.get("/jobs").json() == []


def test_scheduler_fifo_tiebreak_limits_capacity_and_no_automatic_runs(client, clock):
    deps = [deployment(client) for _ in range(3)]
    nodes = [worker(client, str(i)) for i in range(2)]
    assert all(job["state"] == "queued" for job in client.get("/jobs").json())
    # Force equal creation timestamps to test the ID tie breaker.
    with client.app.state.engine.begin() as connection:
        connection.execute(text("UPDATE jobs SET created_at = '2026-10-20 00:00:00.000000', next_run_at = '2026-10-20 00:00:00.000000'"))
    assert client.post("/scheduling/run?limit=1").json()["assignments"] == [
        {"job_id": get_job(client, deps[0]["id"])["id"], "worker_id": nodes[0]["id"], "attempt_number": 1}]
    assert client.post("/scheduling/run").json()["assignments"] == [
        {"job_id": get_job(client, deps[1]["id"])["id"], "worker_id": nodes[1]["id"], "attempt_number": 1}]
    assert client.post("/scheduling/run").json()["assigned_count"] == 0
    assert get_job(client, deps[2]["id"])["state"] == "queued"


def test_scheduler_skips_stale_workers_and_keeps_timeout_boundary(client, clock):
    deployment(client)
    node = worker(client)
    clock[0] += timedelta(seconds=15)
    assert client.post("/scheduling/run").json()["assigned_count"] == 1
    deployment(client)
    worker(client, "stale")
    clock[0] += timedelta(seconds=15, microseconds=1)
    assert client.post("/scheduling/run").json()["assigned_count"] == 0
    assert client.get("/workers").json()[0]["id"] == node["id"]


def test_simultaneous_schedulers_do_not_duplicate_assignments(client, clock):
    for _ in range(8):
        deployment(client)
    for i in range(4):
        worker(client, str(i))
    with ThreadPoolExecutor(max_workers=4) as executor:
        runs = list(executor.map(lambda _: client.post("/scheduling/run").json(), range(4)))
    all_assignments = [assignment for run in runs for assignment in run["assignments"]]
    assert len(all_assignments) == 4
    assert len({item["worker_id"] for item in all_assignments}) == 4
    assert len({item["job_id"] for item in all_assignments}) == 4


def test_clock_and_eligibility_reads_happen_after_write_lock(client, clock):
    deployment(client)
    worker(client)
    statements = []

    def observe(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    def checked_clock():
        assert statements[0] == "BEGIN IMMEDIATE"
        return clock[0]

    event.listen(client.app.state.engine, "before_cursor_execute", observe)
    try:
        with Session(client.app.state.engine) as session:
            assert len(scheduling.schedule_jobs(session, 15, clock=checked_clock)) == 1
    finally:
        event.remove(client.app.state.engine, "before_cursor_execute", observe)


@pytest.mark.parametrize("same_claim", [False, True])
def test_atomic_concurrent_claims(client, clock, same_claim):
    _, _, _, prefix = assigned(client)
    tokens = [str(uuid4()) for _ in range(4)]
    if same_claim:
        tokens = [tokens[0]] * 4
    with ThreadPoolExecutor(max_workers=4) as executor:
        responses = list(executor.map(lambda token: client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": token}), tokens))
    assert [response.status_code for response in responses].count(200) == (4 if same_claim else 1)
    assert all(response.status_code in (200, 409) for response in responses)
    accepted = [response.json() for response in responses if response.status_code == 200]
    assert len({item["claim_id"] for item in accepted}) == 1


def test_wrong_owners_invalid_states_and_contradictory_reports(client, clock):
    _, node, job, prefix = assigned(client)
    other = worker(client, "other")
    wrong = f"/workers/{other['id']}/jobs/{job['id']}"
    token = str(uuid4())
    claim = {"attempt_number": 1, "claim_id": token}
    report = {**claim, "outcome": "completed", "result": {"simulated": True}}
    assert client.post(wrong + "/claim", json=claim).status_code == 409
    assert client.post(prefix + "/report", json=report).status_code == 409
    assert client.post(prefix + "/claim", json=claim).status_code == 200
    assert client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": str(uuid4())}).status_code == 409
    assert client.post(wrong + "/report", json=report).status_code == 409
    assert client.post(prefix + "/report", json={**report, "attempt_number": 1, "claim_id": str(uuid4())}).status_code == 409
    assert client.post(prefix + "/report", json=report).status_code == 200
    assert client.post(prefix + "/report", json={**report, "result": {"changed": True}}).status_code == 409
    assert client.post(prefix + "/report", json={**claim, "outcome": "failed", "error": "different"}).status_code == 409


def test_failed_simulation_is_terminal_and_does_not_change_runtime(client, clock):
    dep, _, _, prefix = assigned(client)
    token = str(uuid4())
    client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": token})
    report = {"attempt_number": 1, "claim_id": token, "outcome": "failed", "error": "simulated failure"}
    first = client.post(prefix + "/report", json=report)
    assert first.status_code == 200 and first.json()["state"] == "failed"
    assert client.post(prefix + "/report", json=report).json() == first.json()
    assert client.get("/deployments").json() == [dep]


def test_expired_heartbeat_does_not_revoke_live_claim(client, clock):
    _, _, _, prefix = assigned(client)
    token = str(uuid4())
    body = {"attempt_number": 1, "claim_id": token}
    assert client.post(prefix + "/claim", json=body).status_code == 200
    clock[0] += timedelta(seconds=16)
    assert client.post(prefix + "/claim", json=body).status_code == 200
    assert client.post(prefix + "/report", json={**body, "outcome": "completed"}).status_code == 200


@pytest.mark.parametrize("claimed", [False, True])
def test_scheduler_recovers_disappeared_worker_job(client, clock, claimed):
    _, _, job, prefix = assigned(client)
    if claimed:
        client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": str(uuid4())})
    clock[0] += timedelta(seconds=100)
    worker(client, "replacement")
    assert client.post("/scheduling/run").json()["assigned_count"] == 0
    assert client.get(f"/jobs/{job['id']}").json()["state"] == "queued"


def test_active_capacity_constraint_and_transition_trigger(client, clock):
    _, node, job, _ = assigned(client)
    second = get_job(client, deployment(client)["id"])
    with client.app.state.engine.begin() as connection:
        with pytest.raises(IntegrityError, match="UNIQUE"):
            connection.execute(text("UPDATE jobs SET state='assigned', worker_id=:worker, assigned_at=:now, assignment_expires_at=:now, attempt_count=1 WHERE id=:job"),
                               {"worker": node["id"], "now": "2026-10-20 00:00:00.000000", "job": second["id"]})
    with client.app.state.engine.begin() as connection:
        with pytest.raises(IntegrityError, match="invalid job state transition"):
            connection.execute(text("UPDATE jobs SET state='completed' WHERE id=:job"), {"job": job["id"]})


@pytest.mark.parametrize("body", [
    {}, {"attempt_number": 1, "claim_id": "bad", "outcome": "completed"},
    {"outcome": "queued"}, {"outcome": "failed"},
    {"outcome": "failed", "error": " "},
    {"outcome": "completed", "error": "wrong"},
    {"outcome": "completed", "result": {"too_large": "x" * 17000}},
    {"outcome": "completed", "extra": True},
])
def test_invalid_reports(client, body):
    body = {"attempt_number": 1, "claim_id": str(uuid4()), **body}
    assert client.post("/workers/1/jobs/1/report", json=body).status_code == 422


def test_missing_records_and_invalid_filters(client):
    assert client.get("/jobs/999").status_code == 404
    assert client.get("/workers/999/jobs").status_code == 404
    assert client.post("/workers/999/jobs/999/claim", json={"attempt_number": 1, "claim_id": str(uuid4())}).status_code == 404
    for path in ("/jobs?state=bad", "/jobs?worker_id=0", "/jobs?limit=101", "/jobs?offset=-1"):
        assert client.get(path).status_code == 422
    assert client.post("/scheduling/run?limit=101").status_code == 422


def test_claim_id_cannot_be_reused_for_a_different_job(client, clock):
    _, _, _, prefix = assigned(client)
    token = str(uuid4())
    client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": token})
    client.post(prefix + "/report", json={"attempt_number": 1, "claim_id": token, "outcome": "completed"})
    dep = deployment(client)
    client.post("/scheduling/run")
    next_job = get_job(client, dep["id"])
    response = client.post(f"/workers/{next_job['worker_id']}/jobs/{next_job['id']}/claim", json={"attempt_number": 1, "claim_id": token})
    assert response.status_code == 409


def test_state_survives_restart_and_lost_claim_response(tmp_path):
    path = tmp_path / "jobs.db"
    token = str(uuid4())
    with TestClient(create_app(path)) as first:
        dep, _, job, prefix = assigned(first)
        claim = first.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": token}).json()
    with TestClient(create_app(path)) as second:
        replay = second.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": token})
        assert replay.status_code == 200
        assert replay.json()["lease_expires_at"] == claim["lease_expires_at"]
        report = {"attempt_number": 1, "claim_id": token, "outcome": "completed", "result": {"simulated": True}}
        assert second.post(prefix + "/report", json=report).status_code == 200
        assert second.get("/deployments").json() == [dep]


def test_queued_job_cannot_be_claimed_or_reported(client, clock):
    dep = deployment(client)
    node = worker(client)
    job = get_job(client, dep["id"])
    prefix = f"/workers/{node['id']}/jobs/{job['id']}"
    token = str(uuid4())
    assert client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": token}).status_code == 409
    assert client.post(prefix + "/report", json={"attempt_number": 1, "claim_id": token, "outcome": "completed"}).status_code == 409
    assert client.get(f"/jobs/{job['id']}").json()["state"] == "queued"


@pytest.mark.parametrize("operation", ["claim", "report"])
def test_ownership_write_failure_rolls_back_and_allows_retry(client, clock, operation):
    dep, _, job, prefix = assigned(client)
    token = str(uuid4())
    if operation == "report":
        assert client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": token}).status_code == 200
    before = client.get(f"/jobs/{job['id']}").json()

    def fail_after_update(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE jobs"):
            raise RuntimeError("Failure after ownership update")

    payload = {"attempt_number": 1, "claim_id": token}
    if operation == "report":
        payload["outcome"] = "completed"
    event.listen(client.app.state.engine, "after_cursor_execute", fail_after_update)
    try:
        with pytest.raises(RuntimeError, match="Failure after ownership"):
            client.post(prefix + "/" + operation, json=payload)
    finally:
        event.remove(client.app.state.engine, "after_cursor_execute", fail_after_update)
    assert client.get(f"/jobs/{job['id']}").json() == before
    assert client.get("/deployments").json() == [dep]
    assert client.post(prefix + "/" + operation, json=payload).status_code == 200


@pytest.mark.parametrize("same_outcome", [True, False])
def test_concurrent_reports_are_idempotent_or_conflict(client, clock, same_outcome):
    _, _, job, prefix = assigned(client)
    token = str(uuid4())
    client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": token})
    reports = [
        {"attempt_number": 1, "claim_id": token, "outcome": "completed", "result": {"simulated": True}},
        {"attempt_number": 1, "claim_id": token, "outcome": "failed", "error": "simulated failure"},
    ]
    if same_outcome:
        reports[1] = reports[0]
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda report: client.post(prefix + "/report", json=report), reports))
    assert sorted(response.status_code for response in responses) == ([200, 200] if same_outcome else [200, 409])
    stored = client.get(f"/jobs/{job['id']}").json()
    assert stored["state"] in ("completed", "failed")


def test_database_capacity_includes_claimed_jobs(client, clock):
    _, node, _, prefix = assigned(client)
    client.post(prefix + "/claim", json={"attempt_number": 1, "claim_id": str(uuid4())})
    second = get_job(client, deployment(client)["id"])
    assert client.post("/scheduling/run").json()["assigned_count"] == 0
    with client.app.state.engine.begin() as connection:
        with pytest.raises(IntegrityError, match="UNIQUE"):
            connection.execute(text("UPDATE jobs SET state='assigned', worker_id=:worker, assigned_at=:now, assignment_expires_at=:now, attempt_count=1 WHERE id=:job"),
                               {"worker": node["id"], "now": "2026-10-20 00:00:00.000000", "job": second["id"]})


def test_fifo_prefers_creation_time_before_job_id(client, clock):
    first = get_job(client, deployment(client)["id"])
    second = get_job(client, deployment(client)["id"])
    worker(client)
    with client.app.state.engine.begin() as connection:
        connection.execute(text("UPDATE jobs SET created_at='2020-01-01 00:00:00.000000', next_run_at='2020-01-01 00:00:00.000000' WHERE id=:id"), {"id": second["id"]})
    assignment = client.post("/scheduling/run").json()["assignments"][0]
    assert assignment["job_id"] == second["id"]
    assert client.get(f"/jobs/{first['id']}").json()["state"] == "queued"
