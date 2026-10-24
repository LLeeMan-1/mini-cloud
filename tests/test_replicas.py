"""Replica intent, concurrent creation, and integration with existing ownership."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import IntegrityError

from backend.control_plane.api.jobs import get_clock
from backend.control_plane.persistence.models import utc_now


@pytest.fixture
def setup(client):
    now = [datetime(2026, 10, 25, tzinfo=timezone.utc)]
    client.app.dependency_overrides[utc_now] = lambda: now[0]
    client.app.dependency_overrides[get_clock] = lambda: lambda: now[0]
    app = client.post("/applications", json={"name": "replicas"}).json()
    return client, now, app["id"]


def deployment(setup, replicas=3):
    response = setup[0].post(f"/applications/{setup[2]}/deployments", json={
        "specification": {"simulated": True}, "desired_replicas": replicas})
    assert response.status_code == 201, response.text
    return response.json()["id"]


def reconcile(client, dep):
    response = client.post(f"/deployments/{dep}/reconcile")
    assert response.status_code == 200, response.text
    return response.json()


def summary(client, dep):
    response = client.get(f"/deployments/{dep}/replicas")
    assert response.status_code == 200, response.text
    return response.json()


def snapshot(client):
    with client.app.state.engine.connect() as connection:
        return {table: connection.exec_driver_sql(f"SELECT * FROM {table} ORDER BY 1").all()
                for table in ("deployments", "deployment_targets", "instances", "jobs", "job_attempts", "job_renewals")}


def claim(client, worker, item, number=1):
    body = {"attempt_number": number, "claim_id": str(uuid4())}
    prefix = f"/workers/{worker}/jobs/{item['start_job_id']}"
    response = client.post(prefix + "/claim", json=body)
    assert response.status_code == 200, response.text
    instance = response.json()["instance"]
    return prefix, {**body, "outcome": "completed", "result": {"simulated": True},
                    "instance_outcome": {"instance_id": instance["id"], "expected_version": instance["version"],
                                         "state": "healthy", "phase": "readiness_passed"}}


def worker(client):
    return client.post("/workers/register", json={"identity": str(uuid4()), "capabilities": ["start_instance"]}).json()["id"]


@pytest.mark.parametrize("count", [None, 0, 3, 100])
def test_default_and_explicit_targets_create_no_instances(setup, count):
    client, _, app = setup
    body = {"specification": {"replicas": 99}}
    if count is not None:
        body["desired_replicas"] = count
    response = client.post(f"/applications/{app}/deployments", json=body)
    assert response.status_code == 201
    dep = response.json()["id"]
    target = summary(client, dep)
    expected = 1 if count is None else count
    assert target["desired_replicas"] == expected and target["version"] == 1
    assert target["missing_indexes"] == list(range(expected))
    assert target["created_at"] == target["updated_at"]
    assert client.get("/instances").json() == []
    assert [job["kind"] for job in client.get("/jobs").json()] == ["prepare_deployment"]
    assert response.json()["specification"] == {"replicas": 99}


@pytest.mark.parametrize("value", [-1, 101, True, 1.0, "3", None])
def test_strict_target_validation_on_create_and_update(setup, value):
    client, _, app = setup
    assert client.post(f"/applications/{app}/deployments", json={
        "specification": {"simulated": True}, "desired_replicas": value}).status_code == 422
    dep = deployment(setup)
    assert client.put(f"/deployments/{dep}/replicas", json={
        "desired_replicas": value, "expected_version": 1}).status_code == 422


def test_target_versions_noop_increase_and_scale_down(setup):
    client, now, _ = setup
    dep = deployment(setup, 1)
    route = f"/deployments/{dep}/replicas"
    before = summary(client, dep)
    assert client.put(route, json={"desired_replicas": 1, "expected_version": 1}).status_code == 200
    assert summary(client, dep) == before
    now[0] += timedelta(seconds=1)
    response = client.put(route, json={"desired_replicas": 3, "expected_version": 1})
    assert response.status_code == 200
    assert response.json()["version"] == 2 and response.json()["updated_at"] != before["updated_at"]
    assert client.put(route, json={"desired_replicas": 4, "expected_version": 1}).json()["detail"]["code"] == "target_version_conflict"
    assert client.put(route, json={"desired_replicas": 2, "expected_version": 2}).json()["detail"]["code"] == "scale_down_not_supported"
    assert client.put(route, json={"desired_replicas": 4, "expected_version": True}).status_code == 422
    assert client.put(route, json={"desired_replicas": 4, "expected_version": 2, "extra": 1}).status_code == 422
    assert client.get("/instances").json() == []


def test_three_replicas_repeated_reconciliation_and_legacy_slot_zero(setup):
    client = setup[0]
    dep = deployment(setup)
    result = reconcile(client, dep)
    assert result["created_count"] == 3
    assert [item["replica_index"] for item in result["created"]] == [0, 1, 2]
    assert len({item["instance_id"] for item in result["created"]}) == 3
    assert len({item["start_job_id"] for item in result["created"]}) == 3
    before = snapshot(client)
    assert reconcile(client, dep)["created_count"] == 0
    legacy = client.post(f"/deployments/{dep}/instances")
    assert legacy.status_code == 200 and legacy.json()["id"] == result["created"][0]["instance_id"]
    assert snapshot(client) == before
    assert summary(client, dep)["lifecycle_counts"]["pending"] == 3
    assert len(client.get("/jobs").json()) == 4


def test_zero_target_and_later_increase(setup):
    client = setup[0]
    dep = deployment(setup, 0)
    assert reconcile(client, dep)["created_count"] == 0
    response = client.post(f"/deployments/{dep}/instances")
    assert response.status_code == 409 and response.json()["detail"]["code"] == "no_desired_replica"
    assert client.put(f"/deployments/{dep}/replicas", json={"desired_replicas": 3, "expected_version": 1}).status_code == 200
    assert reconcile(client, dep)["created_count"] == 3


def test_concurrent_reconciliation_and_slot_zero_creation(setup):
    client = setup[0]
    dep = deployment(setup)
    routes = [f"/deployments/{dep}/reconcile"] * 4 + [f"/deployments/{dep}/instances"] * 3
    with ThreadPoolExecutor(max_workers=7) as executor:
        responses = list(executor.map(client.post, routes))
    assert all(response.status_code in (200, 201) for response in responses)
    slots = client.get("/instances", params={"deployment_id": dep}).json()
    assert sorted(item["replica_index"] for item in slots) == [0, 1, 2]
    assert len(client.get("/jobs").json()) == 4
    assert reconcile(client, dep)["created_count"] == 0


def test_target_increase_races_reconciliation(setup):
    client = setup[0]
    dep = deployment(setup, 1)
    with ThreadPoolExecutor(max_workers=2) as executor:
        creation = executor.submit(client.post, f"/deployments/{dep}/reconcile")
        update = executor.submit(client.put, f"/deployments/{dep}/replicas", json={"desired_replicas": 3, "expected_version": 1})
        assert creation.result().status_code == update.result().status_code == 200
    reconcile(client, dep)
    assert summary(client, dep)["materialized_replicas"] == 3
    assert len(client.get("/jobs").json()) == 4


def test_concurrent_target_updates_accept_only_one_version(setup):
    client = setup[0]
    dep = deployment(setup, 1)
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda count: client.put(f"/deployments/{dep}/replicas", json={
            "desired_replicas": count, "expected_version": 1}), [3, 4]))
    assert sorted(response.status_code for response in responses) == [200, 409]
    assert summary(client, dep)["version"] == 2
    assert client.get("/instances").json() == []


def test_maximum_target_materializes_exactly_one_hundred_slots(setup):
    client = setup[0]
    dep = deployment(setup, 100)
    result = reconcile(client, dep)
    assert result["created_count"] == 100
    assert [item["replica_index"] for item in result["created"]] == list(range(100))
    assert reconcile(client, dep)["created_count"] == 0
    assert summary(client, dep)["missing_indexes"] == []


def test_reconciliation_races_startup_report_without_partial_state(setup):
    client = setup[0]
    dep = deployment(setup, 1)
    slot = reconcile(client, dep)["created"][0]
    worker_id = worker(client)
    client.post("/scheduling/run")
    prefix, payload = claim(client, worker_id, slot)
    with ThreadPoolExecutor(max_workers=2) as executor:
        report = executor.submit(client.post, prefix + "/report", json=payload)
        reconciliation = executor.submit(client.post, f"/deployments/{dep}/reconcile")
        assert report.result().status_code == 200
        assert reconciliation.result().json()["created_count"] == 0
    assert summary(client, dep)["lifecycle_counts"]["healthy"] == 1


def test_expired_assignment_is_retained_until_explicit_recovery(setup):
    client, now, _ = setup
    dep = deployment(setup, 1)
    slot = reconcile(client, dep)["created"][0]
    worker(client)
    client.post("/scheduling/run")
    now[0] += timedelta(seconds=15)
    before = snapshot(client)
    assert summary(client, dep)["expired_ownership_replicas"] == 1
    assert reconcile(client, dep)["created_count"] == 0
    assert snapshot(client) == before
    assert client.get(f"/instances/{slot['instance_id']}").json()["state"] == "pending"
    assert client.post("/jobs/recover").json()["recovered_count"] == 1
    assert reconcile(client, dep)["created_count"] == 0


@pytest.mark.parametrize("table", ["deployment_targets", "jobs"])
def test_deployment_target_and_prepare_job_creation_roll_back_together(setup, table):
    client, _, app = setup
    def fail(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO " + table + " "):
            raise RuntimeError("injected deployment failure")
    event.listen(client.app.state.engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="injected deployment failure"):
            client.post(f"/applications/{app}/deployments", json={"specification": {"simulated": True}})
    finally:
        event.remove(client.app.state.engine, "before_cursor_execute", fail)
    assert all(not rows for rows in snapshot(client).values())


@pytest.mark.parametrize("table", ["jobs", "instances"])
def test_reconciliation_failure_rolls_back_all_slots(setup, table):
    client = setup[0]
    dep = deployment(setup)
    before = snapshot(client)
    inserts = 0
    def fail(connection, cursor, statement, parameters, context, executemany):
        nonlocal inserts
        if statement.startswith("INSERT INTO " + table + " "):
            inserts += 1
            if inserts == 2:
                raise RuntimeError("injected slot failure")
    event.listen(client.app.state.engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="injected slot failure"):
            client.post(f"/deployments/{dep}/reconcile")
    finally:
        event.remove(client.app.state.engine, "before_cursor_execute", fail)
    assert snapshot(client) == before
    assert reconcile(client, dep)["created_count"] == 3


def test_partial_startup_retry_terminal_failure_and_stop_are_materialized(setup):
    client, now, _ = setup
    dep = deployment(setup)
    slots = reconcile(client, dep)["created"]
    worker_id = worker(client)
    client.post("/scheduling/run")
    before = snapshot(client)  # assigned job and queued jobs
    assert reconcile(client, dep)["created_count"] == 0
    assert snapshot(client) == before
    prefix, payload = claim(client, worker_id, slots[0])
    before = snapshot(client)  # claimed job
    assert reconcile(client, dep)["created_count"] == 0
    assert snapshot(client) == before
    failed = {**payload, "outcome": "failed", "result": None, "error": "simulated failure", "retryable": True,
              "instance_outcome": {**payload["instance_outcome"], "state": "failed", "phase": "readiness_failed"}}
    assert client.post(prefix + "/report", json=failed).status_code == 200
    assert summary(client, dep)["retrying_replicas"] == 1
    before = snapshot(client)
    assert reconcile(client, dep)["created_count"] == 0
    assert snapshot(client) == before
    # Use another deployment for terminal failure; all four eligible jobs will
    # receive one worker without bypassing scheduler ordering.
    terminal_dep = deployment(setup, 1)
    terminal_slot = reconcile(client, terminal_dep)["created"][0]
    for _ in range(3):
        worker(client)
    now[0] += timedelta(seconds=5)
    client.post(f"/workers/{worker_id}/heartbeat")
    client.post("/scheduling/run")
    job = client.get(f"/jobs/{terminal_slot['start_job_id']}").json()
    terminal_prefix, terminal_payload = claim(client, job["worker_id"], terminal_slot)
    terminal_failed = {**terminal_payload, "outcome": "failed", "result": None, "error": "permanent failure",
                       "instance_outcome": {**terminal_payload["instance_outcome"], "state": "failed", "phase": "startup_failed"}}
    response = client.post(terminal_prefix + "/report", json=terminal_failed)
    assert response.status_code == 200
    before = snapshot(client)
    assert reconcile(client, terminal_dep)["blocked"][0]["reason"] == "terminal_startup_failure"
    assert snapshot(client) == before
    instance = response.json()["instance"]
    assert client.post(f"/instances/{instance['id']}/stop", json={"expected_version": instance["version"]}).status_code == 200
    before = snapshot(client)
    assert reconcile(client, terminal_dep)["blocked"][0]["reason"] == "stopped"
    assert snapshot(client) == before


def test_expired_ownership_recovery_and_stale_worker_remain_existing_job(setup):
    client, now, _ = setup
    dep = deployment(setup, 1)
    slot = reconcile(client, dep)["created"][0]
    worker_id = worker(client)
    client.post("/scheduling/run")
    prefix, payload = claim(client, worker_id, slot)
    now[0] += timedelta(seconds=30)
    before = snapshot(client)
    assert summary(client, dep)["expired_ownership_replicas"] == 1
    assert reconcile(client, dep)["created_count"] == 0
    assert snapshot(client) == before
    assert client.post(prefix + "/report", json=payload).status_code == 409
    assert client.post("/jobs/recover").json()["recovered_count"] == 1
    now[0] += timedelta(seconds=5)
    second_worker = worker(client)
    client.post("/scheduling/run")
    next_prefix, next_payload = claim(client, second_worker, slot, 2)
    assert client.post(prefix + "/report", json=payload).status_code == 409
    assert client.post(next_prefix + "/report", json=next_payload).status_code == 200
    now[0] += timedelta(seconds=16)
    before = snapshot(client)
    observed = summary(client, dep)
    assert observed["unavailable_worker_replicas"] == 1
    assert observed["lifecycle_counts"]["healthy"] == 1
    assert reconcile(client, dep)["created_count"] == 0
    assert snapshot(client) == before
    assert client.get(f"/jobs/{slot['start_job_id']}/attempts").json()[1]["state"] == "completed"


@pytest.mark.parametrize("corruption", ["orphan_job", "wrong_kind", "wrong_state", "missing_target"])
def test_inconsistent_records_reject_creation_without_repairs(setup, corruption):
    client = setup[0]
    dep = deployment(setup)
    slots = reconcile(client, dep)["created"]
    with client.app.state.engine.begin() as connection:
        if corruption == "orphan_job":
            connection.execute(text("INSERT INTO jobs (deployment_id,kind,state,created_at,next_run_at,attempt_count,max_attempts) VALUES (:dep,'start_instance','queued',:now,:now,0,3)"), {"dep": dep, "now": setup[1][0].replace(tzinfo=None).isoformat(" ")})
        elif corruption == "wrong_kind":
            connection.execute(text("UPDATE jobs SET kind='start_instance' WHERE deployment_id=:dep AND kind='prepare_deployment'"), {"dep": dep})
        elif corruption == "wrong_state":
            connection.execute(text("UPDATE instances SET state='failed',version=version+1 WHERE id=:id"), {"id": slots[0]["instance_id"]})
            # Failed/queued is legitimate, so force a placement without a matching attempt.
            connection.execute(text("UPDATE instances SET last_attempt_id=NULL,version=version+1 WHERE id=:id"), {"id": slots[0]["instance_id"]})
            connection.execute(text("UPDATE instances SET state='starting',version=version+1 WHERE id=:id"), {"id": slots[0]["instance_id"]})
        else:
            connection.execute(text("DELETE FROM deployment_targets WHERE deployment_id=:dep"), {"dep": dep})
    before = snapshot(client)
    for route in (f"/deployments/{dep}/reconcile", f"/deployments/{dep}/instances"):
        response = client.post(route)
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "replica_integrity_conflict"
    assert snapshot(client) == before


def test_sqlite_slot_uniqueness_immutability_and_target_checks(setup):
    client = setup[0]
    dep = deployment(setup)
    slot = reconcile(client, dep)["created"][0]
    with client.app.state.engine.begin() as connection:
        with pytest.raises(IntegrityError, match="invalid instance mutation"):
            connection.execute(text("UPDATE instances SET replica_index=9,version=version+1 WHERE id=:id"), {"id": slot["instance_id"]})
        with pytest.raises(IntegrityError):
            connection.execute(text("UPDATE deployment_targets SET desired_replicas=101,version=version+1 WHERE deployment_id=:dep"), {"dep": dep})
        with pytest.raises(IntegrityError):
            connection.execute(text("UPDATE deployment_targets SET desired_replicas=2,version=version+1 WHERE deployment_id=:dep"), {"dep": dep})
    # A different UUID and startup job still cannot occupy an existing slot.
    with pytest.raises(IntegrityError, match="instances.deployment_id, instances.replica_index"):
        with client.app.state.engine.begin() as connection:
            job_id = connection.execute(text(
                "INSERT INTO jobs (deployment_id,kind,state,created_at,next_run_at,attempt_count,max_attempts) "
                "SELECT deployment_id,kind,state,created_at,next_run_at,attempt_count,max_attempts "
                "FROM jobs WHERE id=:job RETURNING id"), {"job": slot["start_job_id"]}).scalar_one()
            connection.execute(text(
                "INSERT INTO instances (id,deployment_id,replica_index,start_job_id,worker_id,last_attempt_id,"
                "state,version,created_at,updated_at,last_observed_at,healthy_at,stopped_at,state_source,state_reason) "
                "SELECT :newid,deployment_id,replica_index,:job,worker_id,last_attempt_id,state,version,created_at,"
                "updated_at,last_observed_at,healthy_at,stopped_at,state_source,state_reason FROM instances WHERE id=:id"),
                {"newid": str(uuid4()), "job": job_id, "id": slot["instance_id"]})


def test_missing_resources_and_busy_database_response(setup):
    client = setup[0]
    for method, route, body in (("GET", "/deployments/999/replicas", None),
                                ("POST", "/deployments/999/reconcile", None),
                                ("PUT", "/deployments/999/replicas", {"desired_replicas": 3, "expected_version": 1})):
        assert client.request(method, route, json=body).status_code == 404
    dep = deployment(setup)
    # Make every test connection fail fast, rather than sleeping five seconds.
    def short_timeout(connection, cursor, statement, parameters, context, executemany):
        cursor.execute("PRAGMA busy_timeout=0")
    event.listen(client.app.state.engine, "before_cursor_execute", short_timeout)
    try:
        with client.app.state.engine.connect() as lock:
            lock.exec_driver_sql("BEGIN IMMEDIATE")
            try:
                response = client.post(f"/deployments/{dep}/reconcile")
                assert response.status_code == 503
                assert response.json()["detail"]["code"] == "database_busy"
            finally:
                lock.rollback()
    finally:
        event.remove(client.app.state.engine, "before_cursor_execute", short_timeout)
    assert reconcile(client, dep)["created_count"] == 3
