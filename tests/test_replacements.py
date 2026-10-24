"""Simulation-only replacement policy, generation fencing, and atomicity."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, text
from sqlalchemy.exc import IntegrityError

from backend.control_plane.api.jobs import get_clock
from backend.control_plane.main import create_app

from test_replicas import setup, deployment, reconcile, summary, worker, claim


def records(client):
    with client.app.state.engine.connect() as connection:
        return {table: connection.exec_driver_sql(f"SELECT * FROM {table} ORDER BY 1,2").all()
                for table in ("instances", "replica_slots", "jobs", "job_attempts", "job_renewals", "replacement_receipts")}


def current(client, dep):
    result = client.get("/instances", params={"deployment_id": dep, "current": True})
    assert result.status_code == 200, result.text
    assert len(result.json()) == 1
    return result.json()[0]


def exhaust(setup, dep, mode="reported"):
    client, now, _ = setup
    instance = current(client, dep)
    worker_id = worker(client)
    reports = []
    first_claim = None
    for number in range(1, 4):
        client.post(f"/workers/{worker_id}/heartbeat")
        assert client.post("/scheduling/run").json()["assigned_count"] == 1
        worker_id = client.get(f"/jobs/{instance['start_job_id']}").json()["worker_id"]
        if mode == "unclaimed":
            now[0] += timedelta(seconds=15)
            assert client.post("/jobs/recover").json()["recovered_count"] == 1
        else:
            prefix, payload = claim(client, worker_id, instance, number)
            if first_claim is None:
                first_claim = (prefix, payload)
            if mode == "expired_first" and number == 1:
                now[0] += timedelta(seconds=30)
                assert client.post("/jobs/recover").json()["recovered_count"] == 1
            else:
                failed = {**payload, "outcome": "failed", "result": None, "retryable": mode != "permanent",
                          "error": "simulated failure", "instance_outcome": {
                              **payload["instance_outcome"], "state": "failed", "phase": "readiness_failed"}}
                response = client.post(prefix + "/report", json=failed)
                assert response.status_code == 200, response.text
                reports.append((prefix, failed, response.content))
                if mode == "permanent":
                    break
        if number < 3:
            now[0] += timedelta(seconds=5 * 2 ** (number - 1))
    assert client.get(f"/jobs/{instance['start_job_id']}").json()["state"] == "failed"
    return reports, first_claim


def initial(setup):
    dep = deployment(setup, 1)
    reconcile(setup[0], dep)
    return dep


def admin_body(instance, **overrides):
    return {"request_id": str(uuid4()), "expected_slot_version": instance["slot_version"],
            "reason": "Inspected simulated startup; replace this incarnation",
            "acknowledge_simulated_uncertainty": True, **overrides}


def replace_admin(client, instance, body=None):
    return client.post(f"/instances/{instance['id']}/replace", json=body or admin_body(instance))


@pytest.mark.parametrize("mode", ["reported", "unclaimed"])
def test_automatic_replacement_boundary_preserves_history_and_slot_identity(setup, mode):
    client, now, _ = setup
    dep = initial(setup)
    old_id = current(client, dep)["id"]
    reports, _ = exhaust(setup, dep, mode)
    old = current(client, dep)
    assert old["replacement"]["status"] == "cooldown"
    if reports:
        # The accepted final report observes the pending terminal attempt too.
        import json
        assert json.loads(reports[-1][2])["instance"]["replacement"]["status"] == "cooldown"
    assert reconcile(client, dep)["replaced_count"] == 0
    now[0] += timedelta(seconds=29, microseconds=999999)
    assert reconcile(client, dep)["replaced_count"] == 0
    before = records(client)
    now[0] += timedelta(microseconds=1)
    eligible = summary(client, dep)["replacement_eligible"]
    assert eligible[0]["replacement"]["status"] == "eligible"
    result = reconcile(client, dep)
    assert result["created_count"] == 0 and result["replaced_count"] == 1
    new = current(client, dep)
    assert new["generation"] == 2 and new["id"] != old_id
    assert new["replica_index"] == old["replica_index"] == 0
    assert new["creation_reason"] == "automatic_replacement"
    assert new["slot_version"] == 2
    assert summary(client, dep)["slots"][0]["automatic_replacements"] == 1
    assert summary(client, dep)["materialized_replicas"] == 1
    assert summary(client, dep)["lifecycle_counts"] == {"pending": 1, "starting": 0, "healthy": 0, "failed": 0, "stopped": 0}
    after = records(client)
    assert before["instances"][0] in after["instances"]
    assert all(row in after["jobs"] for row in before["jobs"])
    assert after["job_attempts"] == before["job_attempts"]
    assert reconcile(client, dep)["replaced_count"] == 0
    assert client.post(f"/deployments/{dep}/instances").json()["id"] == new["id"]
    assert client.get("/instances", params={"deployment_id": dep, "generation": 1}).json()[0]["is_current"] is False
    assert client.get("/instances", params={"deployment_id": dep, "current": False}).json()[0]["id"] == old_id
    for prefix, payload, acknowledgment in reports:
        replay_before = records(client)
        response = client.post(prefix + "/report", json=payload)
        assert response.status_code == 200 and response.content == acknowledgment
        assert records(client) == replay_before


@pytest.mark.parametrize("mode,reason", [("permanent", "permanent_startup_failure"),
                                        ("expired_first", "uncertain_execution")])
def test_ineligible_failures_stay_blocked_even_after_cooldown(setup, mode, reason):
    client, now, _ = setup
    dep = initial(setup)
    _, old_claim = exhaust(setup, dep, mode)
    now[0] += timedelta(seconds=300)
    before = records(client)
    result = reconcile(client, dep)
    assert result["replaced_count"] == 0 and result["blocked"][0]["reason"] == reason
    assert records(client) == before
    if mode == "expired_first":
        old = current(client, dep)
        assert replace_admin(client, old).status_code == 200
        prefix, payload = old_claim
        for operation, body in (("claim", {"attempt_number": 1, "claim_id": payload["claim_id"]}),
                                ("renew", {"attempt_number": 1, "claim_id": payload["claim_id"], "renewal_id": str(uuid4())}),
                                ("report", payload)):
            response = client.post(prefix + "/" + operation, json=body)
            assert response.status_code == 409
            assert response.json()["detail"]["code"] == "stale_generation"


def test_pending_retry_and_unavailable_healthy_instances_are_never_replaced(setup):
    client, now, _ = setup
    dep = initial(setup)
    item = current(client, dep)
    worker_id = worker(client)
    client.post("/scheduling/run")
    prefix, payload = claim(client, worker_id, item)
    response = client.post(prefix + "/report", json={**payload, "outcome": "failed", "result": None,
        "error": "retry", "retryable": True, "instance_outcome": {
            **payload["instance_outcome"], "state": "failed", "phase": "startup_failed"}})
    assert response.status_code == 200
    now[0] += timedelta(seconds=100)
    before = records(client)
    assert reconcile(client, dep)["replaced_count"] == 0
    assert records(client) == before
    client.post(f"/workers/{worker_id}/heartbeat")
    client.post("/scheduling/run")
    prefix, healthy = claim(client, worker_id, item, 2)
    assert client.post(prefix + "/report", json=healthy).status_code == 200
    now[0] += timedelta(seconds=100)
    before = records(client)
    assert current(client, dep)["worker_available"] is False
    assert current(client, dep)["state"] == "healthy"
    assert reconcile(client, dep)["replaced_count"] == 0
    assert records(client) == before


def test_budget_is_not_reset_by_admin_replacement_or_success(setup):
    client, now, _ = setup
    dep = initial(setup)
    exhaust(setup, dep)
    now[0] += timedelta(seconds=30)
    assert reconcile(client, dep)["replaced_count"] == 1
    new = current(client, dep)
    worker_id = worker(client)
    client.post("/scheduling/run")
    prefix, payload = claim(client, worker_id, new)
    assert client.post(prefix + "/report", json=payload).status_code == 200
    healthy = current(client, dep)
    assert client.post(f"/instances/{healthy['id']}/stop", json={"expected_version": healthy["version"]}).status_code == 200
    assert reconcile(client, dep)["blocked"][0]["reason"] == "stopped"
    assert replace_admin(client, current(client, dep)).status_code == 200
    assert current(client, dep)["generation"] == 3
    exhaust(setup, dep)
    now[0] += timedelta(seconds=30)
    before = records(client)
    result = reconcile(client, dep)
    assert result["replaced_count"] == 0
    assert result["blocked"][0]["reason"] == "automatic_budget_exhausted"
    assert records(client) == before


def test_concurrent_reconciliation_creates_one_replacement(setup):
    client, now, _ = setup
    dep = initial(setup)
    exhaust(setup, dep)
    now[0] += timedelta(seconds=30)
    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(lambda _: reconcile(client, dep), range(6)))
    assert sum(result["replaced_count"] for result in results) == 1
    assert current(client, dep)["generation"] == 2
    assert len(client.get("/instances", params={"deployment_id": dep}).json()) == 2


def test_administrative_receipt_replay_conflict_and_later_generation(setup):
    client = setup[0]
    dep = initial(setup)
    exhaust(setup, dep, "permanent")
    old = current(client, dep)
    payload = admin_body(old)
    with ThreadPoolExecutor(max_workers=4) as executor:
        responses = list(executor.map(lambda _: replace_admin(client, old, payload), range(4)))
    assert all(response.status_code == 200 for response in responses)
    assert all(response.content == responses[0].content for response in responses)
    assert current(client, dep)["generation"] == 2
    assert replace_admin(client, old, {**payload, "reason": "changed"}).status_code == 409
    assert replace_admin(client, old, admin_body(old)).status_code == 409
    exhaust(setup, dep, "permanent")
    assert replace_admin(client, current(client, dep)).status_code == 200
    before = records(client)
    assert replace_admin(client, old, payload).content == responses[0].content
    assert records(client) == before
    assert summary(client, dep)["slots"][0]["automatic_replacements"] == 0
    with client.app.state.engine.begin() as connection:
        with pytest.raises(IntegrityError, match="receipt is immutable"):
            connection.execute(text("UPDATE replacement_receipts SET payload='{}'"))
        with pytest.raises(IntegrityError, match="receipt is immutable"):
            connection.execute(text("DELETE FROM replacement_receipts"))


def test_automatic_replacement_races_administration(setup):
    client, now, _ = setup
    dep = initial(setup)
    exhaust(setup, dep)
    old = current(client, dep)
    now[0] += timedelta(seconds=30)
    with ThreadPoolExecutor(max_workers=2) as executor:
        automatic = executor.submit(client.post, f"/deployments/{dep}/reconcile")
        manual = executor.submit(replace_admin, client, old)
        assert automatic.result().status_code == 200
        assert manual.result().status_code in (200, 409)
    assert current(client, dep)["generation"] == 2
    assert len(client.get("/instances").json()) == 2


def test_distinct_admin_requests_racing_one_slot_accept_only_one(setup):
    client = setup[0]
    dep = initial(setup)
    exhaust(setup, dep, "permanent")
    old = current(client, dep)
    assert replace_admin(client, old, admin_body(old, expected_slot_version=old["slot_version"] + 1)).json()["detail"]["code"] == "slot_version_conflict"
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda _: replace_admin(client, old), range(2)))
    assert sorted(response.status_code for response in responses) == [200, 409]
    assert current(client, dep)["generation"] == 2


def test_restart_after_committed_replacements_preserves_budget_and_receipts(setup):
    client, now, _ = setup
    dep = initial(setup)
    exhaust(setup, dep)
    now[0] += timedelta(seconds=30)
    reconcile(client, dep)
    exhaust(setup, dep, "permanent")
    old = current(client, dep)
    payload = admin_body(old)
    response = replace_admin(client, old, payload)
    assert response.status_code == 200
    before = records(client)
    path = client.app.state.engine.url.database
    with TestClient(create_app(path)) as restarted:
        restarted.app.dependency_overrides[get_clock] = lambda: lambda: now[0]
        assert replace_admin(restarted, old, payload).content == response.content
        assert reconcile(restarted, dep)["replaced_count"] == 0
        assert current(restarted, dep)["generation"] == 3
        assert summary(restarted, dep)["slots"][0]["automatic_replacements"] == 1
        assert records(restarted) == before


@pytest.mark.parametrize("phase", ["pending", "assigned", "claimed", "retrying", "healthy"])
def test_admin_rejects_healthy_or_active_work(setup, phase):
    client = setup[0]
    dep = initial(setup)
    item = current(client, dep)
    if phase != "pending":
        worker_id = worker(client)
        client.post("/scheduling/run")
        if phase != "assigned":
            prefix, payload = claim(client, worker_id, item)
            if phase == "healthy":
                assert client.post(prefix + "/report", json=payload).status_code == 200
            if phase == "retrying":
                assert client.post(prefix + "/report", json={**payload, "outcome": "failed", "result": None,
                    "error": "retry", "retryable": True, "instance_outcome": {
                        **payload["instance_outcome"], "state": "failed", "phase": "startup_failed"}}).status_code == 200
    before = records(client)
    assert replace_admin(client, current(client, dep)).status_code == 409
    assert records(client) == before


@pytest.mark.parametrize("changes", [{"acknowledge_simulated_uncertainty": False},
                                    {"acknowledge_simulated_uncertainty": 1}, {"reason": " "},
                                    {"reason": "x" * 501}, {"expected_slot_version": True},
                                    {"request_id": "bad"}, {"extra": 1}])
def test_admin_validation(setup, changes):
    client = setup[0]
    dep = initial(setup)
    old = current(client, dep)
    assert replace_admin(client, old, admin_body(old, **changes)).status_code == 422


@pytest.mark.parametrize("operation,statement", [("auto", "INSERT INTO jobs "), ("auto", "INSERT INTO instances "),
                                                ("auto", "UPDATE replica_slots "), ("admin", "INSERT INTO replacement_receipts ")])
def test_replacement_failure_rolls_back_incarnation_job_pointer_and_receipt(setup, operation, statement):
    client, now, _ = setup
    dep = initial(setup)
    exhaust(setup, dep)
    now[0] += timedelta(seconds=30)
    old = current(client, dep)
    before = records(client)
    def fail(connection, cursor, sql, parameters, context, executemany):
        if sql.startswith(statement):
            raise RuntimeError("replacement interrupted")
    event.listen(client.app.state.engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="replacement interrupted"):
            if operation == "auto":
                client.post(f"/deployments/{dep}/reconcile")
            else:
                replace_admin(client, old)
    finally:
        event.remove(client.app.state.engine, "before_cursor_execute", fail)
    assert records(client) == before
    assert reconcile(client, dep)["replaced_count"] == 1


def test_healthy_report_replay_after_stop_and_replacement_is_byte_identical(setup):
    client = setup[0]
    dep = initial(setup)
    item = current(client, dep)
    worker_id = worker(client)
    client.post("/scheduling/run")
    prefix, payload = claim(client, worker_id, item)
    acknowledgment = client.post(prefix + "/report", json=payload)
    healthy = current(client, dep)
    client.post(f"/instances/{healthy['id']}/stop", json={"expected_version": healthy["version"]})
    stopped = current(client, dep)
    assert replace_admin(client, stopped).status_code == 200
    before = records(client)
    assert client.post(prefix + "/report", json=payload).content == acknowledgment.content
    assert records(client) == before
    historical = client.get(f"/instances/{stopped['id']}").json()
    assert historical["state"] == "stopped" and historical["version"] == stopped["version"]
    assert historical["stopped_at"] == stopped["stopped_at"] and not historical["is_current"]


def test_slot_pointer_integrity_and_instance_identity_are_enforced_by_sqlite(setup):
    client = setup[0]
    dep = initial(setup)
    with pytest.raises(IntegrityError, match="FOREIGN KEY"):
        with client.app.state.engine.begin() as connection:
            connection.execute(text("UPDATE replica_slots SET current_generation=2,version=version+1 WHERE deployment_id=:id"), {"id": dep})
    for field, value in (("generation", "2"), ("creation_reason", "'administrative_replacement'")):
        with pytest.raises(IntegrityError, match="invalid instance mutation"):
            with client.app.state.engine.begin() as connection:
                connection.execute(text(f"UPDATE instances SET {field}={value},version=version+1 WHERE deployment_id=:id"), {"id": dep})
    assert current(client, dep)["generation"] == 1
