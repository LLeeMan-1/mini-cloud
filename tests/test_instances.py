from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import event, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.control_plane.api.jobs import get_clock
from backend.control_plane.persistence.models import Deployment, Instance, Job, utc_now
from backend.worker.jobs import JobPoller, run_job_agent


@pytest.fixture
def setup(client):
    now = [datetime(2026, 10, 23, tzinfo=timezone.utc)]
    client.app.dependency_overrides[utc_now] = lambda: now[0]
    client.app.dependency_overrides[get_clock] = lambda: lambda: now[0]
    app = client.post("/applications", json={"name": "instances"}).json()
    dep = client.post(f"/applications/{app['id']}/deployments", json={"specification": {"image": "never-executed"}}).json()
    return client, now, dep


def create(setup):
    response = setup[0].post(f"/deployments/{setup[2]['id']}/instances")
    assert response.status_code == 201, response.text
    return response.json()


def register(client, identity="startup", capabilities=None):
    response = client.post("/workers/register", json={"identity": identity,
        "capabilities": ["start_instance"] if capabilities is None else capabilities})
    assert response.status_code == 200
    return response.json()["id"]


def started(setup):
    client, now, dep = setup
    instance = create(setup)
    worker = register(client)
    assert client.post("/scheduling/run").json()["assigned_count"] == 1
    poll = client.get(f"/workers/{worker}/jobs").json()[0]
    assert poll["instance_id"] == instance["id"]
    body = {"attempt_number": poll["attempt_number"], "claim_id": str(uuid4())}
    prefix = f"/workers/{worker}/jobs/{instance['start_job_id']}"
    response = client.post(prefix + "/claim", json=body)
    assert response.status_code == 200, response.text
    return response.json()["instance"], worker, body, prefix


def report_body(instance, body, state="healthy", retryable=False):
    result = {**body, "outcome": "completed" if state == "healthy" else "failed",
              "retryable": retryable,
              "instance_outcome": {"instance_id": instance["id"], "expected_version": instance["version"],
                  "state": state, "phase": "readiness_passed" if state == "healthy" else "readiness_failed"}}
    result.update({"result": {"simulated": True}} if state == "healthy" else {"error": "simulated probe failed"})
    return result


def test_creation_is_atomic_idempotent_and_inspectable(setup):
    client, _, dep = setup
    instance = create(setup)
    assert str(UUID(instance["id"])) == instance["id"]
    assert instance["state"] == "pending" and instance["version"] == 1
    assert instance["worker_id"] is None and instance["last_attempt_id"] is None
    assert instance["last_observed_at"] is None and instance["worker_available"] is None
    assert client.post(f"/deployments/{dep['id']}/instances", json={}).status_code == 200
    assert client.get(f"/instances/{instance['id']}").json() == instance
    assert client.get("/instances", params={"deployment_id": dep["id"], "state": "pending"}).json() == [instance]
    jobs = client.get("/jobs").json()
    assert {job["kind"] for job in jobs} == {"prepare_deployment", "start_instance"}
    assert len(jobs) == 2
    assert client.get("/deployments").json() == [dep]


def test_concurrent_creation_creates_one_instance_and_one_start_job(setup):
    client, _, dep = setup
    with ThreadPoolExecutor(max_workers=6) as executor:
        responses = list(executor.map(lambda _: client.post(f"/deployments/{dep['id']}/instances"), range(6)))
    assert [r.status_code for r in responses].count(201) == 1
    assert [r.status_code for r in responses].count(200) == 5
    assert len({r.json()["id"] for r in responses}) == 1
    assert len(client.get("/instances").json()) == 1
    assert len(client.get("/jobs").json()) == 2


def test_creation_failure_rolls_back_start_job(setup):
    client, _, _ = setup

    def fail(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO instances"):
            raise RuntimeError("instance insert failed")

    event.listen(client.app.state.engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="instance insert failed"):
            client.post(f"/deployments/{setup[2]['id']}/instances")
    finally:
        event.remove(client.app.state.engine, "before_cursor_execute", fail)
    assert client.get("/instances").json() == []
    assert [job["kind"] for job in client.get("/jobs").json()] == ["prepare_deployment"]


def test_success_stop_and_historical_report_replay_are_fenced(setup):
    client, now, dep = setup
    instance, worker, body, prefix = started(setup)
    assert instance["state"] == "starting" and instance["last_observed_at"] is None
    assert instance["current_worker_id"] == worker and instance["ownership_valid"]
    payload = report_body(instance, body)
    result = client.post(prefix + "/report", json=payload)
    assert result.status_code == 200, result.text
    healthy = result.json()["instance"]
    assert healthy["state"] == "healthy" and healthy["state_source"] == "worker_report"
    assert healthy["last_observed_at"] == healthy["healthy_at"]
    assert not healthy["ownership_valid"] and healthy["current_worker_id"] is None
    assert client.post(f"/instances/{instance['id']}/stop", json={"expected_version": instance["version"]}).status_code == 409
    stopped = client.post(f"/instances/{instance['id']}/stop", json={"expected_version": healthy["version"]})
    assert stopped.status_code == 200
    stopped_record = stopped.json()
    assert stopped_record["state"] == "stopped" and stopped_record["state_source"] == "manual"
    assert client.post(f"/instances/{instance['id']}/stop", json={"expected_version": healthy["version"]}).json() == stopped_record
    now[0] += timedelta(seconds=60)
    assert client.post(prefix + "/report", json=payload).json() == result.json()
    current = client.get(f"/instances/{instance['id']}").json()
    assert current["state"] == "stopped" and current["version"] == stopped_record["version"]
    assert current["stopped_at"] == stopped_record["stopped_at"]
    assert client.post(f"/deployments/{dep['id']}/instances").json()["id"] == instance["id"]
    assert client.get("/deployments").json() == [dep]


@pytest.mark.parametrize("state", ["pending", "starting", "retrying"])
def test_stop_rejects_active_startup(setup, state):
    client = setup[0]
    if state == "pending":
        instance = create(setup)
    else:
        instance, _, body, prefix = started(setup)
        if state == "retrying":
            response = client.post(prefix + "/report", json=report_body(instance, body, "failed", True))
            assert response.status_code == 200
            instance = response.json()["instance"]
    assert client.post(f"/instances/{instance['id']}/stop", json={"expected_version": instance["version"]}).status_code == 409


@pytest.mark.parametrize("failure", ["reported", "expired"])
def test_retry_keeps_identity_and_fences_previous_attempt(setup, failure):
    client, now, _ = setup
    instance, worker, body, prefix = started(setup)
    old_payload = report_body(instance, body)
    if failure == "reported":
        assert client.post(prefix + "/report", json=report_body(instance, body, "failed", True)).status_code == 200
    else:
        now[0] += timedelta(seconds=30)
        assert client.post("/jobs/recover").json()["recovered_count"] == 1
    failed = client.get(f"/instances/{instance['id']}").json()
    assert failed["state"] == "failed"
    if failure == "expired":
        assert failed["last_observed_at"] is None
    else:
        assert failed["last_observed_at"] is not None
    now[0] += timedelta(seconds=5)
    client.post(f"/workers/{worker}/heartbeat")
    assert client.post("/scheduling/run").json()["assigned_count"] == 1
    next_body = {"attempt_number": 2, "claim_id": str(uuid4())}
    second = client.post(prefix + "/claim", json=next_body)
    assert second.status_code == 200
    replacement = second.json()["instance"]
    assert replacement["id"] == instance["id"] and replacement["last_attempt_id"] != instance["last_attempt_id"]
    assert replacement["state"] == "starting"
    assert client.post(prefix + "/report", json=old_payload).status_code == 409
    assert client.post(prefix + "/report", json=report_body(replacement, next_body)).status_code == 200


def test_expired_claim_cannot_report_health_even_before_recovery(setup):
    client, now, _ = setup
    instance, _, body, prefix = started(setup)
    now[0] += timedelta(seconds=30)
    response = client.post(prefix + "/report", json=report_body(instance, body))
    assert response.status_code == 409 and response.json()["detail"]["code"] == "lease_expired"
    assert client.get(f"/instances/{instance['id']}").json()["state"] == "starting"


def test_outdated_version_and_wrong_instance_roll_back_everything(setup):
    client = setup[0]
    instance, _, body, prefix = started(setup)
    payload = report_body(instance, body)
    before_job = client.get(f"/jobs/{instance['start_job_id']}").json()
    for key, value in (("expected_version", instance["version"] - 1), ("instance_id", str(uuid4()))):
        invalid = {**payload, "instance_outcome": {**payload["instance_outcome"], key: value}}
        assert client.post(prefix + "/report", json=invalid).status_code == 409
        assert client.get(f"/jobs/{instance['start_job_id']}").json() == before_job
        assert client.get(f"/instances/{instance['id']}").json() == instance
    assert client.post(prefix + "/report", json=payload).status_code == 200


@pytest.mark.parametrize("failing_table", ["jobs", "job_attempts"])
def test_failure_after_instance_change_rolls_back_full_report(setup, failing_table):
    client = setup[0]
    instance, _, body, prefix = started(setup)
    before_job = client.get(f"/jobs/{instance['start_job_id']}").json()

    def fail(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE " + failing_table + " "):
            raise RuntimeError("atomic report failure")

    event.listen(client.app.state.engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="atomic report failure"):
            client.post(prefix + "/report", json=report_body(instance, body))
    finally:
        event.remove(client.app.state.engine, "before_cursor_execute", fail)
    assert client.get(f"/instances/{instance['id']}").json() == instance
    assert client.get(f"/jobs/{instance['start_job_id']}").json() == before_job
    assert client.post(prefix + "/report", json=report_body(instance, body)).status_code == 200


def test_concurrent_readiness_reports_accept_one_outcome(setup):
    client = setup[0]
    instance, _, body, prefix = started(setup)
    payloads = [report_body(instance, body), report_body(instance, body, "failed")]
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda payload: client.post(prefix + "/report", json=payload), payloads))
    assert sorted(response.status_code for response in responses) == [200, 409]
    current = client.get(f"/instances/{instance['id']}").json()
    assert current["state"] in ("healthy", "failed")
    assert current["version"] == instance["version"] + 1


def test_worker_unavailability_preserves_last_known_health(setup):
    client, now, _ = setup
    instance, _, body, prefix = started(setup)
    healthy = client.post(prefix + "/report", json=report_body(instance, body)).json()["instance"]
    now[0] += timedelta(seconds=16)
    current = client.get(f"/instances/{instance['id']}").json()
    assert current["state"] == "healthy" and current["worker_available"] is False
    assert current["version"] == healthy["version"]
    assert current["last_observed_at"] == healthy["last_observed_at"]


def test_capabilities_skip_incompatible_jobs_and_preserve_legacy_defaults(setup):
    client, _, _ = setup
    instance = create(setup)
    legacy = client.post("/workers/register", json={"identity": "legacy"}).json()
    assert legacy["capabilities"] == ["prepare_deployment"]
    assert client.post("/scheduling/run").json()["assigned_count"] == 1
    legacy_poll = client.get(f"/workers/{legacy['id']}/jobs").json()
    assert legacy_poll[0]["kind"] == "prepare_deployment"
    startup = register(client)
    assert client.post("/scheduling/run").json()["assignments"][0]["worker_id"] == startup
    assert client.get(f"/workers/{startup}/jobs").json()[0]["instance_id"] == instance["id"]
    assert client.get(f"/instances/{instance['id']}").json()["state"] == "pending"


@pytest.mark.parametrize("failure_phase", [None, "startup", "readiness"])
def test_http_worker_distinguishes_startup_and_readiness(setup, failure_phase):
    client, now, _ = setup
    instance = create(setup)
    worker = register(client)
    client.post("/scheduling/run")
    poller = JobPoller(clock=lambda: 0)
    if failure_phase == "startup":
        poller.simulate_start = lambda _: False
    if failure_phase == "readiness":
        poller.simulate_readiness = lambda _: False

    def transport(request):
        import json
        response = client.request(request.method, request.url.path,
                                  json=json.loads(request.content) if request.content else None)
        return httpx.Response(response.status_code, json=response.json())

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(transport)) as http:
        assert poller.step(http, worker)  # poll
        assert poller.step(http, worker)  # claim
        assert client.get(f"/instances/{instance['id']}").json()["state"] == "starting"
        assert poller.step(http, worker)  # startup, not health
        assert client.get(f"/instances/{instance['id']}").json()["state"] == "starting"
        if failure_phase != "startup":
            assert poller.pending["phase"] == "readiness"
            assert poller.step(http, worker)  # separate readiness simulation
            assert client.get(f"/instances/{instance['id']}").json()["state"] == "starting"
        assert poller.step(http, worker) is False  # atomic final report
    final = client.get(f"/instances/{instance['id']}").json()
    assert final["state"] == ("healthy" if failure_phase is None else "failed")


def test_invalid_input_and_database_uniqueness(setup):
    client = setup[0]
    instance = create(setup)
    assert client.post("/deployments/999/instances").status_code == 404
    assert client.post(f"/deployments/{setup[2]['id']}/instances", json={"replicas": 2}).status_code == 422
    assert client.get("/instances/not-a-uuid").status_code == 422
    assert client.get(f"/instances/{uuid4()}").status_code == 404
    assert client.get("/instances?state=unknown").status_code == 422
    assert client.post("/workers/register", json={"identity": "bad", "capabilities": ["execute_code"]}).status_code == 422
    with Session(client.app.state.engine) as session:
        session.add(Instance(deployment_id=instance["deployment_id"], start_job_id=instance["start_job_id"]))
        with pytest.raises(IntegrityError, match="UNIQUE"):
            session.commit()


def test_kind_specific_reports_are_required(setup):
    client = setup[0]
    instance, _, body, prefix = started(setup)
    assert client.post(prefix + "/report", json={**body, "outcome": "completed"}).status_code == 422
    prep_worker = register(client, "prepare", ["prepare_deployment"])
    client.post("/scheduling/run")
    prep_job = client.get(f"/workers/{prep_worker}/jobs").json()[0]
    prep_prefix = f"/workers/{prep_worker}/jobs/{prep_job['id']}"
    prep_body = {"attempt_number": 1, "claim_id": str(uuid4())}
    assert client.post(prep_prefix + "/claim", json=prep_body).status_code == 200
    assert client.post(prep_prefix + "/report", json=report_body(instance, prep_body)).status_code == 422
    assert client.post(prep_prefix + "/report", json={**prep_body, "outcome": "completed"}).status_code == 200
    assert client.get(f"/instances/{instance['id']}").json()["state"] == "starting"


def test_unclaimed_assignment_exhaustion_fails_instance_without_observation(setup):
    client, now, _ = setup
    instance = create(setup)
    worker = register(client)
    for number in range(1, 4):
        client.post(f"/workers/{worker}/heartbeat")
        assert client.post("/scheduling/run").json()["assigned_count"] == 1
        now[0] += timedelta(seconds=15)
        assert client.post("/jobs/recover").json()["recovered_count"] == 1
        current = client.get(f"/instances/{instance['id']}").json()
        assert current["id"] == instance["id"]
        assert current["state"] == ("failed" if number == 3 else "pending")
        assert current["last_observed_at"] is None
        if number < 3:
            now[0] += timedelta(seconds=5 * 2 ** (number - 1))
    assert current["state_source"] == "control_plane"
    assert client.post(f"/instances/{instance['id']}/stop", json={"expected_version": current["version"]}).status_code == 200


@pytest.mark.parametrize("operation", ["report", "recover"])
def test_concurrent_stop_cannot_interrupt_live_startup(setup, operation):
    client, now, _ = setup
    instance, _, body, prefix = started(setup)
    if operation == "recover":
        now[0] += timedelta(seconds=30)
    with ThreadPoolExecutor(max_workers=2) as executor:
        operation_result = executor.submit(client.post, prefix + "/report", json=report_body(instance, body)) if operation == "report" else executor.submit(client.post, "/jobs/recover")
        stopping = executor.submit(client.post, f"/instances/{instance['id']}/stop", json={"expected_version": instance["version"]})
        result, stop = operation_result.result(), stopping.result()
    assert result.status_code == 200
    assert stop.status_code == 409
    assert client.get(f"/instances/{instance['id']}").json()["state"] == ("healthy" if operation == "report" else "failed")


def test_concurrent_recovery_and_report_at_deadline_cannot_mark_healthy(setup):
    client, now, _ = setup
    instance, _, body, prefix = started(setup)
    now[0] += timedelta(seconds=30)
    with ThreadPoolExecutor(max_workers=2) as executor:
        report = executor.submit(client.post, prefix + "/report", json=report_body(instance, body))
        recovery = executor.submit(client.post, "/jobs/recover")
        assert report.result().status_code == 409
        assert recovery.result().status_code == 200
    current = client.get(f"/instances/{instance['id']}").json()
    assert current["state"] == "failed" and current["last_observed_at"] is None
    assert current["state_reason"] == "lease_expired"
    version = current["version"]
    assert client.post("/jobs/recover").json()["recovered_count"] == 0
    assert client.get(f"/instances/{instance['id']}").json()["version"] == version


def test_scheduler_can_skip_more_than_one_page_of_incompatible_jobs(setup):
    client, _, dep = setup
    instance = create(setup)
    # Make a page of older startup jobs; a preparation-only worker must still find
    # preparation work behind the unsupported queue head.
    with Session(client.app.state.engine) as session:
        first_job = session.get(Job, instance["start_job_id"])
        for _ in range(101):
            # Separate deployments, each with one instance; no replicas or orphan jobs.
            older = first_job.created_at - timedelta(seconds=1)
            deployment = Deployment(application_id=dep["application_id"], specification={"simulated": True}, created_at=older)
            session.add(deployment)
            session.flush()
            startup = Job(deployment_id=deployment.id, kind="start_instance", created_at=older, next_run_at=older)
            session.add(startup)
            session.flush()
            session.add(Instance(deployment_id=deployment.id, start_job_id=startup.id, created_at=older, updated_at=older))
        session.commit()
    worker = register(client, "prepare-only", ["prepare_deployment"])
    response = client.post("/scheduling/run?limit=1")
    assert response.status_code == 200 and response.json()["assigned_count"] == 1
    assert client.get(f"/workers/{worker}/jobs").json()[0]["kind"] == "prepare_deployment"


def test_instance_transition_and_version_guards_exist_in_sqlite(setup):
    instance = create(setup)
    with setup[0].app.state.engine.begin() as connection:
        with pytest.raises(IntegrityError, match="invalid instance mutation"):
            connection.execute(text("UPDATE instances SET state='healthy', version=version+1 WHERE id=:id"), {"id": instance["id"]})
    with setup[0].app.state.engine.begin() as connection:
        with pytest.raises(IntegrityError, match="invalid instance mutation"):
            connection.execute(text("UPDATE instances SET state_reason='rewrite' WHERE id=:id"), {"id": instance["id"]})


@pytest.mark.parametrize("lost_response", ["claim", "report"])
def test_optional_agent_completes_startup_with_lost_response_retry(setup, lost_response):
    client, now, dep = setup
    instance = create(setup)
    worker = register(client)
    client.post("/scheduling/run")
    origin = now[0]

    class ControlledTime:
        elapsed = 0.0

        def is_set(self):
            return self.elapsed >= 20

        def wait(self, seconds):
            self.elapsed += max(seconds, 0.001)
            now[0] = origin + timedelta(seconds=self.elapsed)
            return self.is_set()

    time = ControlledTime()
    dropped = False
    reports = []

    def transport(request):
        nonlocal dropped
        body = json.loads(request.content) if request.content else None
        if request.url.path == "/workers/register":
            assert body["capabilities"] == ["prepare_deployment", "start_instance"]
        response = client.request(request.method, request.url.path, json=body)
        if request.url.path.endswith("/report"):
            reports.append(body)
        if request.url.path.endswith("/" + lost_response) and not dropped:
            assert response.status_code == 200
            dropped = True
            raise httpx.ReadTimeout("Committed response lost", request=request)
        return httpx.Response(response.status_code, json=response.json())

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(transport)) as http:
        run_job_agent(http, "startup", 5, time, clock=lambda: time.elapsed)
    current = client.get(f"/instances/{instance['id']}").json()
    assert current["state"] == "healthy" and current["worker_id"] == worker
    assert current["version"] == 4  # create, assign, claim, report; retries add none.
    assert all(body == reports[0] for body in reports)
    assert len(reports) == (2 if lost_response == "report" else 1)
    jobs = client.get("/jobs").json()
    assert {job["kind"]: job["state"] for job in jobs} == {"prepare_deployment": "queued", "start_instance": "completed"}
    assert client.get("/deployments").json() == [dep]
