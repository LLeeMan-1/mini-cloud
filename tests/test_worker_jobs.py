import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from backend.control_plane.api.jobs import get_clock
from backend.control_plane.persistence.models import utc_now
from backend.worker.agent import run_agent
from backend.worker.jobs import JobPoller, run_job_agent


class SimulatedTime:
    def __init__(self, duration=20):
        self.now = 0.0
        self.duration = duration
        self.waits = []

    def is_set(self):
        return self.now >= self.duration

    def wait(self, delay):
        self.waits.append(delay)
        self.now += max(delay, 0.001)
        assert len(self.waits) < 1000, "Agent must not busy-loop"
        return self.is_set()


@pytest.mark.parametrize("lost_response", ["claim", "report"])
def test_agent_end_to_end_simulation_recovers_from_lost_committed_response(client, lost_response):
    time = SimulatedTime()
    origin = datetime(2026, 10, 20, tzinfo=timezone.utc)
    server_time = lambda: origin + timedelta(seconds=time.now)
    client.app.dependency_overrides[utc_now] = server_time
    client.app.dependency_overrides[get_clock] = lambda: server_time
    app = client.post("/applications", json={"name": "simulation"}).json()
    dep = client.post(f"/applications/{app['id']}/deployments", json={
        "specification": {"command": "This is data and must never execute"}
    }).json()
    calls = []
    dropped = False

    def handler(request):
        nonlocal dropped
        path = request.url.path
        body = json.loads(request.content) if request.content else None
        calls.append((path, body))
        response = client.request(request.method, path, json=body)
        if path == "/workers/register":
            assert client.post("/scheduling/run").status_code == 200
        if path.endswith("/" + lost_response) and not dropped:
            assert response.status_code == 200
            dropped = True
            raise httpx.ReadTimeout("Response lost after commit", request=request)
        return httpx.Response(response.status_code, json=response.json())

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as http:
        run_job_agent(http, "simulation-worker", 5, time, clock=lambda: time.now)
    job = client.get("/jobs").json()[0]
    assert job["state"] == "completed"
    assert job["result"] == {"simulated": True}
    assert client.get("/deployments").json() == [dep]
    ownership_requests = [(path, body) for path, body in calls if path.endswith(("/claim", "/report"))]
    assert len({body["claim_id"] for _, body in ownership_requests}) == 1
    assert len([path for path, _ in ownership_requests if path.endswith("/" + lost_response)]) == 2
    assert len([path for path, _ in calls if path.endswith("/heartbeat")]) >= 2


def test_polling_backoff_does_not_suspend_heartbeats():
    time = SimulatedTime(21)
    heartbeat_times = []
    poll_times = []

    def handler(request):
        if request.url.path == "/workers/register":
            return httpx.Response(200, json={"id": 1})
        if request.url.path.endswith("/heartbeat"):
            heartbeat_times.append(time.now)
            return httpx.Response(200, json={"id": 1})
        poll_times.append(time.now)
        return httpx.Response(503)

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
        run_job_agent(client, "local", 5, time, clock=lambda: time.now)
    assert heartbeat_times == [5, 10, 15, 20]
    assert poll_times == [0, 1, 3, 7, 15]


def test_polling_agent_recovers_from_full_api_outage():
    time = SimulatedTime(40)
    successes = []

    def handler(request):
        if 4 <= time.now < 17:
            raise httpx.ConnectError("API down", request=request)
        successes.append((request.url.path, time.now))
        if request.url.path.endswith("/jobs"):
            return httpx.Response(200, json=[])
        return httpx.Response(200, json={"id": 1})

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
        run_job_agent(client, "local", 5, time, clock=lambda: time.now)
    assert any(path.endswith("/heartbeat") and time >= 17 for path, time in successes)
    assert any(path.endswith("/jobs") and time >= 17 for path, time in successes)


def test_polling_agent_reregisters_unknown_worker():
    time = SimulatedTime(12)
    calls = []
    registrations = 0

    def handler(request):
        nonlocal registrations
        path = request.url.path
        calls.append(path)
        if path == "/workers/register":
            registrations += 1
            return httpx.Response(200, json={"id": registrations})
        if path == "/workers/1/heartbeat":
            return httpx.Response(404)
        return httpx.Response(200, json=[] if path.endswith("/jobs") else {"id": 2})

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
        run_job_agent(client, "local", 5, time, clock=lambda: time.now)
    assert registrations == 2
    assert "/workers/2/jobs" in calls
    assert "/workers/2/heartbeat" in calls


def test_claim_conflict_does_not_crash_polling_agent():
    time = SimulatedTime(12)
    heartbeats = []

    def handler(request):
        path = request.url.path
        if path.endswith("/claim"):
            return httpx.Response(409)
        if path.endswith("/jobs"):
            return httpx.Response(200, json=[{"id": 1, "kind": "prepare_deployment", "attempt_number": 1}])
        if path.endswith("/heartbeat"):
            heartbeats.append(path)
        return httpx.Response(200, json={"id": 1})

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
        run_job_agent(client, "local", 5, time, clock=lambda: time.now)
    assert len(heartbeats) == 2


def test_opt_in_dispatch(monkeypatch):
    called = []
    monkeypatch.setattr("backend.worker.jobs.run_job_agent", lambda *args: called.append(args))
    with httpx.Client(base_url="http://test") as client:
        run_agent(client, "local", poll_jobs=True)
    assert len(called) == 1


def test_replayed_claim_explicitly_rejected_is_not_reported_again():
    paths = []

    def handler(request):
        paths.append(request.url.path)
        if request.method == "GET":
            return httpx.Response(200, json=[{"id": 1, "kind": "prepare_deployment", "attempt_number": 1}])
        return httpx.Response(409)

    poller = JobPoller()
    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
        assert poller.step(client, 1) is True
        with pytest.raises(httpx.HTTPStatusError):
            poller.step(client, 1)
    assert paths == ["/workers/1/jobs", "/workers/1/jobs/1/claim"]
