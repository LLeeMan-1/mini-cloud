import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from backend.control_plane.api.jobs import get_clock
from backend.control_plane.persistence.models import utc_now
from backend.worker.jobs import JobPoller, OwnershipLost, run_job_agent


class Time:
    def __init__(self, duration=70):
        self.now = 0.0
        self.duration = duration
        self.waits = 0

    def is_set(self):
        return self.now >= self.duration

    def wait(self, delay):
        self.now += max(delay, 0.001)
        self.waits += 1
        assert self.waits < 2000, "Agent must not busy loop"
        return self.is_set()


def prepare(client, time):
    origin = datetime(2026, 10, 22, tzinfo=timezone.utc)
    server = lambda: origin + timedelta(seconds=time.now)
    client.app.dependency_overrides[utc_now] = server
    client.app.dependency_overrides[get_clock] = lambda: server
    app = client.post("/applications", json={"name": "renewal"}).json()
    dep = client.post(f"/applications/{app['id']}/deployments", json={"specification": {"test": True}}).json()
    return dep


def forwarded(client, request):
    body = json.loads(request.content) if request.content else None
    response = client.request(request.method, request.url.path, json=body)
    return httpx.Response(response.status_code, json=response.json())


def test_lost_renewal_response_reuses_id_and_keeps_heartbeats_independent(client):
    time = Time(70)
    dep = prepare(client, time)
    renewal_requests = []
    heartbeats = []
    dropped = False

    def handler(request):
        nonlocal dropped
        path = request.url.path
        if path.endswith("/report") and time.now < 35:
            return httpx.Response(503)
        response = forwarded(client, request)
        if path == "/workers/register":
            client.post("/scheduling/run")
        if path.endswith("/heartbeat"):
            heartbeats.append(time.now)
        if path.endswith("/renew"):
            renewal_requests.append(json.loads(request.content))
            assert response.status_code == 200
            if not dropped:
                dropped = True
                raise httpx.ReadTimeout("Renewal accepted, response lost", request=request)
        return response

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as http:
        run_job_agent(http, "renewing", 5, time, clock=lambda: time.now)
    assert len(renewal_requests) >= 2
    assert renewal_requests[0] == renewal_requests[1]
    assert len(heartbeats) >= 5
    job = client.get("/jobs").json()[0]
    assert job["state"] == "completed"
    assert job["attempt_count"] == 1
    assert client.get("/deployments").json() == [dep]


def test_outage_beyond_deadline_discards_work_and_never_sends_late_report(client):
    time = Time(65)
    prepare(client, time)
    reports = []
    claims = []

    def handler(request):
        path = request.url.path
        if path.endswith("/report"):
            reports.append(time.now)
        if path.endswith("/claim"):
            claims.append(time.now)
        # After claim, simulate a partition until after server lease expiry.
        if time.now >= 0.002 and time.now < 40:
            raise httpx.ConnectError("Partition", request=request)
        response = forwarded(client, request)
        if path == "/workers/register":
            client.post("/scheduling/run")
        return response

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as http:
        run_job_agent(http, "partitioned", 5, time, clock=lambda: time.now)
    assert claims
    assert reports and max(reports) < 29.1
    job = client.get("/jobs").json()[0]
    assert job["state"] == "claimed"  # No hidden recovery loop.
    assert client.post("/jobs/recover").json()["recovered_count"] == 1


def test_explicit_renewal_rejection_stops_reporting_but_not_heartbeats(client):
    time = Time(30)
    prepare(client, time)
    reports = []
    rejected_at = []
    heartbeat_times = []

    def handler(request):
        path = request.url.path
        if path.endswith("/report"):
            reports.append(time.now)
            return httpx.Response(503)
        if path.endswith("/renew"):
            rejected_at.append(time.now)
            return httpx.Response(409, json={"detail": {"code": "stale_attempt"}})
        response = forwarded(client, request)
        if path == "/workers/register":
            client.post("/scheduling/run")
        if path.endswith("/heartbeat"):
            heartbeat_times.append(time.now)
        return response

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as http:
        run_job_agent(http, "rejected", 5, time, clock=lambda: time.now)
    assert rejected_at and reports
    assert max(reports) < rejected_at[0]
    assert max(heartbeat_times) > rejected_at[0]


def test_worker_restart_does_not_adopt_old_process_claim(client):
    first = Time(0.002)
    prepare(client, first)

    def handler(request):
        response = forwarded(client, request)
        if request.url.path == "/workers/register":
            client.post("/scheduling/run")
        return response

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as http:
        run_job_agent(http, "restarted", 5, first, clock=lambda: first.now)
    assert client.get("/jobs").json()[0]["state"] == "claimed"
    second = Time(20)
    second.now = first.now
    origin = datetime(2026, 10, 22, tzinfo=timezone.utc)
    server = lambda: origin + timedelta(seconds=second.now)
    client.app.dependency_overrides[utc_now] = server
    client.app.dependency_overrides[get_clock] = lambda: server
    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as http:
        run_job_agent(http, "restarted", 5, second, clock=lambda: second.now)
    assert client.get("/jobs").json()[0]["state"] == "claimed"
    second.now = 31
    assert client.post("/jobs/recover").json()["recovered_count"] == 1


def test_grant_accounts_for_slow_response_without_trusting_worker_wall_clock():
    time = Time()
    poller = JobPoller(clock=lambda: time.now)
    poller.pending = {"id": 1}
    # Response spends 29.5 seconds in transit; there is no conservative lease left.
    time.now = 29.5
    with pytest.raises(OwnershipLost):
        poller.grant({"server_time": "2026-10-22T00:00:00Z",
                      "lease_expires_at": "2026-10-22T00:00:30Z", "renewal_interval": 10}, 0)
    assert poller.pending is None


def test_new_claim_resets_previous_attempt_renewal_backoff():
    time = Time(60)
    origin = datetime(2026, 10, 22, tzinfo=timezone.utc)
    polls = 0
    second_renewals = []

    def handler(request):
        nonlocal polls
        path = request.url.path
        if path == "/workers/register" or path.endswith("/heartbeat"):
            return httpx.Response(200, json={"id": 1})
        if path.endswith("/jobs"):
            polls += 1
            return httpx.Response(200, json=[{
                "id": polls, "kind": "prepare_deployment", "attempt_number": 1,
            }])
        if path.endswith("/report"):
            return httpx.Response(503)
        if path.endswith("/renew"):
            if "/jobs/1/" in path:
                return httpx.Response(503)
            second_renewals.append(time.now)
            if len(second_renewals) == 1:
                return httpx.Response(503)
        return httpx.Response(200, json={
            "state": "claimed",
            "server_time": (origin + timedelta(seconds=time.now)).isoformat(),
            "lease_expires_at": (origin + timedelta(seconds=time.now + 30)).isoformat(),
            "renewal_interval": 10,
        })

    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as http:
        run_job_agent(http, "reset-backoff", 5, time, clock=lambda: time.now)
    assert len(second_renewals) >= 2
    assert second_renewals[1] - second_renewals[0] == pytest.approx(1)
