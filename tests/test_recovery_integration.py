"""Complete HTTP recovery lifecycle, with controlled time and no real sleeps."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from backend.control_plane.api.jobs import get_clock
from backend.control_plane.persistence.models import utc_now


def test_expired_owner_is_fenced_and_replacement_completes_with_history(client):
    now = [datetime(2026, 10, 22, tzinfo=timezone.utc)]
    client.app.dependency_overrides[utc_now] = lambda: now[0]
    client.app.dependency_overrides[get_clock] = lambda: lambda: now[0]

    app_response = client.post("/applications", json={"name": "recovery-integration"})
    assert app_response.status_code == 201
    application_id = app_response.json()["id"]
    deployment_response = client.post(
        f"/applications/{application_id}/deployments",
        json={"specification": {"simulated": True}},
    )
    assert deployment_response.status_code == 201
    deployment = deployment_response.json()
    job = client.get("/jobs", params={"deployment_id": deployment["id"]}).json()[0]
    job_id = job["id"]

    first_registration = client.post("/workers/register", json={"identity": "worker-1"})
    assert first_registration.status_code == 200
    first_id = first_registration.json()["id"]
    scheduled = client.post("/scheduling/run")
    assert scheduled.status_code == 200
    assert scheduled.json()["assignments"] == [
        {"job_id": job_id, "worker_id": first_id, "attempt_number": 1}
    ]
    first_poll = client.get(f"/workers/{first_id}/jobs")
    assert first_poll.status_code == 200
    assert [(item["id"], item["attempt_number"]) for item in first_poll.json()] == [
        (job_id, 1)
    ]
    first_claim = {"attempt_number": 1, "claim_id": str(uuid4())}
    first_prefix = f"/workers/{first_id}/jobs/{job_id}"
    claimed = client.post(first_prefix + "/claim", json=first_claim)
    assert claimed.status_code == 200
    assert claimed.json()["state"] == "claimed"

    # Worker 1 sends no renewal. Ownership expires exactly at the server deadline.
    now[0] = datetime.fromisoformat(claimed.json()["lease_expires_at"])
    recovered = client.post("/jobs/recover")
    assert recovered.status_code == 200
    assert recovered.json()["recovered"] == [
        {"job_id": job_id, "attempt_number": 1, "state": "queued",
         "failure_code": "lease_expired"}
    ]
    queued = client.get(f"/jobs/{job_id}").json()
    assert queued["worker_id"] is None
    assert queued["attempt_count"] == 1
    due = datetime.fromisoformat(queued["next_run_at"])
    assert due - now[0] == timedelta(seconds=5)
    first_history = client.get(f"/jobs/{job_id}/attempts").json()[0]
    assert first_history["state"] == "expired"
    assert first_history["claim_id"] == first_claim["claim_id"]

    now[0] = due
    second_registration = client.post("/workers/register", json={"identity": "worker-2"})
    assert second_registration.status_code == 200
    second_id = second_registration.json()["id"]
    scheduled_again = client.post("/scheduling/run")
    assert scheduled_again.status_code == 200
    assert scheduled_again.json()["assignments"] == [
        {"job_id": job_id, "worker_id": second_id, "attempt_number": 2}
    ]
    second_poll = client.get(f"/workers/{second_id}/jobs")
    assert second_poll.status_code == 200
    assert [(item["id"], item["attempt_number"]) for item in second_poll.json()] == [
        (job_id, 2)
    ]
    second_claim = {"attempt_number": 2, "claim_id": str(uuid4())}
    second_prefix = f"/workers/{second_id}/jobs/{job_id}"
    replacement_claimed = client.post(second_prefix + "/claim", json=second_claim)
    assert replacement_claimed.status_code == 200
    before_stale_report = client.get(f"/jobs/{job_id}").json()

    stale_report = client.post(
        first_prefix + "/report",
        json={**first_claim, "outcome": "completed", "result": {"simulated": True}},
    )
    assert stale_report.status_code == 409
    assert stale_report.json()["detail"]["code"] == "stale_attempt"
    assert client.get(f"/jobs/{job_id}").json() == before_stale_report

    final_report = client.post(
        second_prefix + "/report",
        json={**second_claim, "outcome": "completed", "result": {"simulated": True}},
    )
    assert final_report.status_code == 200
    assert final_report.json()["acknowledged_attempt_number"] == 2
    completed = client.get(f"/jobs/{job_id}").json()
    assert completed["state"] == "completed"
    assert completed["attempt_count"] == 2
    assert completed["worker_id"] == second_id
    assert completed["claim_id"] == second_claim["claim_id"]

    history = client.get(f"/jobs/{job_id}/attempts").json()
    assert len(history) == 2
    assert history[0] == first_history  # Recovery history survives later operations intact.
    assert history[1]["number"] == 2
    assert history[1]["state"] == "completed"
    assert history[1]["worker_id"] == second_id
    assert history[1]["claim_id"] == second_claim["claim_id"]
    assert history[1]["result"] == {"simulated": True}
    assert client.get("/deployments").json() == [deployment]
    assert deployment["runtime_status"] == "pending"
