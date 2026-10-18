from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from backend.control_plane.main import create_app
from backend.control_plane.persistence.models import Worker, utc_now


@pytest.fixture
def clock(client):
    time = [datetime(2026, 10, 14, tzinfo=timezone.utc)]
    client.app.dependency_overrides[utc_now] = lambda: time[0]
    return time


def register(client, identity="laptop-1"):
    response = client.post("/workers/register", json={"identity": identity})
    assert response.status_code == 200
    return response.json()


def test_registration_and_aware_storage(client, clock):
    assert client.get("/workers").json() == []
    worker = register(client, " laptop-1 ")
    assert worker["identity"] == "laptop-1"
    assert worker["status"] == "available"
    for field in ("registered_at", "last_heartbeat_at"):
        assert datetime.fromisoformat(worker[field]) == clock[0]
    assert client.get("/workers").json() == [worker]
    with Session(client.app.state.engine) as session:
        record = session.get(Worker, worker["id"])
        assert record.registered_at.tzinfo == timezone.utc
        assert record.last_heartbeat_at == clock[0]
        assert not hasattr(record, "status")


def test_duplicate_registration_preserves_identity_and_registration_time(client, clock):
    first = register(client)
    clock[0] += timedelta(seconds=20)
    second = register(client)
    assert second["id"] == first["id"]
    assert second["registered_at"] == first["registered_at"]
    assert datetime.fromisoformat(second["last_heartbeat_at"]) == clock[0]
    assert second["status"] == "available"
    assert len(client.get("/workers").json()) == 1


def test_timeout_boundary_and_heartbeat_recovery(client, clock):
    worker = register(client)
    clock[0] += timedelta(seconds=15)
    assert client.get("/workers").json()[0]["status"] == "available"
    clock[0] += timedelta(microseconds=1)
    assert client.get("/workers").json()[0]["status"] == "unavailable"
    with Session(client.app.state.engine) as session:
        # Listing derives availability without changing the persisted timestamp.
        assert session.get(Worker, worker["id"]).last_heartbeat_at == datetime.fromisoformat(
            worker["last_heartbeat_at"]
        )
    response = client.post(f"/workers/{worker['id']}/heartbeat")
    assert response.status_code == 200
    assert response.json()["status"] == "available"
    assert response.json()["registered_at"] == worker["registered_at"]
    assert datetime.fromisoformat(response.json()["last_heartbeat_at"]) == clock[0]
    assert client.get("/workers").json()[0]["status"] == "available"


@pytest.mark.parametrize("body", [
    {}, {"identity": ""}, {"identity": "   "}, {"identity": 123},
    {"identity": "x" * 101}, {"identity": "demo", "status": "available"},
])
def test_invalid_registration(client, body):
    assert client.post("/workers/register", json=body).status_code == 422
    assert client.get("/workers").json() == []


@pytest.mark.parametrize("worker_id,status", [(999, 404), (0, 422), (-1, 422), ("bad", 422)])
def test_invalid_heartbeat(client, worker_id, status):
    assert client.post(f"/workers/{worker_id}/heartbeat").status_code == status
    assert client.get("/workers").json() == []


def test_persistence_restart_and_custom_timeout(tmp_path):
    path = tmp_path / "workers.db"
    time = datetime(2026, 10, 14, tzinfo=timezone.utc)
    with TestClient(create_app(path)) as first:
        first.app.dependency_overrides[utc_now] = lambda: time
        original = register(first)
    with TestClient(create_app(path, worker_timeout=3)) as second:
        second.app.dependency_overrides[utc_now] = lambda: time + timedelta(seconds=4)
        stored = second.get("/workers").json()[0]
        assert stored == {**original, "status": "unavailable"}
        renewed = register(second)
        assert renewed["id"] == original["id"]
        assert renewed["registered_at"] == original["registered_at"]


def test_concurrent_registration_is_idempotent(client):
    with ThreadPoolExecutor(max_workers=4) as executor:
        records = list(executor.map(lambda _: register(client), range(8)))
    assert len({record["id"] for record in records}) == 1
    assert len(client.get("/workers").json()) == 1


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_invalid_timeout_configuration(timeout):
    with pytest.raises(ValueError, match="finite positive"):
        create_app(worker_timeout=timeout)


def test_timeout_environment(monkeypatch):
    monkeypatch.setenv("MINI_CLOUD_WORKER_TIMEOUT_SECONDS", "8")
    assert create_app().state.worker_timeout == 8


def test_existing_milestone_1a_database_gains_workers_table(tmp_path):
    from backend.control_plane.persistence.database import create_database_engine
    from backend.control_plane.persistence.models import Application, Deployment

    path = tmp_path / "existing.db"
    engine = create_database_engine(path)
    Application.__table__.create(engine)
    Deployment.__table__.create(engine)
    with Session(engine) as session:
        session.add(Application(name="existing", configuration={}))
        session.commit()
    engine.dispose()
    with TestClient(create_app(path)) as client:
        assert client.get("/applications").json()[0]["name"] == "existing"
        assert register(client)["status"] == "available"


@pytest.mark.parametrize("operation", ["register", "heartbeat"])
def test_out_of_order_timestamps_never_decrease(client, clock, operation):
    worker = register(client)
    origin = clock[0]
    latest = origin
    for seconds in (12, 2, 20, 7, 20, 0):
        clock[0] = origin + timedelta(seconds=seconds)
        if operation == "register":
            response = register(client)
        else:
            result = client.post(f"/workers/{worker['id']}/heartbeat")
            assert result.status_code == 200
            response = result.json()
        latest = max(latest, clock[0])
        assert datetime.fromisoformat(response["last_heartbeat_at"]) == latest
        assert response["registered_at"] == worker["registered_at"]
        with Session(client.app.state.engine) as session:
            assert session.get(Worker, worker["id"]).last_heartbeat_at == latest


def test_concurrent_registration_and_heartbeats_preserve_latest_time(client, clock):
    from backend.control_plane.services.workers import heartbeat, register_worker

    worker = register(client)
    origin = clock[0]

    def report(item):
        operation, seconds = item
        with Session(client.app.state.engine) as session:
            now = origin + timedelta(seconds=seconds)
            if operation == "register":
                register_worker(session, "laptop-1", now)
            else:
                heartbeat(session, worker["id"], now)

    # Write the newest timestamp first, then overlap older registrations/heartbeats.
    report(("heartbeat", 60))
    reports = [(operation, seconds) for seconds in (20, 5, 40, 0)
               for operation in ("register", "heartbeat")]
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(report, reports))
    with Session(client.app.state.engine) as session:
        assert session.get(Worker, worker["id"]).last_heartbeat_at == origin + timedelta(seconds=60)
    assert len(client.get("/workers").json()) == 1


def test_timestamp_comparison_normalizes_non_utc_offsets(client, clock):
    from backend.control_plane.services.workers import heartbeat, register_worker

    worker = register(client)
    now = clock[0]
    offset = timezone(timedelta(hours=5))
    with Session(client.app.state.engine) as session:
        heartbeat(session, worker["id"], (now + timedelta(seconds=10)).astimezone(offset))
        register_worker(session, "laptop-1", (now + timedelta(seconds=5)).astimezone(offset))
        session.expire_all()
        assert session.get(Worker, worker["id"]).last_heartbeat_at == now + timedelta(seconds=10)
