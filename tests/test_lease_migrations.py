from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, select

from backend.control_plane.main import create_app
from backend.control_plane.persistence.database import create_database_engine
from backend.control_plane.persistence.migrations import create_version_two_jobs, initialize_database
from backend.control_plane.persistence.models import Application, Deployment, Worker, Job


@pytest.fixture
def v2(tmp_path):
    path = tmp_path / "actual-v2.db"
    engine = create_database_engine(path)
    claims = {state: str(uuid4()) for state in ("claimed", "completed", "failed")}
    with engine.begin() as connection:
        for table in (Application.__table__, Deployment.__table__, Worker.__table__):
            table.create(connection)
        create_version_two_jobs(connection)
        connection.exec_driver_sql("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)")
        connection.exec_driver_sql("INSERT INTO schema_migrations VALUES (1), (2)")
        connection.exec_driver_sql("INSERT INTO applications VALUES (1, 'original', '2026-10-01 00:00:00.000000', '{}')")
        for index, state in enumerate(("queued", "assigned", "claimed", "completed", "failed"), start=1):
            connection.exec_driver_sql("INSERT INTO deployments VALUES (?, 1, '2026-10-01 00:00:00.000000', '{\"test\":true}', 'pending')", (index,))
            connection.exec_driver_sql("INSERT INTO workers VALUES (?, ?, '2026-10-01 00:00:00.000000', '2026-10-01 00:00:00.000000')", (index, "node-" + str(index)))
            stamp = "2026-10-01 00:00:00.000000"
            connection.exec_driver_sql("""
                INSERT INTO jobs (id, deployment_id, worker_id, kind, state, created_at,
                    assigned_at, claimed_at, finished_at, claim_id, result, error)
                VALUES (?, ?, ?, 'prepare_deployment', ?, ?, ?, ?, ?, ?, ?, ?)
            """, (index, index, None if state == "queued" else index, state, stamp,
                  stamp if state != "queued" else None,
                  stamp if state in claims else None,
                  stamp if state in ("completed", "failed") else None, claims.get(state),
                  '{"simulated":true}' if state == "completed" else None,
                  "old failure" if state == "failed" else None))
    yield path, engine, claims
    engine.dispose()


def old_snapshot(connection):
    return {table: connection.exec_driver_sql(f"SELECT * FROM {table} ORDER BY id").all()
            for table in ("applications", "deployments", "workers", "jobs")}


def test_v3_preserves_all_job_states_and_terminal_report_acknowledgments(v2):
    path, engine, claims = v2
    now = datetime(2026, 10, 22, tzinfo=timezone.utc)
    with engine.connect() as connection:
        before = old_snapshot(connection)
    initialize_database(engine, clock=lambda: now)
    with engine.connect() as connection:
        after = old_snapshot(connection)
        for table in ("applications", "deployments", "workers"):
            assert after[table] == before[table]
        assert [row[:12] for row in after["jobs"]] == before["jobs"]
        rows = connection.execute(select(Job)).mappings().all()
        assert rows[0]["attempt_count"] == 0
        assert rows[1]["assignment_expires_at"] == now + timedelta(seconds=15)
        assert rows[2]["lease_expires_at"] == now + timedelta(seconds=30)
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM job_attempts").scalar_one() == 4
    initialize_database(engine, clock=lambda: now + timedelta(days=1))
    with engine.connect() as connection:
        assert connection.execute(select(Job.lease_expires_at).where(Job.id == 3)).scalar_one() == now + timedelta(seconds=30)
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM job_attempts").scalar_one() == 4
    with TestClient(create_app(path)) as client:
        for job, state in ((4, "completed"), (5, "failed")):
            body = {"attempt_number": 1, "claim_id": claims[state], "outcome": state}
            body.update({"result": {"simulated": True}} if state == "completed" else {"error": "old failure"})
            ack = client.post(f"/workers/{job}/jobs/{job}/report", json=body)
            assert ack.status_code == 200, ack.text
            assert ack.json()["attempt_outcome"] == state
            assert client.get(f"/jobs/{job}").json()["state"] == state
    # Exercise the actual API lifespan repeatedly, not just the migration function.
    for _ in range(2):
        with TestClient(create_app(path)) as restarted:
            jobs = restarted.get("/jobs").json()
            assert len(jobs) == 5
            assert sum(len(restarted.get(f"/jobs/{job['id']}/attempts").json())
                       for job in jobs) == 4
            assert datetime.fromisoformat(jobs[1]["assignment_expires_at"]) == now + timedelta(seconds=15)
            assert datetime.fromisoformat(jobs[2]["lease_expires_at"]) == now + timedelta(seconds=30)


def test_v3_failure_rolls_back_rebuild_and_version(v2):
    _, engine, _ = v2
    with engine.connect() as connection:
        before = old_snapshot(connection)

    def interrupt(connection, cursor, statement, parameters, context, executemany):
        if statement == "INSERT INTO schema_migrations (version) VALUES (3)":
            raise RuntimeError("Interrupted v3")

    event.listen(engine, "before_cursor_execute", interrupt)
    try:
        with pytest.raises(RuntimeError, match="Interrupted v3"):
            initialize_database(engine)
    finally:
        event.remove(engine, "before_cursor_execute", interrupt)
    with engine.connect() as connection:
        assert old_snapshot(connection) == before
        assert connection.exec_driver_sql("PRAGMA table_info(job_attempts)").all() == []
        assert connection.exec_driver_sql("SELECT MAX(version) FROM schema_migrations").scalar_one() == 2
    initialize_database(engine)


def test_simultaneous_v3_startups_preserve_one_history(v2):
    from concurrent.futures import ThreadPoolExecutor
    path, engine, _ = v2

    def upgrade(_):
        separate = create_database_engine(path)
        try:
            initialize_database(separate)
        finally:
            separate.dispose()

    with ThreadPoolExecutor(max_workers=3) as executor:
        list(executor.map(upgrade, range(3)))
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM job_attempts").scalar_one() == 4
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
