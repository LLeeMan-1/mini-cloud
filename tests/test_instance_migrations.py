from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, select

from backend.control_plane.api.jobs import get_clock
from backend.control_plane.main import create_app
from backend.control_plane.persistence import schema_v3
from backend.control_plane.persistence.database import create_database_engine
from backend.control_plane.persistence.migrations import initialize_database
from backend.control_plane.persistence.models import Application, Deployment, Job, JobAttempt, JobRenewal


TABLES = ("applications", "deployments", "workers", "jobs", "job_attempts", "job_renewals")


def snapshot(connection):
    return {table: connection.exec_driver_sql(f"SELECT * FROM {table} ORDER BY id").all() for table in TABLES}


@pytest.fixture
def version_three(tmp_path):
    path = tmp_path / "v3-with-history.db"
    engine = create_database_engine(path)
    now = datetime(2026, 10, 23, tzinfo=timezone.utc)
    claims = {state: str(uuid4()) for state in ("claimed", "completed", "failed")}
    renewal_id = str(uuid4())
    with engine.begin() as connection:
        Application.__table__.create(connection)
        Deployment.__table__.create(connection)
        for name in ("workers", "jobs", "job_attempts", "job_renewals"):
            schema_v3.create_table(connection, name)
        connection.exec_driver_sql("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)")
        connection.exec_driver_sql("INSERT INTO schema_migrations VALUES (1), (2), (3)")
        connection.execute(Application.__table__.insert().values(id=1, name="existing", configuration={"owner": "local"}, created_at=now))
        for index, state in enumerate(("queued", "assigned", "claimed", "completed", "failed"), 1):
            connection.execute(Deployment.__table__.insert().values(
                id=index, application_id=1, created_at=now, specification={"image": "existing"}, runtime_status="pending"))
            stamp = now.replace(tzinfo=None).isoformat(" ", timespec="microseconds")
            connection.exec_driver_sql("INSERT INTO workers VALUES (?, ?, ?, ?)", (index, "worker-" + str(index), stamp, stamp))
            terminal = state in ("completed", "failed")
            connection.execute(Job.__table__.insert().values(
                id=index, deployment_id=index, kind="prepare_deployment", state=state,
                created_at=now, next_run_at=now, max_attempts=3, attempt_count=0 if state == "queued" else 1,
                worker_id=None if state == "queued" else index, assigned_at=None if state == "queued" else now,
                claim_id=claims.get(state), claimed_at=now if state in claims else None,
                finished_at=now if terminal else None, result={"simulated": True} if state == "completed" else None,
                error="original failure" if state == "failed" else None,
                assignment_expires_at=now + timedelta(seconds=15) if state == "assigned" else None,
                lease_expires_at=now + timedelta(seconds=60) if state == "claimed" else None,
                last_failure_code="permanent_failure" if state == "failed" else None,
            ))
            if state != "queued":
                row = connection.execute(select(Job).where(Job.id == index)).mappings().one()
                ack = {key: value.isoformat() if isinstance(value, datetime) else value for key, value in row.items()} if terminal else None
                if ack is not None:
                    ack.update(acknowledged_attempt_number=1, attempt_outcome=state)
                payload = {"outcome": state, "result": row["result"], "error": row["error"], "retryable": False} if terminal else None
                connection.execute(JobAttempt.__table__.insert().values(
                    id=100 + index, job_id=index, number=1, worker_id=index, state=state,
                    assigned_at=now, claimed_at=row["claimed_at"], finished_at=row["finished_at"],
                    claim_id=claims.get(state), result=row["result"], error=row["error"],
                    retryable=False, failure_code=row["last_failure_code"], report_payload=payload, report_ack=ack,
                ))
        connection.execute(JobRenewal.__table__.insert().values(
            id=201, attempt_id=103, renewal_id=renewal_id, granted_at=now + timedelta(seconds=10),
            lease_expires_at=now + timedelta(seconds=60)))
    yield path, engine, now, claims, renewal_id
    engine.dispose()


def test_v4_preserves_every_record_receipt_and_live_claim(version_three):
    path, engine, now, claims, renewal_id = version_three
    with engine.connect() as connection:
        before = snapshot(connection)
    initialize_database(engine)
    initialize_database(engine)
    with engine.connect() as connection:
        after = snapshot(connection)
        for table in TABLES:
            if table == "workers":
                assert [row[:4] for row in after[table]] == before[table]
                assert all(row[4] == '["prepare_deployment"]' for row in after[table])
            else:
                assert after[table] == before[table]
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM instances").scalar_one() == 0
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
    for _ in range(2):
        with TestClient(create_app(path)) as client:
            client.app.dependency_overrides[get_clock] = lambda: lambda: now + timedelta(seconds=20)
            assert client.get("/instances").json() == []
            for index, state in ((4, "completed"), (5, "failed")):
                body = {"attempt_number": 1, "claim_id": claims[state], "outcome": state}
                body.update({"result": {"simulated": True}} if state == "completed" else {"error": "original failure"})
                response = client.post(f"/workers/{index}/jobs/{index}/report", json=body)
                assert response.status_code == 200
                assert "instance" not in response.json()
            replay = client.post("/workers/3/jobs/3/renew", json={
                "attempt_number": 1, "claim_id": claims["claimed"], "renewal_id": renewal_id})
            assert replay.status_code == 200
            assert datetime.fromisoformat(replay.json()["lease_expires_at"]) == now + timedelta(seconds=60)
    with engine.connect() as connection:
        after = snapshot(connection)
        assert after["job_attempts"] == before["job_attempts"]
        assert after["job_renewals"] == before["job_renewals"]


@pytest.mark.parametrize("failure_point", ["drop_children", "restore_parent", "version_record"])
def test_v4_rollback_restores_complete_original_schema_and_data(version_three, failure_point):
    _, engine, *_ = version_three
    with engine.connect() as connection:
        before = snapshot(connection)
        schema_before = connection.exec_driver_sql("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name").all()

    def interrupt(connection, cursor, statement, parameters, context, executemany):
        if ((failure_point == "drop_children" and statement == "DROP TABLE job_attempts")
                or (failure_point == "restore_parent" and statement.startswith("INSERT INTO jobs ("))
                or (failure_point == "version_record" and statement == "INSERT INTO schema_migrations (version) VALUES (4)")):
            raise RuntimeError("interrupted version 4")

    event.listen(engine, "before_cursor_execute", interrupt)
    try:
        with pytest.raises(RuntimeError, match="interrupted version 4"):
            initialize_database(engine)
    finally:
        event.remove(engine, "before_cursor_execute", interrupt)
    with engine.connect() as connection:
        assert snapshot(connection) == before
        assert connection.exec_driver_sql("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name").all() == schema_before
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
    initialize_database(engine)


def test_concurrent_v4_startups_preserve_history(version_three):
    path, engine, *_ = version_three

    def migrate(_):
        separate_engine = create_database_engine(path)
        try:
            initialize_database(separate_engine)
        finally:
            separate_engine.dispose()

    with ThreadPoolExecutor(max_workers=3) as executor:
        list(executor.map(migrate, range(3)))
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM job_attempts").scalar_one() == 4
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM job_renewals").scalar_one() == 1
        assert connection.exec_driver_sql("SELECT MAX(version) FROM schema_migrations").scalar_one() == 5
