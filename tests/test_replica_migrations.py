"""True v4 fixtures, raw receipt preservation, and transactional v5 rollback."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, select, text
from sqlalchemy.exc import IntegrityError

from backend.control_plane.api.jobs import get_clock
from backend.control_plane.api.schemas import ReportRead
from backend.control_plane.main import create_app
from backend.control_plane.persistence import schema_v4
from backend.control_plane.persistence.database import create_database_engine
from backend.control_plane.persistence.migrations import initialize_database
from backend.control_plane.persistence.models import Application, Deployment, Job, JobAttempt, JobRenewal, Worker


TABLES = ("applications", "deployments", "workers", "jobs", "job_attempts", "job_renewals", "instances", "schema_migrations")


def snapshot(connection):
    return {name: connection.exec_driver_sql(f"SELECT * FROM {name} ORDER BY 1").all() for name in TABLES}


@pytest.fixture
def version_four(tmp_path):
    path = tmp_path / "true-v4.db"
    engine = create_database_engine(path)
    now = datetime(2026, 10, 25, tzinfo=timezone.utc)
    stamp = now.replace(tzinfo=None).isoformat(" ", timespec="microseconds")
    receipts = []
    claims = {}
    with engine.begin() as connection:
        for table in (Application.__table__, Deployment.__table__, Worker.__table__):
            table.create(connection)
        for name in ("jobs", "job_attempts", "job_renewals", "instances"):
            schema_v4.create_table(connection, name)
        connection.exec_driver_sql("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY)")
        connection.exec_driver_sql("INSERT INTO schema_migrations VALUES (1),(2),(3),(4)")
        connection.execute(Application.__table__.insert().values(id=1, name="historical", configuration={"preserved": True}, created_at=now))
        states = ("pending", "starting", "healthy", "failed", "stopped", "assigned", "no_instance")
        for number, state in enumerate(states, 1):
            connection.execute(Deployment.__table__.insert().values(
                id=number, application_id=1, created_at=now, specification={"replicas": 42}, runtime_status="pending"))
            connection.execute(Job.__table__.insert().values(
                id=number * 10, deployment_id=number, kind="prepare_deployment", state="queued",
                created_at=now, next_run_at=now, attempt_count=0, max_attempts=3))
            if state == "no_instance":
                continue
            connection.execute(Worker.__table__.insert().values(
                id=number, identity=f"worker-{number}", registered_at=now, last_heartbeat_at=now,
                capabilities=["start_instance"]))
            job_state = {"pending": "queued", "starting": "claimed", "healthy": "completed",
                         "failed": "failed", "stopped": "completed", "assigned": "assigned"}[state]
            instance_state = "pending" if state == "assigned" else state
            active_or_terminal = job_state != "queued"
            claimed = job_state in ("claimed", "completed", "failed")
            terminal = job_state in ("completed", "failed")
            claim_id = str(uuid4()) if claimed else None
            claims[number] = claim_id
            connection.execute(Job.__table__.insert().values(
                id=number * 10 + 1, deployment_id=number, kind="start_instance", state=job_state,
                created_at=now, next_run_at=now, max_attempts=3, attempt_count=int(active_or_terminal),
                worker_id=number if active_or_terminal else None, assigned_at=now if active_or_terminal else None,
                claimed_at=now if claimed else None, claim_id=claim_id, finished_at=now if terminal else None,
                result={"simulated": True} if job_state == "completed" else None,
                error="original failure" if job_state == "failed" else None,
                last_failure_code="permanent_failure" if job_state == "failed" else None,
                assignment_expires_at=now + timedelta(seconds=15) if job_state == "assigned" else None,
                lease_expires_at=now + timedelta(seconds=30) if job_state == "claimed" else None))
            instance = {
                "id": str(uuid4()), "deployment_id": number, "start_job_id": number * 10 + 1,
                "worker_id": number if active_or_terminal else None,
                "last_attempt_id": number + 100 if active_or_terminal else None,
                "state": instance_state, "version": 5 if state == "stopped" else 4 if terminal else 3 if claimed else 2 if active_or_terminal else 1,
                "created_at": stamp, "updated_at": stamp,
                "last_observed_at": stamp if terminal else None,
                "healthy_at": stamp if state in ("healthy", "stopped") else None,
                "stopped_at": stamp if state == "stopped" else None,
                "state_source": "manual" if state == "stopped" else "worker_report" if terminal else "control_plane",
                "state_reason": "simulated_stop" if state == "stopped" else "readiness_passed" if terminal else "created",
            }
            payload = ack = None
            if terminal:
                outcome = "healthy" if job_state == "completed" else "failed"
                payload = {"outcome": job_state, "result": {"simulated": True} if outcome == "healthy" else None,
                           "error": "original failure" if outcome == "failed" else None, "retryable": False,
                           "instance_outcome": {"instance_id": instance["id"], "expected_version": 3,
                                                "state": outcome, "phase": "readiness_passed" if outcome == "healthy" else "readiness_failed"}}
                row = connection.execute(select(Job).where(Job.id == number * 10 + 1)).mappings().one()
                ack = {key: value.isoformat() if isinstance(value, datetime) else value for key, value in row.items()}
                old_instance = {key: now.isoformat() if key.endswith("_at") and value is not None else value
                                for key, value in instance.items()}
                old_instance.update(state=outcome, version=4, stopped_at=None, state_source="worker_report",
                                    state_reason=payload["instance_outcome"]["phase"], simulated=True,
                                    worker_available=True, startup_job_state=job_state,
                                    current_worker_id=None, current_attempt_number=None, ownership_valid=False)
                ack.update(acknowledged_attempt_number=1, attempt_outcome=job_state, instance=old_instance)
                receipts.append((number, number * 10 + 1, {**payload, "claim_id": claim_id, "attempt_number": 1},
                                 ReportRead.model_validate(ack).model_dump_json(exclude_unset=True).encode()))
            if active_or_terminal:
                connection.execute(JobAttempt.__table__.insert().values(
                    id=number + 100, job_id=number * 10 + 1, number=1, worker_id=number, state=job_state,
                    assigned_at=now, claimed_at=now if claimed else None, claim_id=claim_id,
                    finished_at=now if terminal else None, failure_code="permanent_failure" if job_state == "failed" else None,
                    retryable=False, result=payload["result"] if payload else None,
                    error=payload["error"] if payload else None, report_payload=payload, report_ack=ack))
            if job_state == "claimed":
                connection.execute(JobRenewal.__table__.insert().values(
                    id=number + 200, attempt_id=number + 100, renewal_id=str(uuid4()),
                    granted_at=now, lease_expires_at=now + timedelta(seconds=30)))
            columns = ", ".join(instance)
            values = ", ".join(":" + key for key in instance)
            connection.execute(text(f"INSERT INTO instances ({columns}) VALUES ({values})"), instance)
    yield path, engine, now, receipts, claims
    engine.dispose()


def test_v5_preserves_all_records_and_byte_exact_historical_http_acknowledgments(version_four):
    path, engine, now, receipts, claims = version_four
    with engine.connect() as connection:
        before = snapshot(connection)
    for _ in range(2):
        with TestClient(create_app(path)) as client:
            client.app.dependency_overrides[get_clock] = lambda: lambda: now + timedelta(seconds=1)
            for worker_id, job_id, payload, original_response in receipts:
                response = client.post(f"/workers/{worker_id}/jobs/{job_id}/report", json=payload)
                assert response.status_code == 200, response.text
                assert response.content == original_response
                assert "replica_index" not in response.json()["instance"]
            # Existing active claims and their exact deadlines remain usable.
            response = client.post("/workers/2/jobs/21/claim", json={"claim_id": claims[2], "attempt_number": 1})
            assert response.status_code == 200 and response.json()["instance"]["replica_index"] == 0
    with engine.connect() as connection:
        after = snapshot(connection)
        for table in TABLES:
            if table == "instances":
                columns = [column[1] for column in connection.exec_driver_sql("PRAGMA table_info(instances)")]
                index = columns.index("replica_index")
                assert [tuple(value for name, value in zip(columns, row)
                              if name not in ("replica_index", "generation", "creation_reason"))
                        for row in after[table]] == before[table]
                assert all(row[index] == 0 for row in after[table])
            elif table != "schema_migrations":
                assert after[table] == before[table]
        assert after["schema_migrations"] == [(1,), (2,), (3,), (4,), (5,), (6,)]
        assert connection.exec_driver_sql("SELECT desired_replicas,version FROM deployment_targets ORDER BY deployment_id").all() == [(1, 1)] * 7
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM instances WHERE deployment_id=7").scalar_one() == 0
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        with pytest.raises(IntegrityError, match="UNIQUE"):
            connection.exec_driver_sql("INSERT INTO instances SELECT 'different-id', deployment_id, replica_index, generation, creation_reason, start_job_id, worker_id, last_attempt_id, state,version,created_at,updated_at,last_observed_at,healthy_at,stopped_at,state_source,state_reason FROM instances WHERE deployment_id=1")


@pytest.mark.parametrize("failure_point", ["drop_instances", "restore_instances", "version_record", "foreign_keys"])
def test_v5_failure_rolls_back_schema_records_and_target_creation(version_four, failure_point):
    _, engine, *_ = version_four
    with engine.connect() as connection:
        before = snapshot(connection)
        schema = connection.exec_driver_sql("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name").all()
    def fail(connection, cursor, statement, parameters, context, executemany):
        if ((failure_point == "drop_instances" and statement == "DROP TABLE instances")
                or (failure_point == "restore_instances" and statement.startswith("INSERT INTO instances ("))
                or (failure_point == "version_record" and statement == "INSERT INTO schema_migrations (version) VALUES (5)")
                or (failure_point == "foreign_keys" and statement == "PRAGMA foreign_key_check")):
            raise RuntimeError("interrupted v5")
    event.listen(engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="interrupted v5"):
            initialize_database(engine)
    finally:
        event.remove(engine, "before_cursor_execute", fail)
    with engine.connect() as connection:
        assert snapshot(connection) == before
        assert connection.exec_driver_sql("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name").all() == schema
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
    initialize_database(engine)


def test_concurrent_v5_startup_preserves_slots_targets_and_receipts(version_four):
    path, engine, *_ = version_four
    def startup(_):
        separate = create_database_engine(path)
        try:
            initialize_database(separate)
        finally:
            separate.dispose()
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(startup, range(4)))
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM deployment_targets").scalar_one() == 7
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM instances").scalar_one() == 6
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM job_attempts").scalar_one() == 5
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM job_renewals").scalar_one() == 1
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
