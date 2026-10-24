"""Version 6 upgrades preserve v5 incarnations and all historical receipts."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event

from backend.control_plane.api.jobs import get_clock
from backend.control_plane.main import create_app
from backend.control_plane.persistence.database import create_database_engine
from backend.control_plane.persistence.migrations import initialize_database, upgrade_replicas_v5
from test_replica_migrations import version_four


TABLES = ("applications", "deployments", "workers", "jobs", "job_attempts", "job_renewals",
          "deployment_targets", "instances", "schema_migrations")


def snapshot(connection):
    return {table: connection.exec_driver_sql(f"SELECT * FROM {table} ORDER BY 1").all() for table in TABLES}


@pytest.fixture
def version_five(version_four):
    path, engine, now, receipts, claims = version_four
    with engine.begin() as connection:
        upgrade_replicas_v5(connection, now)
        connection.exec_driver_sql("INSERT INTO schema_migrations VALUES (5)")
    return path, engine, now, receipts, claims


def test_v6_preserves_every_v5_record_and_serialized_acknowledgment(version_five):
    path, engine, now, receipts, claims = version_five
    with engine.connect() as connection:
        before = snapshot(connection)
        old_columns = [row[1] for row in connection.exec_driver_sql("PRAGMA table_info(instances)")]
    for _ in range(2):
        with TestClient(create_app(path)) as client:
            client.app.dependency_overrides[get_clock] = lambda: lambda: now + timedelta(seconds=1)
            for worker_id, job_id, payload, original_response in receipts:
                response = client.post(f"/workers/{worker_id}/jobs/{job_id}/report", json=payload)
                assert response.status_code == 200
                assert response.content == original_response
                assert "generation" not in response.json()["instance"]
                assert "is_current" not in response.json()["instance"]
            claim = client.post("/workers/2/jobs/21/claim", json={"attempt_number": 1, "claim_id": claims[2]})
            assert claim.status_code == 200
            assert claim.json()["instance"]["generation"] == 1
            assert claim.json()["instance"]["is_current"] is True
    with engine.connect() as connection:
        after = snapshot(connection)
        for table in TABLES:
            if table == "instances":
                assert connection.exec_driver_sql(f"SELECT {','.join(old_columns)} FROM instances ORDER BY id").all() == before[table]
                assert connection.exec_driver_sql("SELECT generation,creation_reason FROM instances").all() == [(1, "initial")] * 6
            elif table != "schema_migrations":
                assert after[table] == before[table]
        assert after["schema_migrations"] == [(1,), (2,), (3,), (4,), (5,), (6,)]
        assert connection.exec_driver_sql("SELECT deployment_id,replica_index,current_generation,version,automatic_replacements,replacement_limit FROM replica_slots ORDER BY deployment_id").all() == [(i, 0, 1, 1, 0, 1) for i in range(1, 7)]
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM replica_slots WHERE deployment_id=7").scalar_one() == 0
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM replacement_receipts").scalar_one() == 0
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []


@pytest.mark.parametrize("failure_point", ["drop_instances", "create_slots", "restore_slots", "restore_instances", "create_receipts", "version_record"])
def test_v6_rollback_restores_old_tables_triggers_data_and_receipts(version_five, failure_point):
    _, engine, *_ = version_five
    with engine.connect() as connection:
        before = snapshot(connection)
        schema = connection.exec_driver_sql("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name").all()
    def fail(connection, cursor, statement, parameters, context, executemany):
        if ((failure_point == "drop_instances" and statement == "DROP TABLE instances")
                or (failure_point == "create_slots" and "CREATE TABLE replica_slots" in statement)
                or (failure_point == "restore_slots" and "INSERT INTO replica_slots" in statement)
                or (failure_point == "restore_instances" and statement.startswith("INSERT INTO instances ("))
                or (failure_point == "create_receipts" and "CREATE TABLE replacement_receipts" in statement)
                or (failure_point == "version_record" and statement == "INSERT INTO schema_migrations (version) VALUES (6)")):
            raise RuntimeError("interrupted v6")
    event.listen(engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="interrupted v6"):
            initialize_database(engine)
    finally:
        event.remove(engine, "before_cursor_execute", fail)
    with engine.connect() as connection:
        assert snapshot(connection) == before
        assert connection.exec_driver_sql("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name").all() == schema
        assert connection.exec_driver_sql("SELECT name FROM sqlite_temp_master WHERE name='v6_instances'").all() == []
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
    initialize_database(engine)


def test_concurrent_v6_startups_create_no_extra_generations(version_five):
    path, engine, *_ = version_five
    def startup(_):
        separate = create_database_engine(path)
        try:
            initialize_database(separate)
        finally:
            separate.dispose()
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(startup, range(4)))
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM instances").scalar_one() == 6
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM replica_slots").scalar_one() == 6
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM job_attempts").scalar_one() == 5
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM job_renewals").scalar_one() == 1
        assert connection.exec_driver_sql("SELECT MAX(generation) FROM instances").scalar_one() == 1
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
