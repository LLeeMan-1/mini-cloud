"""Exercise real legacy SQLite schemas, including transactional DDL rollback."""

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event
from sqlalchemy.exc import IntegrityError

from backend.control_plane.main import create_app
from backend.control_plane.persistence.database import create_database_engine
from backend.control_plane.persistence.migrations import SCHEMA_VERSION, initialize_database
from backend.control_plane.persistence.models import Application, Deployment, Worker


@pytest.fixture
def legacy_database(tmp_path):
    path = tmp_path / "legacy.db"
    engine = create_database_engine(path)
    with engine.begin() as connection:
        Application.__table__.create(connection)
        Deployment.__table__.create(connection)
        connection.exec_driver_sql("""
            CREATE TABLE workers (
                id INTEGER NOT NULL PRIMARY KEY,
                identity VARCHAR(100) NOT NULL UNIQUE,
                registered_at DATETIME NOT NULL,
                last_heartbeat_at DATETIME NOT NULL,
                status VARCHAR(20) NOT NULL,
                CONSTRAINT valid_worker_status CHECK (status IN ('available', 'unavailable'))
            )
        """)
        connection.exec_driver_sql("""
            INSERT INTO applications VALUES
                (3, 'existing-app', '2026-10-14 00:00:00.000000', '{"owner":"local"}')
        """)
        connection.exec_driver_sql("""
            INSERT INTO deployments VALUES
                (9, 3, '2026-10-14 00:00:00.000000', '{"image":"demo:v1"}', 'pending')
        """)
        connection.exec_driver_sql("""
            INSERT INTO workers VALUES
                (7, 'existing-worker', '2026-10-14 00:00:00.000000',
                 '2026-10-14 00:00:05.123456', 'unavailable'),
                (12, 'second-worker', '2026-10-14 00:00:01.000000',
                 '2026-10-14 00:00:06.000000', 'available')
        """)
    yield path, engine
    engine.dispose()


def snapshot(connection):
    return {
        table: connection.exec_driver_sql(f"SELECT * FROM {table} ORDER BY id").all()
        for table in ("applications", "deployments", "workers")
    }


def test_legacy_upgrade_preserves_data_constraints_and_trigger(legacy_database):
    path, engine = legacy_database
    with engine.connect() as connection:
        before = snapshot(connection)
    initialize_database(engine)
    initialize_database(engine)  # Repeated startups must be harmless.
    with engine.connect() as connection:
        after = snapshot(connection)
        assert after["applications"] == before["applications"]
        assert after["deployments"] == before["deployments"]
        assert after["workers"] == [row[:4] for row in before["workers"]]
        assert [row[1] for row in connection.exec_driver_sql("PRAGMA table_info(workers)")] == [
            "id", "identity", "registered_at", "last_heartbeat_at"
        ]
        table_sql = connection.exec_driver_sql(
            "SELECT sql FROM sqlite_master WHERE name = 'workers'"
        ).scalar_one()
        assert "valid_worker_status" not in table_sql
        assert connection.exec_driver_sql("SELECT version FROM schema_migrations ORDER BY version").all() == [(1,), (2,)]
    with engine.begin() as connection:
        with pytest.raises(IntegrityError, match="specification is immutable"):
            connection.exec_driver_sql("UPDATE deployments SET specification = '{}' WHERE id = 9")
    with engine.begin() as connection:
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            connection.exec_driver_sql("UPDATE deployments SET application_id = 999 WHERE id = 9")
    with engine.begin() as connection:
        with pytest.raises(IntegrityError, match="UNIQUE"):
            connection.exec_driver_sql("""
                INSERT INTO workers (identity, registered_at, last_heartbeat_at)
                SELECT identity, registered_at, last_heartbeat_at FROM workers WHERE id = 7
            """)
    with TestClient(create_app(path)) as client:
        result = client.post("/workers/register", json={"identity": "existing-worker"})
        assert result.status_code == 200
        assert result.json()["id"] == 7
        assert result.json()["registered_at"] == "2026-10-14T00:00:00Z"
        assert client.post("/workers/12/heartbeat").status_code == 200
        assert client.post("/workers/register", json={"identity": "new-worker"}).status_code == 200


def test_migration_failure_rolls_back_table_rebuild(legacy_database):
    _, engine = legacy_database
    with engine.connect() as connection:
        before = snapshot(connection)

    def fail_before_rename(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("ALTER TABLE workers_migration_v1"):
            raise RuntimeError("Simulated migration interruption")

    event.listen(engine, "before_cursor_execute", fail_before_rename)
    try:
        with pytest.raises(RuntimeError, match="Simulated migration interruption"):
            initialize_database(engine)
    finally:
        event.remove(engine, "before_cursor_execute", fail_before_rename)
    with engine.connect() as connection:
        assert snapshot(connection) == before
        names = connection.exec_driver_sql("SELECT name FROM sqlite_master WHERE type = 'table'").scalars().all()
        assert "workers_migration_v1" not in names
        assert "schema_migrations" not in names
    initialize_database(engine)  # A later startup can retry successfully.


def test_fresh_database_has_no_stored_status(tmp_path):
    engine = create_database_engine(tmp_path / "fresh.db")
    try:
        initialize_database(engine)
        with engine.connect() as connection:
            assert "status" not in [row[1] for row in connection.exec_driver_sql("PRAGMA table_info(workers)")]
    finally:
        engine.dispose()


def test_simultaneous_startups_serialize_migration(legacy_database):
    path, engine = legacy_database

    def startup(_):
        separate_engine = create_database_engine(path)
        try:
            initialize_database(separate_engine)
        finally:
            separate_engine.dispose()

    with ThreadPoolExecutor(max_workers=3) as executor:
        list(executor.map(startup, range(3)))
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT version FROM schema_migrations ORDER BY version").all() == [(1,), (2,)]
        assert connection.exec_driver_sql("SELECT id FROM workers ORDER BY id").all() == [(7,), (12,)]
        assert "status" not in [row[1] for row in connection.exec_driver_sql("PRAGMA table_info(workers)")]


def test_newer_schema_is_rejected_without_changes(tmp_path):
    engine = create_database_engine(tmp_path / "future.db")
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)")
            connection.exec_driver_sql("INSERT INTO schema_migrations VALUES (?)", (SCHEMA_VERSION + 1,))
        with pytest.raises(RuntimeError, match="newer"):
            initialize_database(engine)
        with engine.connect() as connection:
            assert connection.exec_driver_sql("SELECT version FROM schema_migrations").scalar_one() == SCHEMA_VERSION + 1
            assert connection.exec_driver_sql("PRAGMA table_info(workers)").all() == []
    finally:
        engine.dispose()


@pytest.fixture
def version_one_database(tmp_path):
    path = tmp_path / "version-one.db"
    engine = create_database_engine(path)
    with engine.begin() as connection:
        for table in (Application.__table__, Deployment.__table__, Worker.__table__):
            table.create(connection)
        connection.exec_driver_sql("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)")
        connection.exec_driver_sql("INSERT INTO schema_migrations VALUES (1)")
        connection.exec_driver_sql("""
            INSERT INTO applications VALUES (1, 'existing', '2026-10-14 00:00:00.000000', '{}')
        """)
        connection.exec_driver_sql("""
            INSERT INTO deployments VALUES
                (1, 1, '2026-10-14 00:00:00.000000', '{"image":"demo"}', 'pending'),
                (2, 1, '2026-10-14 00:00:01.000000', '{"image":"old"}', 'healthy')
        """)
        connection.exec_driver_sql("""
            INSERT INTO workers VALUES (8, 'existing-node', '2026-10-14 00:00:00.000000',
                                        '2026-10-14 00:00:05.123456')
        """)
    yield path, engine
    engine.dispose()


def test_version_two_backfills_only_pending_and_preserves_all_records(version_one_database):
    _, engine = version_one_database
    with engine.connect() as connection:
        before = snapshot(connection)
    initialize_database(engine)
    initialize_database(engine)
    with engine.connect() as connection:
        assert snapshot(connection) == before
        assert connection.exec_driver_sql(
            "SELECT deployment_id, state, created_at FROM jobs"
        ).all() == [(1, 'queued', '2026-10-14 00:00:00.000000')]
        assert connection.exec_driver_sql("SELECT version FROM schema_migrations ORDER BY version").all() == [(1,), (2,)]


def test_version_two_failure_rolls_back_and_can_retry(version_one_database):
    _, engine = version_one_database
    with engine.connect() as connection:
        before = snapshot(connection)

    def fail_version_record(connection, cursor, statement, parameters, context, executemany):
        if statement == "INSERT INTO schema_migrations (version) VALUES (2)":
            raise RuntimeError("Interrupted version 2 migration")

    event.listen(engine, "before_cursor_execute", fail_version_record)
    try:
        with pytest.raises(RuntimeError, match="Interrupted version 2"):
            initialize_database(engine)
    finally:
        event.remove(engine, "before_cursor_execute", fail_version_record)
    with engine.connect() as connection:
        assert snapshot(connection) == before
        assert connection.exec_driver_sql("PRAGMA table_info(jobs)").all() == []
        assert connection.exec_driver_sql("SELECT version FROM schema_migrations").all() == [(1,)]
    initialize_database(engine)


def test_simultaneous_version_two_upgrades(version_one_database):
    path, engine = version_one_database

    def upgrade(_):
        separate_engine = create_database_engine(path)
        try:
            initialize_database(separate_engine)
        finally:
            separate_engine.dispose()

    with ThreadPoolExecutor(max_workers=3) as executor:
        list(executor.map(upgrade, range(3)))
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM jobs").scalar_one() == 1
