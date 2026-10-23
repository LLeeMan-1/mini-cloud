"""Small, versioned SQLite startup migrations, serialized in one transaction."""

from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.engine import Engine

from backend.control_plane.persistence.models import Application, Deployment, Worker, Job, JobAttempt, JobRenewal, utc_now
from backend.control_plane.job_policy import JobPolicy


SCHEMA_VERSION = 3


def create_version_two_jobs(connection):
    """Frozen old schema: older migrations must not depend on today's Job model."""
    connection.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY NOT NULL,
            deployment_id INTEGER NOT NULL UNIQUE REFERENCES deployments(id),
            worker_id INTEGER REFERENCES workers(id),
            kind VARCHAR(30) NOT NULL, state VARCHAR(20) NOT NULL,
            created_at DATETIME NOT NULL, assigned_at DATETIME, claimed_at DATETIME,
            finished_at DATETIME, claim_id VARCHAR(36) UNIQUE, result JSON, error VARCHAR(2000)
        )
    """)


def upgrade_jobs_v3(connection, policy, now):
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Migration time must be timezone-aware")
    now = now.astimezone(timezone.utc)
    # No tables reference jobs in v2. Rebuild it before adding attempt foreign keys.
    connection.exec_driver_sql("DROP TRIGGER IF EXISTS jobs_valid_transition")
    connection.exec_driver_sql("DROP INDEX IF EXISTS one_active_job_per_worker")
    connection.exec_driver_sql("DROP INDEX IF EXISTS jobs_queue_order")
    connection.exec_driver_sql("ALTER TABLE jobs RENAME TO jobs_v2")
    Job.__table__.create(connection)
    connection.exec_driver_sql("""
        INSERT INTO jobs (id, deployment_id, worker_id, kind, state, created_at, assigned_at,
            claimed_at, finished_at, claim_id, result, error, attempt_count, max_attempts,
            next_run_at, assignment_expires_at, lease_expires_at, last_failure_code)
        SELECT id, deployment_id, worker_id, kind, state, created_at, assigned_at,
            claimed_at, finished_at, claim_id, result, error,
            CASE WHEN state='queued' THEN 0 ELSE 1 END, ?, created_at,
            CASE WHEN state='assigned' THEN ? ELSE NULL END,
            CASE WHEN state='claimed' THEN ? ELSE NULL END,
            CASE WHEN state='failed' THEN 'permanent_failure' ELSE NULL END
        FROM jobs_v2
    """, (policy.max_attempts,
           (now + timedelta(seconds=policy.assignment_seconds)).replace(tzinfo=None).isoformat(" ", timespec="microseconds"),
           (now + timedelta(seconds=policy.lease_seconds)).replace(tzinfo=None).isoformat(" ", timespec="microseconds")))
    connection.exec_driver_sql("DROP TABLE jobs_v2")
    JobAttempt.__table__.create(connection)
    JobRenewal.__table__.create(connection)
    for row in connection.execute(select(Job)).mappings():
        if row["attempt_count"] == 0:
            continue
        terminal = row["state"] in ("completed", "failed")
        payload = {"outcome": row["state"], "result": row["result"], "error": row["error"],
                   "retryable": False} if terminal else None
        ack = None
        if terminal:
            ack = {key: value.isoformat() if isinstance(value, datetime) else value for key, value in row.items()}
            ack.update(acknowledged_attempt_number=1, attempt_outcome=row["state"])
        connection.execute(JobAttempt.__table__.insert().values(
            job_id=row["id"], number=1, worker_id=row["worker_id"], state=row["state"],
            claim_id=row["claim_id"], assigned_at=row["assigned_at"], claimed_at=row["claimed_at"],
            finished_at=row["finished_at"], failure_code=row["last_failure_code"], retryable=False,
            result=row["result"], error=row["error"], report_payload=payload, report_ack=ack,
        ))


def initialize_database(engine: Engine, policy: JobPolicy | None = None, clock=utc_now) -> None:
    """Upgrade known legacy schemas without resetting records or other tables.

    Explicit BEGIN makes SQLite DDL transactional even with sqlite3's legacy
    transaction mode. IMMEDIATE serializes simultaneous API startup migrations.
    """
    policy = policy or JobPolicy()
    with engine.connect() as connection:
        try:
            connection.exec_driver_sql("BEGIN IMMEDIATE")
            connection.exec_driver_sql("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY
                )
            """)
            version = connection.exec_driver_sql(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
            ).scalar_one()
            if version > SCHEMA_VERSION:
                raise RuntimeError("Database schema is newer than this control plane supports")
            if version < 1:
                columns = connection.exec_driver_sql("PRAGMA table_info(workers)").all()
                if any(column[1] == "status" for column in columns):
                    # SQLite cannot drop a column referenced by a table CHECK.
                    # Rebuild only workers, preserving IDs and timestamps exactly.
                    connection.exec_driver_sql("""
                        CREATE TABLE workers_migration_v1 (
                            id INTEGER NOT NULL PRIMARY KEY,
                            identity VARCHAR(100) NOT NULL UNIQUE,
                            registered_at DATETIME NOT NULL,
                            last_heartbeat_at DATETIME NOT NULL
                        )
                    """)
                    connection.exec_driver_sql("""
                        INSERT INTO workers_migration_v1
                            (id, identity, registered_at, last_heartbeat_at)
                        SELECT id, identity, registered_at, last_heartbeat_at FROM workers
                    """)
                    connection.exec_driver_sql("DROP TABLE workers")
                    connection.exec_driver_sql("ALTER TABLE workers_migration_v1 RENAME TO workers")
                for table in (Application.__table__, Deployment.__table__, Worker.__table__):
                    table.create(connection, checkfirst=True)
                connection.exec_driver_sql("INSERT INTO schema_migrations (version) VALUES (1)")
            if version < 2:
                create_version_two_jobs(connection)
                connection.exec_driver_sql("""
                    INSERT INTO jobs (deployment_id, kind, state, created_at)
                    SELECT d.id, 'prepare_deployment', 'queued', d.created_at
                    FROM deployments AS d
                    WHERE d.runtime_status = 'pending'
                      AND NOT EXISTS (SELECT 1 FROM jobs AS j WHERE j.deployment_id = d.id)
                """)
                connection.exec_driver_sql("INSERT INTO schema_migrations (version) VALUES (2)")
            if version < 3:
                upgrade_jobs_v3(connection, policy, clock())
                connection.exec_driver_sql("INSERT INTO schema_migrations (version) VALUES (3)")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
