"""Small, versioned SQLite startup migrations, serialized in one transaction."""

from datetime import datetime, timedelta, timezone

from sqlalchemy import literal, select
from sqlalchemy.engine import Engine

from backend.control_plane.persistence.models import Application, Deployment, Worker, Job, JobAttempt, JobRenewal, Instance, DeploymentTarget, utc_now
from backend.control_plane.persistence import schema_v3, schema_v4
from backend.control_plane.job_policy import JobPolicy


SCHEMA_VERSION = 5


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
    schema_v3.create_table(connection, "jobs")
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
    schema_v3.create_table(connection, "job_attempts")
    schema_v3.create_table(connection, "job_renewals")
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


def upgrade_instances_v4(connection):
    """Rebuild parent/children together with foreign keys enabled throughout."""
    tables = (Job.__table__, JobAttempt.__table__, JobRenewal.__table__)
    for table in tables:
        connection.exec_driver_sql(f"CREATE TEMP TABLE v4_{table.name} AS SELECT * FROM {table.name}")
    for table in reversed(tables):
        connection.exec_driver_sql(f"DROP TABLE {table.name}")
    for table in tables:
        schema_v4.create_table(connection, table.name)
        columns = ", ".join(column.name for column in table.columns)
        connection.exec_driver_sql(f"INSERT INTO {table.name} ({columns}) SELECT {columns} FROM v4_{table.name}")
        connection.exec_driver_sql(f"DROP TABLE temp.v4_{table.name}")
    columns = connection.exec_driver_sql("PRAGMA table_info(workers)").all()
    if not any(column[1] == "capabilities" for column in columns):
        connection.exec_driver_sql("""ALTER TABLE workers ADD COLUMN capabilities JSON NOT NULL DEFAULT '["prepare_deployment"]'""")
    schema_v4.create_table(connection, "instances")
    if connection.exec_driver_sql("PRAGMA foreign_key_check").all():
        raise RuntimeError("Foreign-key integrity check failed during version 4 migration")


def upgrade_replicas_v5(connection, now):
    DeploymentTarget.__table__.create(connection)
    connection.execute(DeploymentTarget.__table__.insert().from_select(
        ["deployment_id", "desired_replicas", "version", "created_at", "updated_at"],
        select(Deployment.id, literal(1), literal(1),
               literal(now, type_=DeploymentTarget.created_at.type),
               literal(now, type_=DeploymentTarget.updated_at.type)),
        include_defaults=False,
    ))
    connection.exec_driver_sql("CREATE TEMP TABLE v5_instances AS SELECT * FROM instances")
    connection.exec_driver_sql("DROP TABLE instances")
    Instance.__table__.create(connection)
    columns = ", ".join(column.name for column in Instance.__table__.columns)
    restored = ", ".join("0" if column.name == "replica_index" else column.name
                         for column in Instance.__table__.columns)
    connection.exec_driver_sql(f"INSERT INTO instances ({columns}) SELECT {restored} FROM v5_instances")
    connection.exec_driver_sql("DROP TABLE temp.v5_instances")
    if connection.exec_driver_sql("PRAGMA foreign_key_check").all():
        raise RuntimeError("Foreign-key integrity check failed during version 5 migration")


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
                for table in (Application.__table__, Deployment.__table__):
                    table.create(connection, checkfirst=True)
                if not connection.exec_driver_sql("PRAGMA table_info(workers)").all():
                    schema_v3.create_table(connection, "workers")
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
            if version < 4:
                upgrade_instances_v4(connection)
                connection.exec_driver_sql("INSERT INTO schema_migrations (version) VALUES (4)")
            if version < 5:
                upgrade_replicas_v5(connection, clock())
                connection.exec_driver_sql("INSERT INTO schema_migrations (version) VALUES (5)")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
