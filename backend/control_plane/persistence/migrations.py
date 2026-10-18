"""Small, versioned SQLite startup migrations, serialized in one transaction."""

from sqlalchemy.engine import Engine

from backend.control_plane.persistence.models import Base


SCHEMA_VERSION = 1


def initialize_database(engine: Engine) -> None:
    """Upgrade known legacy schemas without resetting records or other tables.

    Explicit BEGIN makes SQLite DDL transactional even with sqlite3's legacy
    transaction mode. IMMEDIATE serializes simultaneous API startup migrations.
    """
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
                Base.metadata.create_all(connection)
                connection.exec_driver_sql("INSERT INTO schema_migrations (version) VALUES (1)")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
