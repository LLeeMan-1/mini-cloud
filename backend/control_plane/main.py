"""Application factory; database initialization happens only during startup."""

from contextlib import asynccontextmanager
import math
import os
from pathlib import Path
import sqlite3

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError

from backend.control_plane.api.routes import router
from backend.control_plane.api.workers import router as worker_router
from backend.control_plane.api.jobs import router as job_router
from backend.control_plane.api.instances import router as instance_router
from backend.control_plane.api.replicas import router as replica_router
from backend.control_plane.persistence.database import DEFAULT_DATABASE_PATH, create_database_engine
from backend.control_plane.persistence.migrations import initialize_database
from backend.control_plane.job_policy import JobPolicy


def create_app(
    database_path: Path | str | None = None, *, worker_timeout: float | None = None,
    job_policy: JobPolicy | None = None,
) -> FastAPI:
    path = database_path if database_path is not None else os.getenv(
        "MINI_CLOUD_DATABASE_PATH", str(DEFAULT_DATABASE_PATH)
    )
    timeout = float(worker_timeout if worker_timeout is not None else os.getenv(
        "MINI_CLOUD_WORKER_TIMEOUT_SECONDS", "15"
    ))
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Worker timeout must be a finite positive number")
    policy = job_policy or JobPolicy.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = create_database_engine(path)
        app.state.engine = engine
        try:
            initialize_database(engine, policy)
            yield
        finally:
            engine.dispose()

    app = FastAPI(title="Mini Cloud Control Plane", lifespan=lifespan)

    @app.exception_handler(OperationalError)
    async def database_busy(_request, error):
        code = getattr(error.orig, "sqlite_errorcode", 0) or 0
        if (code & 0xff) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            return JSONResponse(status_code=503, headers={"Retry-After": "1"},
                                content={"detail": {"code": "database_busy", "message": "Database is busy; retry the request"}})
        raise error

    app.state.worker_timeout = timeout
    app.state.job_policy = policy
    app.include_router(router)
    app.include_router(worker_router)
    app.include_router(job_router)
    app.include_router(instance_router)
    app.include_router(replica_router)
    return app


app = create_app()
