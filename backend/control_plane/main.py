"""Application factory; database initialization happens only during startup."""

from contextlib import asynccontextmanager
import os
from pathlib import Path

from fastapi import FastAPI

from backend.control_plane.api.routes import router
from backend.control_plane.persistence.database import DEFAULT_DATABASE_PATH, create_database_engine
from backend.control_plane.persistence.models import Base


def create_app(database_path: Path | str | None = None) -> FastAPI:
    path = database_path if database_path is not None else os.getenv(
        "MINI_CLOUD_DATABASE_PATH", str(DEFAULT_DATABASE_PATH)
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = create_database_engine(path)
        app.state.engine = engine
        try:
            Base.metadata.create_all(engine)
            yield
        finally:
            engine.dispose()

    app = FastAPI(title="Mini Cloud Control Plane", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
