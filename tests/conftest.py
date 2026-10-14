import pytest
from fastapi.testclient import TestClient

from backend.control_plane.main import create_app


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path / "test.db")) as client:
        yield client
