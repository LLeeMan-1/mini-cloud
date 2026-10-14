from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.control_plane.main import create_app
from backend.control_plane.persistence.models import Deployment


def create_application(client):
    response = client.post("/applications", json={
        "name": " example ", "configuration": {"owner": "local", "tags": ["demo"]}
    })
    assert response.status_code == 201
    return response.json()


def test_health_and_empty_lists(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    for route in ("/applications", "/deployments"):
        response = client.get(route)
        assert response.status_code == 200
        assert response.json() == []


def test_create_and_list_application(client):
    application = create_application(client)
    assert application["id"] > 0
    assert application["name"] == "example"
    assert application["configuration"] == {"owner": "local", "tags": ["demo"]}
    assert datetime.fromisoformat(application["created_at"]).utcoffset().total_seconds() == 0
    assert client.get("/applications").json() == [application]


def test_default_configuration(client):
    response = client.post("/applications", json={"name": "minimal"})
    assert response.status_code == 201
    assert response.json()["configuration"] == {}


@pytest.mark.parametrize("body", [
    {}, {"name": ""}, {"name": "   "}, {"name": "x" * 101},
    {"name": 123}, {"name": None}, {"name": "demo", "configuration": []},
    {"name": "demo", "configuration": None}, {"name": "demo", "unexpected": True},
])
def test_invalid_applications_do_not_persist(client, body):
    assert client.post("/applications", json=body).status_code == 422
    assert client.get("/applications").json() == []


def test_create_and_list_deployments(client):
    application = create_application(client)
    specification = {"image": "example:v1", "environment": {"MODE": "demo"}}
    response = client.post(
        f"/applications/{application['id']}/deployments", json={"specification": specification}
    )
    assert response.status_code == 201
    deployment = response.json()
    assert deployment["application_id"] == application["id"]
    assert deployment["specification"] == specification
    assert deployment["runtime_status"] == "pending"
    assert datetime.fromisoformat(deployment["created_at"]).tzinfo is not None
    assert client.get("/deployments").json() == [deployment]
    second = client.post(
        f"/applications/{application['id']}/deployments",
        json={"specification": {"image": "example:v2"}},
    )
    assert second.status_code == 201
    assert second.json()["id"] != deployment["id"]
    assert client.get("/deployments").json()[0] == deployment


@pytest.mark.parametrize("body", [
    {}, {"specification": {}}, {"specification": []}, {"specification": "code"},
    {"specification": None}, {"specification": {"image": "demo"}, "runtime_status": "healthy"},
])
def test_invalid_deployment_requests(client, body):
    application = create_application(client)
    response = client.post(f"/applications/{application['id']}/deployments", json=body)
    assert response.status_code == 422
    assert client.get("/deployments").json() == []


@pytest.mark.parametrize("application_id,status", [(999, 404), (0, 422), (-1, 422), ("bad", 422)])
def test_invalid_application_reference(client, application_id, status):
    response = client.post(
        f"/applications/{application_id}/deployments", json={"specification": {"image": "demo"}}
    )
    assert response.status_code == status
    assert client.get("/deployments").json() == []


def test_malformed_json(client):
    response = client.post(
        "/applications", content="{broken", headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422


def test_database_persists_across_app_restarts(tmp_path):
    path = tmp_path / "persistent.db"
    with TestClient(create_app(path)) as first:
        application = create_application(first)
        deployment = first.post(
            f"/applications/{application['id']}/deployments",
            json={"specification": {"image": "demo:v1"}},
        ).json()
    assert path.is_file()
    with TestClient(create_app(path)) as second:
        assert second.get("/applications").json() == [application]
        assert second.get("/deployments").json() == [deployment]


def test_specification_is_immutable_but_status_can_change(client):
    application = create_application(client)
    deployment = client.post(
        f"/applications/{application['id']}/deployments",
        json={"specification": {"image": "demo:v1"}},
    ).json()
    with Session(client.app.state.engine) as session:
        record = session.get(Deployment, deployment["id"])
        record.specification = {"image": "demo:v2"}
        with pytest.raises(IntegrityError, match="specification is immutable"):
            session.commit()
        session.rollback()
        record.runtime_status = "healthy"
        session.commit()
    stored = client.get("/deployments").json()[0]
    assert stored["specification"] == deployment["specification"]
    assert stored["runtime_status"] == "healthy"


def test_database_rejects_orphan_deployment(client):
    with Session(client.app.state.engine) as session:
        session.add(Deployment(application_id=999, specification={"image": "demo"}))
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            session.commit()


def test_database_rejects_invalid_runtime_status(client):
    application = create_application(client)
    with Session(client.app.state.engine) as session:
        session.add(Deployment(
            application_id=application["id"], specification={"image": "demo"}, runtime_status="invalid"
        ))
        with pytest.raises(IntegrityError, match="valid_runtime_status"):
            session.commit()


def test_raw_sql_cannot_change_specification(client):
    application = create_application(client)
    deployment = client.post(
        f"/applications/{application['id']}/deployments",
        json={"specification": {"image": "demo:v1"}},
    ).json()
    with client.app.state.engine.begin() as connection:
        with pytest.raises(IntegrityError, match="specification is immutable"):
            connection.execute(
                text("UPDATE deployments SET specification = :spec WHERE id = :id"),
                {"spec": '{"image":"demo:v2"}', "id": deployment["id"]},
            )
