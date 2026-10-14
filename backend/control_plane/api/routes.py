from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.orm import Session

from backend.control_plane.api.schemas import (
    ApplicationCreate, ApplicationRead, DeploymentCreate, DeploymentRead,
)
from backend.control_plane.persistence.database import get_session
from backend.control_plane.services import applications


router = APIRouter()
DatabaseSession = Annotated[Session, Depends(get_session)]


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/applications", response_model=ApplicationRead, status_code=201)
def create_application(body: ApplicationCreate, session: DatabaseSession):
    return applications.create_application(session, body.name, body.configuration)


@router.get("/applications", response_model=list[ApplicationRead])
def list_applications(session: DatabaseSession):
    return applications.list_applications(session)


@router.post("/applications/{id}/deployments", response_model=DeploymentRead, status_code=201)
def create_deployment(
    id: Annotated[int, Path(gt=0)], body: DeploymentCreate, session: DatabaseSession
):
    try:
        return applications.create_deployment(session, id, body.specification)
    except applications.ApplicationNotFound:
        raise HTTPException(status_code=404, detail="Application not found") from None


@router.get("/deployments", response_model=list[DeploymentRead])
def list_deployments(session: DatabaseSession):
    return applications.list_deployments(session)
