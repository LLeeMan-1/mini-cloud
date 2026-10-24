from typing import Annotated
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from sqlalchemy.orm import Session

from backend.control_plane.api.schemas import (
    ApplicationCreate, ApplicationRead, DeploymentCreate, DeploymentRead, DeploymentPage,
)
from backend.control_plane.persistence.database import get_session
from backend.control_plane.persistence.models import utc_now
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
    id: Annotated[int, Path(gt=0)], body: DeploymentCreate, session: DatabaseSession, request: Request,
    now: Annotated[datetime, Depends(utc_now)],
):
    try:
        return applications.create_deployment(session, id, body.specification,
                                               request.app.state.job_policy.max_attempts, now,
                                               desired_replicas=body.desired_replicas)
    except applications.ApplicationNotFound:
        raise HTTPException(status_code=404, detail="Application not found") from None


@router.get("/deployments", response_model=list[DeploymentRead])
def list_deployments(session: DatabaseSession):
    return applications.list_deployments(session)


@router.get("/deployments/page", response_model=DeploymentPage)
def discover_deployments(
    session: DatabaseSession,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
    after_id: Annotated[int, Query(ge=0)] = 0,
    through_id: Annotated[int | None, Query(ge=0)] = None,
):
    if (through_id is None and after_id != 0) or (through_id is not None and after_id > through_id):
        raise HTTPException(status_code=422, detail="Continuation requires a valid upper bound")
    return applications.discover_deployments(session, limit, after_id, through_id)
