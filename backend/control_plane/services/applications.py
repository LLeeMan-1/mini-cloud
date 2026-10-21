"""Application and deployment operations independent of HTTP."""

from copy import deepcopy
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.control_plane.persistence.models import Application, Deployment, Job


class ApplicationNotFound(Exception):
    pass


def create_application(session: Session, name: str, configuration: dict[str, Any]) -> Application:
    application = Application(name=name, configuration=deepcopy(configuration))
    session.add(application)
    session.commit()
    session.refresh(application)
    return application


def list_applications(session: Session) -> list[Application]:
    return list(session.scalars(select(Application).order_by(Application.id)))


def create_deployment(
    session: Session, application_id: int, specification: dict[str, Any]
) -> Deployment:
    if session.get(Application, application_id) is None:
        raise ApplicationNotFound(application_id)
    deployment = Deployment(
        application_id=application_id,
        specification=deepcopy(specification),
        runtime_status="pending",
    )
    session.add(deployment)
    session.flush()
    session.add(Job(deployment_id=deployment.id))
    session.commit()
    session.refresh(deployment)
    return deployment


def list_deployments(session: Session) -> list[Deployment]:
    return list(session.scalars(select(Deployment).order_by(Deployment.id)))
