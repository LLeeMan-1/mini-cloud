"""Application and deployment operations independent of HTTP."""

from copy import deepcopy
from datetime import datetime
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from backend.control_plane.persistence.models import Application, Deployment, DeploymentTarget, Job, utc_now
from backend.control_plane.services.jobs import write_transaction


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
    session: Session, application_id: int, specification: dict[str, Any], max_attempts: int = 3,
    now: datetime | None = None,
    *, desired_replicas: int = 1,
) -> Deployment:
    with write_transaction(session):
        if session.get(Application, application_id) is None:
            raise ApplicationNotFound(application_id)
        deployment = Deployment(
            application_id=application_id,
            specification=deepcopy(specification),
            runtime_status="pending",
            created_at=now or utc_now(),
        )
        session.add(deployment)
        session.flush()
        session.add(DeploymentTarget(deployment_id=deployment.id, desired_replicas=desired_replicas,
                                     created_at=deployment.created_at, updated_at=deployment.created_at))
        session.add(Job(deployment_id=deployment.id, max_attempts=max_attempts,
                        next_run_at=deployment.created_at, created_at=deployment.created_at))
    session.refresh(deployment)
    return deployment


def list_deployments(session: Session) -> list[Deployment]:
    return list(session.scalars(select(Deployment).order_by(Deployment.id)))


def discover_deployments(session, limit, after_id, through_id=None):
    """A bounded membership scan, not a snapshot of mutable deployment targets."""
    with session.begin():
        session.execute(text("BEGIN"))
        boundary = through_id if through_id is not None else session.scalar(
            select(func.coalesce(func.max(Deployment.id), 0)))
        ids = list(session.scalars(select(Deployment.id).where(
            Deployment.id > after_id, Deployment.id <= boundary).order_by(Deployment.id).limit(limit + 1)))
        page = ids[:limit]
        return {"items": [{"id": value} for value in page], "snapshot_max_id": boundary,
                "next_after_id": page[-1] if len(ids) > limit else None}
