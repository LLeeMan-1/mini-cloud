"""Replica intent and explicitly invoked reconciliation."""

from typing import Annotated

from fastapi import APIRouter, Path, Request

from backend.control_plane.api.jobs import ServerClock, translate_error
from backend.control_plane.api.routes import DatabaseSession
from backend.control_plane.api.schemas import ReplicaSummary, ReplicaTargetRead, ReplicaTargetUpdate, ReconciliationRead
from backend.control_plane.services import jobs, replicas


router = APIRouter(tags=["replicas"])
DeploymentID = Annotated[int, Path(gt=0)]


@router.get("/deployments/{deployment_id}/replicas", response_model=ReplicaSummary)
def get_replicas(deployment_id: DeploymentID, session: DatabaseSession, request: Request, clock: ServerClock):
    try:
        return replicas.get_summary(session, deployment_id, request.app.state.worker_timeout, clock)
    except (jobs.RecordNotFound, jobs.JobConflict) as error:
        raise translate_error(error) from None


@router.put("/deployments/{deployment_id}/replicas", response_model=ReplicaTargetRead)
def update_replicas(deployment_id: DeploymentID, body: ReplicaTargetUpdate, session: DatabaseSession, clock: ServerClock):
    try:
        return replicas.update_target(session, deployment_id, body.desired_replicas, body.expected_version, clock)
    except (jobs.RecordNotFound, jobs.JobConflict) as error:
        raise translate_error(error) from None


@router.post("/deployments/{deployment_id}/reconcile", response_model=ReconciliationRead)
def reconcile(deployment_id: DeploymentID, session: DatabaseSession, request: Request, clock: ServerClock):
    try:
        return replicas.reconcile_deployment(session, deployment_id, request.app.state.job_policy, clock)
    except (jobs.RecordNotFound, jobs.JobConflict) as error:
        raise translate_error(error) from None
