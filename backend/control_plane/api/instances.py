from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Path, Query, Request, Response

from backend.control_plane.api.jobs import ServerClock, translate_error
from backend.control_plane.api.routes import DatabaseSession
from backend.control_plane.api.schemas import InstanceCreate, InstanceRead, InstanceState, InstanceStop
from backend.control_plane.services import instances, jobs


router = APIRouter(tags=["instances"])


@router.post("/deployments/{deployment_id}/instances", response_model=InstanceRead, status_code=201)
def create_instance(
    deployment_id: Annotated[int, Path(gt=0)], session: DatabaseSession,
    request: Request, response: Response, clock: ServerClock, body: InstanceCreate | None = None,
):
    try:
        instance, created = instances.create_instance(session, deployment_id, request.app.state.job_policy, clock)
        response.status_code = 201 if created else 200
        return instances.instance_view(session, instance, clock(), request.app.state.worker_timeout)
    except (jobs.RecordNotFound, jobs.JobConflict) as error:
        raise translate_error(error) from None


@router.get("/instances", response_model=list[InstanceRead])
def list_instances(
    session: DatabaseSession, request: Request, clock: ServerClock,
    deployment_id: Annotated[int | None, Query(gt=0)] = None,
    worker_id: Annotated[int | None, Query(gt=0)] = None,
    state: InstanceState | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    now = clock()
    records = instances.list_instances(session, deployment_id=deployment_id, worker_id=worker_id,
                                       state=state, limit=limit, offset=offset)
    return [instances.instance_view(session, instance, now, request.app.state.worker_timeout) for instance in records]


@router.get("/instances/{instance_id}", response_model=InstanceRead)
def get_instance(instance_id: UUID, session: DatabaseSession, request: Request, clock: ServerClock):
    try:
        return instances.instance_view(session, instances.get_instance(session, str(instance_id)), clock(), request.app.state.worker_timeout)
    except jobs.RecordNotFound as error:
        raise translate_error(error) from None


@router.post("/instances/{instance_id}/stop", response_model=InstanceRead)
def stop_instance(
    instance_id: UUID, body: InstanceStop, session: DatabaseSession, request: Request, clock: ServerClock,
):
    try:
        instance = instances.stop_instance(session, str(instance_id), body.expected_version, clock)
        return instances.instance_view(session, instance, clock(), request.app.state.worker_timeout)
    except (jobs.RecordNotFound, jobs.JobConflict) as error:
        raise translate_error(error) from None
