from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Request

from backend.control_plane.api.routes import DatabaseSession
from backend.control_plane.api.schemas import WorkerRead, WorkerRegister
from backend.control_plane.persistence.models import utc_now
from backend.control_plane.services import workers


router = APIRouter(prefix="/workers", tags=["workers"])
ServerTime = Annotated[datetime, Depends(utc_now)]


@router.post("/register", response_model=WorkerRead)
def register_worker(
    body: WorkerRegister, session: DatabaseSession, now: ServerTime, request: Request
):
    worker = workers.register_worker(session, body.identity, now)
    return workers.worker_view(worker, now, request.app.state.worker_timeout)


@router.post("/{id}/heartbeat", response_model=WorkerRead)
def heartbeat(
    id: Annotated[int, Path(gt=0)], session: DatabaseSession, now: ServerTime, request: Request
):
    try:
        worker = workers.heartbeat(session, id, now)
    except workers.WorkerNotFound:
        raise HTTPException(status_code=404, detail="Worker not found") from None
    return workers.worker_view(worker, now, request.app.state.worker_timeout)


@router.get("", response_model=list[WorkerRead])
def list_workers(session: DatabaseSession, now: ServerTime, request: Request):
    return [workers.worker_view(worker, now, request.app.state.worker_timeout)
            for worker in workers.list_workers(session)]
