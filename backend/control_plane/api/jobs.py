"""Job HTTP contracts. All decision-making and transactions live in services."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request

from backend.control_plane.api.routes import DatabaseSession
from backend.control_plane.api.schemas import (
    JobAssignment, JobClaim, JobRead, JobReport, JobState, SchedulingResult,
    AttemptRead, ClaimRead, ReportRead, JobRenew, RenewalRead, RecoveryResult,
)
from backend.control_plane.persistence.models import utc_now
from backend.control_plane.services import jobs, scheduling


router = APIRouter(tags=["jobs"])
PositiveID = Annotated[int, Path(gt=0)]


def get_clock() -> jobs.Clock:
    # Pass a callable so write services sample time after obtaining the lock.
    return utc_now


ServerClock = Annotated[jobs.Clock, Depends(get_clock)]


def translate_error(error: Exception) -> HTTPException:
    return HTTPException(status_code=404 if isinstance(error, jobs.RecordNotFound) else 409,
                         detail={"code": getattr(error, "code", "not_found"), "message": str(error)})


@router.post("/scheduling/run", response_model=SchedulingResult)
def run_scheduler(
    session: DatabaseSession, request: Request, clock: ServerClock,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
):
    assignments = scheduling.schedule_jobs(session, request.app.state.worker_timeout, limit, clock,
                                           request.app.state.job_policy)
    return {"assigned_count": len(assignments), "assignments": assignments}


@router.get("/jobs", response_model=list[JobRead])
def list_jobs(
    session: DatabaseSession, state: JobState | None = None,
    deployment_id: Annotated[int | None, Query(gt=0)] = None,
    worker_id: Annotated[int | None, Query(gt=0)] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    return jobs.list_jobs(session, state=state, deployment_id=deployment_id,
                          worker_id=worker_id, limit=limit, offset=offset)


@router.post("/jobs/recover", response_model=RecoveryResult)
def recover_jobs(
    session: DatabaseSession, request: Request, clock: ServerClock,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
):
    return jobs.recover_jobs(session, limit, clock, request.app.state.job_policy)


@router.get("/jobs/{job_id}/attempts", response_model=list[AttemptRead])
def list_attempts(job_id: PositiveID, session: DatabaseSession):
    try:
        return jobs.list_attempts(session, job_id)
    except jobs.RecordNotFound as error:
        raise translate_error(error) from None


@router.get("/jobs/{job_id}", response_model=JobRead)
def get_job(job_id: PositiveID, session: DatabaseSession):
    try:
        return jobs.get_job(session, job_id)
    except jobs.RecordNotFound as error:
        raise translate_error(error) from None


@router.get("/workers/{worker_id}/jobs", response_model=list[JobAssignment])
def poll_jobs(worker_id: PositiveID, session: DatabaseSession, clock: ServerClock):
    try:
        return jobs.poll_jobs(session, worker_id, clock)
    except jobs.RecordNotFound as error:
        raise translate_error(error) from None


@router.post("/workers/{worker_id}/jobs/{job_id}/claim", response_model=ClaimRead)
def claim_job(
    worker_id: PositiveID, job_id: PositiveID, body: JobClaim,
    session: DatabaseSession, request: Request, clock: ServerClock,
):
    try:
        return jobs.claim_job(session, worker_id, job_id, str(body.claim_id),
                              request.app.state.worker_timeout, clock,
                              attempt_number=body.attempt_number, policy=request.app.state.job_policy)
    except (jobs.RecordNotFound, jobs.JobConflict) as error:
        raise translate_error(error) from None


@router.post("/workers/{worker_id}/jobs/{job_id}/renew", response_model=RenewalRead)
def renew_job(
    worker_id: PositiveID, job_id: PositiveID, body: JobRenew,
    session: DatabaseSession, request: Request, clock: ServerClock,
):
    try:
        return jobs.renew_job(session, worker_id, job_id, body.attempt_number, str(body.claim_id),
                              str(body.renewal_id), clock, request.app.state.job_policy)
    except (jobs.RecordNotFound, jobs.JobConflict) as error:
        raise translate_error(error) from None


@router.post("/workers/{worker_id}/jobs/{job_id}/report", response_model=ReportRead)
def report_job(
    worker_id: PositiveID, job_id: PositiveID, body: JobReport,
    session: DatabaseSession, clock: ServerClock, request: Request,
):
    try:
        return jobs.report_job(session, worker_id, job_id, str(body.claim_id),
                               body.outcome, body.result, body.error, clock,
                               attempt_number=body.attempt_number, retryable=body.retryable,
                               policy=request.app.state.job_policy)
    except (jobs.RecordNotFound, jobs.JobConflict) as error:
        raise translate_error(error) from None
