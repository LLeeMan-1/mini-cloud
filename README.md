# Mini Cloud

A lightweight, self-hosted application deployment and orchestration platform.
Milestone 1A implements FastAPI and SQLite persistence. Milestone 1B adds a separate
HTTP worker agent, worker registration, and heartbeats. Scheduling, Docker,
application execution, and the frontend are future milestones.

## Windows PowerShell setup

Use Python 3.11 or newer. From the repository root:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m uvicorn backend.control_plane.main:app --host 127.0.0.1 --port 8000
```

Calling the environment's Python directly avoids changing PowerShell execution policy.
For runtime-only installation, use `requirements.txt`. Stop the server with Ctrl+C.
Open <http://127.0.0.1:8000/docs> for interactive documentation. You can append `--reload`
during development; one server process is sufficient.

The default database is `var/mini-cloud.db`, resolved relative to the repository.
To use another file, set this variable before starting the server:

```powershell
$env:MINI_CLOUD_DATABASE_PATH = Join-Path $PWD "var\development.db"
```

SQLite enables foreign keys and WAL mode. Each request gets its own SQLAlchemy session.
Virtual environments, databases, and SQLite journal files are ignored by Git.
Startup runs the small SQLite migration runner in `persistence/migrations.py`.
It records the schema version in `schema_migrations`. Version 1 creates missing tables
and upgrades the earlier Milestone 1B workers table to remove stored status. Existing
application and deployment tables, records, foreign keys, and deployment immutability
trigger are preserved. Worker IDs, identities, and timestamps are copied unchanged.

SQLite cannot drop a column used by a table CHECK constraint directly, so the migration
copies workers into a replacement table with no status column or status constraint,
then replaces the old workers table within one explicit transaction. `BEGIN IMMEDIATE`
serializes competing startup migrations. Failure rolls back the schema and data changes
and prevents startup; a later startup can retry. Repeated startups do not repeat the
migration, and databases with newer schema versions are rejected. Stop old API processes
before starting the upgraded version; old code expects the removed status column.
Do not delete or reset the database to upgrade. No migration dependency was added.
Future schema changes require another explicit versioned migration; `create_all` alone
does not upgrade tables.

## Run the control plane and worker separately

After installing dependencies, use two PowerShell terminals. Terminal 1 runs the API:

```powershell
Set-Location C:\Projects\mini-cloud-1
$env:MINI_CLOUD_WORKER_TIMEOUT_SECONDS = "15"
.\.venv\Scripts\python.exe -m uvicorn backend.control_plane.main:app --host 127.0.0.1 --port 8000
```

Terminal 2 runs the worker as an independent process:

```powershell
Set-Location C:\Projects\mini-cloud-1
.\.venv\Scripts\python.exe -m backend.worker.agent --identity laptop-worker-1 --api-url http://127.0.0.1:8000 --heartbeat-interval 5
```

Use the same identity after restarting this worker. Give different workers different
identities; two processes using one identity represent the same worker record.
The agent has no database access and imports no control-plane modules. HTTPX is its
only third-party runtime dependency. For a worker-only environment, install it with
`python -m pip install 'httpx>=0.28,<1.0'`.

Stop either process with Ctrl+C. Stopping a worker does not send an unavailable event;
its heartbeat expires naturally. Inspect availability from another terminal:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/workers | Format-Table
```

The heartbeat interval defaults to 5 seconds. `--request-timeout` defaults to 5 seconds
per HTTP operation. The interval is the delay after a successful request, so network
latency adds to the time between heartbeats. The API's availability timeout defaults
to 15 seconds and must be positive and finite. Keep it comfortably above the heartbeat
interval. Configure it before starting the API and use the same value for all API processes.

Connection failures, request timeouts, HTTP 408/429, and server errors retry with delays
of 1, 2, 4, 8, 16, then at most 30 seconds. A successful request resets the backoff.
The agent can start before the API and recover after API downtime. A heartbeat 404
causes re-registration with the same identity. Other client errors stop the agent
with an error for operator correction. There is no retry limit for transient errors.

## API

| Method | Path | Behavior |
| --- | --- | --- |
| GET | `/health` | Liveness response: `{"status":"ok"}`. |
| POST | `/applications` | Create an application; returns 201. |
| GET | `/applications` | List applications in ID order. |
| POST | `/applications/{id}/deployments` | Create a pending deployment; returns 201, or 404 for an unknown application. |
| GET | `/deployments` | List deployments in ID order. |
| POST | `/workers/register` | Register or refresh a worker by identity; returns 200. |
| POST | `/workers/{id}/heartbeat` | Refresh a known worker; returns 200, or 404 for an unknown worker. |
| GET | `/workers` | List workers in ID order with calculated availability. |

Worker registration accepts `{"identity":"laptop-worker-1"}`. Identity is case-sensitive,
trimmed, and must be a string of 1–100 characters; extra fields return 422. A unique
database constraint and atomic upsert prevent duplicate records, including simultaneous
registrations. Re-registration preserves the worker ID and original registration time
while refreshing its heartbeat. Heartbeat requests need no body and accept positive IDs.
Clients do not supply timestamps or choose status.

Worker responses include `id`, `identity`, `registered_at`, `last_heartbeat_at`, and
`status` (`available` or `unavailable`). Both timestamps are server-generated, timezone-aware
UTC in Python and API responses. SQLite stores their UTC values without offsets, and the
worker timestamp type restores the UTC timezone on retrieval.

SQLite stores no worker availability status. Every worker response derives `status`
exclusively from the latest heartbeat timestamp and configured timeout: a heartbeat
older than the timeout is `unavailable`; exactly at the timeout it is still `available`.
Listing does not write to the database. A fresh heartbeat or registration makes the
worker available again. No background expiration task runs in any API process.

Registration upserts and heartbeat updates use SQLite's scalar `max` inside their write
statements to preserve the later of the stored and incoming timestamps atomically.
Delayed or out-of-order requests therefore cannot move the last heartbeat backward.
Timestamps are normalized to the same UTC storage format before SQL comparison.
Wall-clock changes can still affect calculated availability; a backward clock adjustment
can temporarily leave the stored heartbeat in the future.

Application bodies contain `name` and optional `configuration` (a JSON object, default `{}`).
Names are trimmed and must have 1–100 characters. Deployment bodies contain `specification`
(a required, nonempty JSON object). Invalid inputs and unknown body fields return 422.
Specifications are opaque JSON at this stage; execution settings will receive their own
validation when an execution adapter is introduced. Submitted data is stored and never executed.

Example requests:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
$application = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/applications -ContentType 'application/json' -Body '{"name":"demo","configuration":{"owner":"local"}}'
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8000/applications/$($application.id)/deployments" -ContentType 'application/json' -Body '{"specification":{"image":"example:v1"}}'
Invoke-RestMethod http://127.0.0.1:8000/deployments
```

Responses include integer IDs and UTC creation timestamps. A deployment references its
application, stores a separate specification snapshot, and starts with `runtime_status`
set to `pending`. A SQLite trigger rejects specification changes, including direct SQL
updates. Runtime status is a separate constrained column; there is no status mutation
endpoint or background processing yet. A new specification requires a new deployment.
Application names need not be unique.

The API currently has no authentication and is intended for local development; the server
command binds to the laptop's loopback interface.

## Structure and request flow

```text
backend/control_plane/
  main.py                  App factory and startup/shutdown lifecycle
  api/routes.py            HTTP endpoints and HTTP error mapping
  api/schemas.py           Pydantic input/output contracts
  api/workers.py           Worker HTTP endpoints
  services/applications.py Application and deployment operations
  services/workers.py      Registration, heartbeats, and availability calculation
  persistence/models.py    SQLAlchemy entities and database constraints
  persistence/database.py  SQLite engine and request-scoped sessions
  persistence/migrations.py Versioned, transactional SQLite startup upgrades
backend/worker/
  agent.py                 Separate HTTP client process and retry loop
tests/                     API validation and persistence tests
var/                       Ignored runtime data
```

For `POST /applications/{id}/deployments`, FastAPI validates the positive application ID
and JSON body with Pydantic. The route receives a request-scoped database session and calls
the service. The service checks that the application exists, copies the specification into
a SQLAlchemy entity, commits it to SQLite, and refreshes it to obtain generated fields.
The route serializes the entity through its Pydantic response schema. Missing applications
become HTTP 404 responses. The session closes after the request, rolling back uncommitted work.

Services do not depend on HTTP types, and persistence does not depend on API schemas.
No repository wrapper or separate domain layer is necessary for these simple operations.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Tests use temporary SQLite files, never the development database. They cover successful
requests, invalid inputs, missing applications, separate deployment snapshots, UTC
timestamps, persistence across restarts, foreign keys, specification immutability, and
independent runtime status changes.
Worker tests additionally cover duplicate and concurrent registration, UTC persistence,
the exact timeout boundary, custom timeout settings, heartbeat recovery, restart
persistence, and opening an existing Milestone 1A database. Agent tests use HTTPX mock
transports and a simulated wait event to exercise outages, timeouts, capped backoff,
re-registration, and recovery without real sleeps or network access.
Additional tests deliberately send out-of-order timestamps, mix concurrent registrations
and heartbeats, and compare timestamps supplied with non-UTC offsets. Migration tests
verify legacy record preservation, removal of the status column/constraint, retained
uniqueness/foreign keys/deployment trigger, repeated startup, and rollback after a
simulated failure midway through the table replacement.

## Manual worker checks with two VS Code PowerShell terminals

Run the API in terminal 1 (stop any older API process first):

```powershell
Set-Location C:\Projects\mini-cloud-1
$env:MINI_CLOUD_WORKER_TIMEOUT_SECONDS = "15"
.\.venv\Scripts\python.exe -m uvicorn backend.control_plane.main:app --host 127.0.0.1 --port 8000
```

In terminal 2, register a stable identity and check that re-registration reuses its ID:

```powershell
Set-Location C:\Projects\mini-cloud-1
$base = "http://127.0.0.1:8000"
$worker = Invoke-RestMethod -Method Post -Uri "$base/workers/register" -ContentType 'application/json' -Body '{"identity":"manual-worker-1"}'
$worker | Format-List
$again = Invoke-RestMethod -Method Post -Uri "$base/workers/register" -ContentType 'application/json' -Body '{"identity":"manual-worker-1"}'
$again.id -eq $worker.id
```

Expect `available` and `True`. With no agent using this identity yet, exercise a
heartbeat, timeout, and recovery in terminal 2:

```powershell
Start-Sleep -Seconds 2
Invoke-RestMethod -Method Post -Uri "$base/workers/$($worker.id)/heartbeat" | Format-List
Start-Sleep -Seconds 16
Invoke-RestMethod "$base/workers" | Where-Object { $_.id -eq $worker.id } | Format-List
Invoke-RestMethod -Method Post -Uri "$base/workers/$($worker.id)/heartbeat" | Format-List
```

The first heartbeat advances `last_heartbeat_at`. After 16 seconds the list reports
`unavailable`; the next heartbeat reports `available`, with the same ID and registration time.

Start the agent in terminal 2, reusing that identity:

```powershell
.\.venv\Scripts\python.exe -m backend.worker.agent --identity manual-worker-1 --api-url http://127.0.0.1:8000 --heartbeat-interval 5
```

While the agent runs, press Ctrl+C in terminal 1 to stop the API. Leave it stopped for
at least 10 seconds; terminal 2 should show retry warnings without the worker exiting.
Restart the API in terminal 1 using the same command and database:

```powershell
.\.venv\Scripts\python.exe -m uvicorn backend.control_plane.main:app --host 127.0.0.1 --port 8000
```

Allow up to 35 seconds for the next retry and heartbeat. The API access log should
show successful heartbeat requests again. In terminal 2, stop the agent with Ctrl+C
and immediately inspect the record:

```powershell
Invoke-RestMethod "$base/workers" | Where-Object { $_.id -eq $worker.id } | Format-List
```

Expect the same ID, original registration time, a recent heartbeat, and `available`
if inspected within 15 seconds of the last heartbeat. Restart the agent with the same
identity to confirm process restarts also reuse the record. All API commands bind to
localhost; authentication and remote worker setup remain outside this milestone.
