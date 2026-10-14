# Mini Cloud

A lightweight, self-hosted application deployment and orchestration platform.
Milestone 1A implements FastAPI and SQLite persistence. Workers, scheduling, Docker,
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
Tables are created at startup with `create_all`, which does not upgrade existing schemas.
Add explicit migrations before changing an existing database schema in a later milestone.

## API

| Method | Path | Behavior |
| --- | --- | --- |
| GET | `/health` | Liveness response: `{"status":"ok"}`. |
| POST | `/applications` | Create an application; returns 201. |
| GET | `/applications` | List applications in ID order. |
| POST | `/applications/{id}/deployments` | Create a pending deployment; returns 201, or 404 for an unknown application. |
| GET | `/deployments` | List deployments in ID order. |

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
  services/applications.py Application and deployment operations
  persistence/models.py    SQLAlchemy entities and database constraints
  persistence/database.py  SQLite engine and request-scoped sessions
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
