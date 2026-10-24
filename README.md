# Mini Cloud

A lightweight, self-hosted application deployment and orchestration platform.
Milestone 1A implements FastAPI and SQLite persistence. Milestone 1B adds a separate
HTTP worker agent, worker registration, and heartbeats. Milestone 1C-A adds persistent
jobs, explicit basic scheduling, and opt-in simulated job processing. Milestone 1C-B
adds numbered attempts, leases, retries, and explicit failure recovery. Milestone 1D-A
adds explicit simulated instance records and lifecycle management. Docker,
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
before starting the upgraded version; old code may expect an older schema/protocol.
Do not delete or reset the database to upgrade. No migration dependency was added.
Future schema changes require another explicit versioned migration; `create_all` alone
does not upgrade tables.

Version 2 adds the jobs table, ownership/state constraints, a lifecycle transition
trigger, and a partial unique index allowing one assigned or claimed job per worker.
It backfills one queued job for each existing pending deployment without a job.
Non-pending deployments are not backfilled. Existing applications, deployments,
workers, and specifications remain unchanged. Versions 1 and 2 run in order for older
databases, in the same startup transaction. Migration tests cover rollback, repeated
startup, and concurrent startups. No dependency was added for Milestone 1C-A.

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
| POST | `/scheduling/run` | Recover expired work, then explicitly assign up to 100 ready jobs; optional `limit=1..100`. |
| GET | `/jobs` | List jobs; filters: `state`, `deployment_id`, `worker_id`; `limit=1..100`, `offset>=0`. |
| GET | `/jobs/{id}` | Return one durable job. |
| GET | `/workers/{id}/jobs` | Poll this worker's assigned jobs, including immutable specifications. |
| POST | `/workers/{id}/jobs/{job_id}/claim` | Claim using `attempt_number` and UUID `claim_id`; return a live lease grant. |
| POST | `/workers/{id}/jobs/{job_id}/report` | Report a live attempt; return its durable acknowledgment. |
| POST | `/workers/{id}/jobs/{job_id}/renew` | Renew a live lease using attempt, claim, and renewal UUID. |
| GET | `/jobs/{id}/attempts` | Ordered assignment/claim/outcome history. |
| POST | `/jobs/recover` | Recover up to 100 expired attempts without assigning new work. |
| POST | `/deployments/{id}/instances` | Create one instance and startup job atomically; 201 new, 200 existing. |
| GET | `/instances` | Inspect instances; filters `deployment_id`, `worker_id`, `state`; `limit` and `offset`. |
| GET | `/instances/{uuid}` | Instance state, placement provenance, and derived worker availability. |
| POST | `/instances/{uuid}/stop` | Version-checked simulated stop after startup work is terminal. |

Worker registration accepts `{"identity":"laptop-worker-1"}`. Identity is case-sensitive,
trimmed, and must be a string of 1–100 characters; extra fields return 422. A unique
database constraint and atomic upsert prevent duplicate records, including simultaneous
registrations. Re-registration preserves the worker ID and original registration time
while refreshing its heartbeat. Heartbeat requests need no body and accept positive IDs.
Clients do not supply timestamps or choose status. Registration optionally accepts
`capabilities`, an array containing `prepare_deployment` and/or `start_instance`.
Omission defaults to preparation-only for legacy compatibility. An empty array means
no new job kinds are eligible. Re-registration updates capabilities without revoking
an already-assigned or claimed job. Upgraded `--poll-jobs` agents advertise both kinds;
heartbeat-only operation retains its previous behavior.

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
  job_policy.py            Validated configurable lease/retry defaults
  api/routes.py            HTTP endpoints and HTTP error mapping
  api/schemas.py           Pydantic input/output contracts
  api/workers.py           Worker HTTP endpoints
  api/jobs.py              Job, claim/report, and explicit scheduling endpoints
  api/instances.py         Explicit instance creation, inspection, and stop
  services/applications.py Application and deployment operations
  services/workers.py      Registration, heartbeats, and availability calculation
  services/jobs.py         Durable claims, reports, queries, and write transactions
  services/scheduling.py   Capability-aware FIFO placement
  services/instances.py    Instance transactions and lifecycle helpers
  persistence/models.py    SQLAlchemy entities and database constraints
  persistence/database.py  SQLite engine and request-scoped sessions
  persistence/migrations.py Versioned, transactional SQLite startup upgrades
  persistence/schema_v3.py Frozen historical DDL for safe upgrades
backend/worker/
  agent.py                 Separate HTTP client process and retry loop
  jobs.py                  Opt-in HTTP polling and fixed simulation handler
tests/                     API validation and persistence tests
var/                       Ignored runtime data
```

For `POST /applications/{id}/deployments`, FastAPI validates the positive application ID
and JSON body with Pydantic. The route receives a request-scoped database session and calls
the service. The service checks that the application exists, copies the specification into
a SQLAlchemy entity, flushes it to obtain its ID, and creates a queued job. It commits
both records together and refreshes the deployment to obtain generated fields.
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

## Persistent jobs, attempts, and recovery

A deployment and its preparation job are created together. Explicit instance creation
adds a separate startup job; there is still exactly one preparation job per deployment. Each assignment creates a
numbered `JobAttempt` and consumes one of the job's persisted `max_attempts`. HTTP
retries consume no attempts. Jobs are current state; attempts preserve ownership,
claim UUID, failure reason, and outcomes. Terminal attempts are immutable. A partial
unique index still permits exactly one assigned/claimed job per worker.

The lifecycle is `queued -> assigned -> claimed -> completed|failed`. Assignment
expiration, execution lease expiration, or an explicitly retryable failure can instead
return a job to queued while attempts remain. The closed attempt remains failed or
expired in history. At the limit the job becomes failed with `attempts_exhausted`.
Permanent failures become terminal immediately. Neither success nor failure starts an
application or changes deployment runtime status; new deployments remain pending.

All ownership mutations use short `BEGIN IMMEDIATE` transactions, sample UTC after
acquiring the write reservation, and use conditional writes. Scheduling recovers a
bounded batch first, then selects eligible queued jobs by `(next_run_at, created_at, id)`
and fresh idle compatible workers by ID, all inside the same transaction. Unsupported
job kinds do not block compatible work later in the queue; queued jobs are scanned in
bounded pages until the assignment limit is reached or the queue is exhausted. One call may leave more
than 100 expirations for a subsequent call. SQLite serializes writers; lock contention
can still fail a request, which workers retry as a server error.

There is no FastAPI background scheduler. Invoke `/jobs/recover` or `/scheduling/run`
explicitly. An expired lease is rejected by mutation endpoints immediately, even before
recovery updates stored state. Polling remains read-only and omits expired assignments.
Recovery releases worker capacity, closes history, and sets a retry eligibility time;
it does not assign work until a scheduling operation finds it ready.

### Defaults and configuration

Set these before starting the API. All processes must use the same policy.

| Environment variable | Default |
| --- | --- |
| `MINI_CLOUD_JOB_ASSIGNMENT_SECONDS` | 15 |
| `MINI_CLOUD_JOB_LEASE_SECONDS` | 30 |
| `MINI_CLOUD_JOB_RENEWAL_SECONDS` | 10 |
| `MINI_CLOUD_JOB_MAX_ATTEMPTS` | 3 total assignments, including the first |
| `MINI_CLOUD_JOB_RETRY_SECONDS` | 5 |
| `MINI_CLOUD_JOB_RETRY_CAP_SECONDS` | 60 |

Durations must be finite and positive; renewal must be shorter than the lease. The
retry cap must be at least the initial delay. Attempts must be a positive integer.
The attempt limit is persisted per job, so changing configuration does not reset
existing retry budgets. Retry delays are 5, 10, 20, 40, 60, 60... seconds when the
configured budget allows that many attempts. Initial jobs are immediately eligible.

An assignment has 15 seconds to be claimed. A claim starts a 30-second lease. A lease
or assignment is expired at **now >= deadline**. This differs intentionally from the
worker heartbeat boundary: heartbeat age exactly 15 seconds is still available.

Heartbeats control eligibility for new assignments and first claims. They do not
renew leases or revoke unexpired leases. A live owner may renew or report even with
an expired heartbeat. A fresh heartbeat cannot revive expired ownership.

### Protocol and idempotency

Polling responses include `attempt_number` and `assignment_expires_at`. Old job agents
must upgrade: missing attempt numbers are rejected with 422, never silently mapped
to the latest attempt. Heartbeat-only worker operation is unchanged.

Claim body:

```json
{"attempt_number":1,"claim_id":"<UUID>"}
```

The same worker/attempt/claim can repeat a claim while it is live, without extending
the deadline. A stale generation, wrong worker/claim, expired assignment, or expired
lease returns 409. Claim retries after terminal completion are also rejected; durable
historical acknowledgment is specifically a report feature.

Renewal body:

```json
{"attempt_number":1,"claim_id":"<UUID>","renewal_id":"<UUID>"}
```

Renewal extends to the later of the current deadline and `server_now + lease_duration`.
Reuse the same renewal UUID after an uncertain response. A small `job_renewals` table
stores receipts so even a delayed replay of an older renewal cannot extend or shorten
the current lease. This is an intentional extension of the original latest-receipt
proposal. Receipts remain subject to live-current-owner checks; an expired or replaced
attempt cannot renew. History/renewal receipts currently have no retention policy.

Claim and renewal responses contain the grant deadline, current `server_time`, and
`renewal_interval`. A replay grants the same deadline, while `server_time` reflects
this request. The worker uses the time remaining, not a fresh full lease duration.

Report bodies contain attempt and claim IDs, `outcome`, optional `result` or `error`,
and `retryable` (false by default). Only failed outcomes can be retryable. A failed
report requires a nonblank error (up to 2000 characters) and cannot contain a result.
A completed report can contain a JSON-object result of up to 16 KiB but no error.

An accepted report stores its payload and historical acknowledgment atomically.
Identical retries return that acknowledgment even after expiration, reassignment,
completion of a newer attempt, or an API restart. Contradictory reports return 409.
Acknowledgments contain `acknowledged_attempt_number` and `attempt_outcome`, and their
job fields describe state **at acceptance time**. Use `GET /jobs/{id}` for current
state; replaying an old acknowledgment never changes a newer attempt.

Errors use `detail.code` and `detail.message`: examples include `stale_attempt`,
`assignment_expired`, `lease_expired`, `worker_unavailable`, and `report_conflict`.
Unknown records return 404; invalid bodies return 422; ownership/state conflicts 409.
Attempt history exposes `assignment_expired`, `lease_expired`, `retryable_failure`, or
`permanent_failure`, including original errors. No claim UUID is an authentication
credential. Development remains on localhost without remote worker support.

### Migration version 3

Startup rebuilds the v2 jobs table transactionally, adds attempt history and renewal
receipts, and recreates indexes and triggers. Applications, deployments, workers,
job IDs, original ownership/timestamps, specifications, and terminal outcomes remain
intact. Existing queued jobs start with zero attempts. Assigned, claimed, completed,
and failed jobs get attempt 1. Active assignments get a fresh 15-second grace window;
active claims get a fresh 30-second lease, using one migration timestamp. Terminal
reports get historical receipts with retryable=false.

Repeated startup neither recreates history nor extends migration grace windows.
Any failure rolls back data, DDL, and the schema version; later startup can retry.
Stop old API/worker processes before upgrading. Old migrations now use a frozen v2
jobs schema rather than today's evolving model. No dependency was added. Tests use
only temporary files; your development database is upgraded only when you start the API.
Do not delete or reset it.

### Worker operation and limitations

```powershell
.\.venv\Scripts\python.exe -m backend.worker.agent --identity laptop-worker-1 --heartbeat-interval 5 --poll-jobs
```

The opt-in agent generates fresh claim/renewal UUIDs and retains them for ambiguous
network retries within its process. It reports only the fixed `{"simulated":true}`
result and never interprets the specification. Polling, heartbeats, and renewal have
independent monotonic deadlines/backoffs; renewal runs before further job processing.
A conservative local expiry uses the time immediately before the HTTP request plus
the server's remaining lease, minus a safety margin. Slow responses therefore cannot
give the worker a fresh full lease. Expiry or an ownership conflict discards local
simulation state. Heartbeats continue even when work is abandoned.

Synchronous HTTP requests remain timeout-bounded and can delay other operations.
Claim state is not saved across worker process restarts: a new process does not adopt
an old claim; it expires and is recovered. API downtime longer than a lease can cause
work to be abandoned even if an unconfirmed renewal succeeded. This is conservative.

Leases provide at-least-once attempts, not exactly-once execution. API fencing prevents
stale database writes; it cannot stop a paused old process, undo external side effects,
or prevent overlapping external execution after reassignment. Real execution will
need idempotent resource operations and enforcement beyond this API. Wall-clock jumps
on the control plane can change deadline behavior. Explicit recovery/scheduling calls
are still required for progress. No Docker, broker, external execution, or background
API scheduler is introduced.

## Manual lease fault injection in two PowerShell terminals

Stop older API/agents. Terminal 1 starts the API using a fresh ignored test database
so existing queues cannot interfere. This does not reset your development database:

```powershell
Set-Location C:\Projects\mini-cloud-1
$env:MINI_CLOUD_DATABASE_PATH = Join-Path $PWD ("var\manual-1cb-{0}.db" -f [guid]::NewGuid().ToString("N"))
$env:MINI_CLOUD_WORKER_TIMEOUT_SECONDS = "15"
$env:MINI_CLOUD_JOB_ASSIGNMENT_SECONDS = "15"
$env:MINI_CLOUD_JOB_LEASE_SECONDS = "30"
$env:MINI_CLOUD_JOB_RENEWAL_SECONDS = "10"
$env:MINI_CLOUD_JOB_MAX_ATTEMPTS = "3"
$env:MINI_CLOUD_JOB_RETRY_SECONDS = "5"
$env:MINI_CLOUD_JOB_RETRY_CAP_SECONDS = "60"
.\.venv\Scripts\python.exe -m uvicorn backend.control_plane.main:app --host 127.0.0.1 --port 8000
```

In terminal 2 define helpers, create a job and worker, and expire its first assignment:

```powershell
Set-Location C:\Projects\mini-cloud-1
$base = "http://127.0.0.1:8000"
function Post-Json($path, $body = $null) {
    $options = @{ Method = 'Post'; Uri = "$base$path" }
    if ($null -ne $body) {
        $options.ContentType = 'application/json'
        $options.Body = $body | ConvertTo-Json -Depth 10
    }
    Invoke-RestMethod @options
}
$app = Post-Json '/applications' @{ name = 'lease-demo' }
function New-DemoJob {
    $dep = Post-Json "/applications/$($app.id)/deployments" @{ specification = @{ simulated = $true } }
    @(Invoke-RestMethod "$base/jobs?deployment_id=$($dep.id)")[0]
}
$job = New-DemoJob
$oldWorker = Post-Json '/workers/register' @{ identity = 'old-worker' }
Post-Json '/scheduling/run' | ConvertTo-Json -Depth 5
Start-Sleep -Seconds 16
Post-Json '/jobs/recover' | ConvertTo-Json -Depth 5
Invoke-RestMethod "$base/jobs/$($job.id)/attempts" | Format-Table
```

Attempt 1 is expired and the job is queued with a 5-second delay. Assign attempt 2 to
that same worker, claim it, then simulate its loss by sending no heartbeats or renewals:

```powershell
Start-Sleep -Seconds 5
Post-Json "/workers/$($oldWorker.id)/heartbeat" | Out-Null
Post-Json '/scheduling/run' | ConvertTo-Json -Depth 5
$oldClaim = @{ attempt_number = 2; claim_id = [guid]::NewGuid().ToString() }
Post-Json "/workers/$($oldWorker.id)/heartbeat" | Out-Null
Post-Json "/workers/$($oldWorker.id)/jobs/$($job.id)/claim" $oldClaim | Format-List
Start-Sleep -Seconds 31
Post-Json '/jobs/recover' | ConvertTo-Json -Depth 5
```

Attempt 2 is expired; the next delay is 10 seconds. Register another worker after that
delay; the first worker is stale, so attempt 3 will go to the new worker:

```powershell
Start-Sleep -Seconds 10
$newWorker = Post-Json '/workers/register' @{ identity = 'new-worker' }
Post-Json '/scheduling/run' | ConvertTo-Json -Depth 5
# Expected 409: the old owner cannot complete the new attempt.
try {
    Post-Json "/workers/$($oldWorker.id)/jobs/$($job.id)/report" (@{} + $oldClaim + @{ outcome = 'completed'; result = @{ simulated = $true } })
} catch {
    $_.Exception.Response.StatusCode
    $_.ErrorDetails.Message
}
$newClaim = @{ attempt_number = 3; claim_id = [guid]::NewGuid().ToString() }
Post-Json "/workers/$($newWorker.id)/heartbeat" | Out-Null
Post-Json "/workers/$($newWorker.id)/jobs/$($job.id)/claim" $newClaim | Format-List
Post-Json "/workers/$($newWorker.id)/jobs/$($job.id)/report" (@{} + $newClaim + @{ outcome = 'completed'; result = @{ simulated = $true } }) | Format-List
Invoke-RestMethod "$base/jobs/$($job.id)/attempts" | Format-Table
Invoke-RestMethod "$base/deployments" | Format-Table id, runtime_status
```

Expect expired attempts 1 and 2, completed attempt 3, and deployment runtime `pending`.
If typing takes more than 15 seconds between scheduling and claiming, the assignment
will expire; run each scheduling/claim sequence promptly.

Create another job and exhaust its three assignments without claiming any of them:

```powershell
$exhausted = New-DemoJob
foreach ($number in 1..3) {
    Post-Json "/workers/$($newWorker.id)/heartbeat" | Out-Null
    Post-Json '/scheduling/run' | ConvertTo-Json -Depth 5
    Start-Sleep -Seconds 16
    Post-Json '/jobs/recover' | ConvertTo-Json -Depth 5
    if ($number -lt 3) { Start-Sleep -Seconds (5 * [math]::Pow(2, $number - 1)) }
}
Invoke-RestMethod "$base/jobs/$($exhausted.id)" | Format-List
Invoke-RestMethod "$base/jobs/$($exhausted.id)/attempts" | Format-Table
```

Expect `state=failed`, `attempt_count=3`, and `last_failure_code=attempts_exhausted`.
The fixed simulated worker completes too quickly for an interactive kill reliably;
manual claim requests above provide deterministic fault injection without adding a
fake long-running application executor.

Run all automated checks:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Tests cover exact deadlines, heartbeat/lease independence, renewable ownership,
out-of-order renewal retries, bounded delays and exhaustion, stale generations,
concurrent scheduler/recovery/renewal/report races, historical receipts across restarts,
transaction rollback, v2 upgrades with grace windows and terminal receipts, process
restarts, API outages, and lease loss in the HTTP-only agent. No real sleeps or network
services are required by these tests.

The final engineering review adds a complete HTTP two-worker recovery regression:
the first claim expires, recovery releases it, the second worker claims the retry,
the stale owner gets 409, and the second completes with both attempts preserved and
the deployment still pending. A separate regression verifies that a new claim resets
renewal backoff accumulated by a previous attempt.

## Instance records and lifecycle management (1D-A)

Instances are explicit and simulated. Deployment creation and existing
`prepare_deployment` jobs retain their previous behavior, including reports without
instance fields. No existing job is repurposed. An instance is not inferred from a
completed preparation job, and no instance is created automatically at startup.

`POST /deployments/{id}/instances` accepts an empty body or `{}`. It creates a stable
UUID instance and one `start_instance` job together under `BEGIN IMMEDIATE`. SQLite
uniquely constrains the deployment and startup-job relationships. Concurrent requests
produce one record/job. Repeated creation returns the existing instance without
resetting state, ownership, or retry budgets, including after failure or stop.

The instance stores deployment/startup-job IDs, most recent worker and attempt IDs,
state, version, creation/update timestamps, last worker observation, healthy/stop
timestamps, source, and reason. All times use aware UTC at the Python/API boundary.
The worker ID is placement provenance, not proof of an existing application process.

Allowed transitions:

| Transition | Authority |
| --- | --- |
| pending -> starting | Control plane accepts a live first startup claim. |
| pending -> failed | Unclaimed startup assignments exhaust the retry budget. |
| starting -> healthy | Current lease owner reports successful simulated readiness. |
| starting -> failed | Startup/readiness failure or control-plane lease recovery. |
| failed -> starting | A valid retry claim, keeping the same UUID. |
| healthy/failed -> stopped | Explicit manual simulated stop after startup work is terminal. |

A successful claim sets starting, not healthy. The worker executes a fixed startup
simulation and a separate fixed readiness phase, then reports. Specifications are
never interpreted as commands. There are no processes, containers, real probes,
sleeps to simulate applications, replicas, or replacement instances.

Startup polling includes `instance_id` and `instance_version`; the claim response
includes the updated `instance` snapshot. A startup report must include:

```json
{
  "attempt_number": 1,
  "claim_id": "<UUID>",
  "outcome": "completed",
  "result": {"simulated": true},
  "instance_outcome": {
    "instance_id": "<instance UUID>",
    "expected_version": 3,
    "state": "healthy",
    "phase": "readiness_passed"
  }
}
```

Use the version from the claim response, not the earlier polling response. A failed
startup report instead uses outcome/state failed, a bounded error, and phase
`startup_failed` or `readiness_failed`; existing retryable failure policy applies.
Preparation jobs cannot report instance outcomes. Job/instance outcome mismatches
and missing required startup observations return 422. Expired claims, wrong workers,
stale attempts, instance mismatches, and stale versions return 409.

Within one write transaction the control plane validates job/attempt/worker/claim,
lease, instance linkage and version, updates the instance, closes the job and attempt,
and stores the historical acknowledgment. Failure rolls back all of them. Replayed
accepted reports return the original job and instance snapshots without changing
current state, even after stop. The lease is not an ongoing health-reporting credential
once the startup job completes.

Inspection includes persisted state plus `simulated`, `worker_available`,
`startup_job_state`, `current_worker_id`, `current_attempt_number`, and `ownership_valid`.
Derived availability is not persisted. A healthy instance on an unavailable worker
stays last-known healthy; its observation timestamp and worker reachability explain
the uncertainty. Expiration recovery records startup ownership failure, not proof a
process terminated. Assignment expiry leaves pending/previously failed state until
retry exhaustion. Deployment runtime status is never changed by instance operations.

`POST /instances/{uuid}/stop` requires `{"expected_version":<integer>}`. It accepts
healthy/failed instances only after the startup job is completed or terminally failed.
Pending, starting, and queued-retry startup operations return 409. An already-stopped
instance returns its unchanged record for an idempotent retry. No active job is
cancelled. Stopped is terminal in 1D-A; creation does not restart it.

### Migration version 4

The current jobs schema permits `start_instance` and replaces global deployment-job
uniqueness with a partial unique index for preparation jobs. Version 4 stages jobs,
attempts, and renewal receipts in transaction-local SQLite tables, drops children
before their parent, then recreates/restores the parent before children. Original
IDs and raw serialized report acknowledgments are preserved. Foreign keys remain
enabled. The new instance table is created only after restoration, and foreign-key
integrity is checked before recording version 4.

Workers receive a preparation-only capabilities default. Historical version 3 DDL
is frozen in `schema_v3.py`, so older migrations do not silently use the new model.
Instances are not backfilled and existing live deadlines are not extended. Repeated
startup is a no-op. Injected failures at child removal, parent restoration, and version
recording verify complete rollback, including original triggers and indexes.

Tests cover versions 1/2/3, active assignments/claims, terminal outcomes and accepted
reports, renewal receipts, concurrent upgrades, repeat startup, and foreign-key
integrity. Tests use temporary files. Your existing development database is upgraded
only when you start the new API; it is never deleted or reset. Stop old API/agents
before upgrading. No new dependency was introduced.

### Quick manual instance lifecycle in two PowerShell terminals

Terminal 1 starts a fresh ignored manual-test database, preserving development data:

```powershell
Set-Location C:\Projects\mini-cloud-1
$env:MINI_CLOUD_DATABASE_PATH = Join-Path $PWD ("var\manual-1da-{0}.db" -f [guid]::NewGuid().ToString("N"))
.\.venv\Scripts\python.exe -m uvicorn backend.control_plane.main:app --host 127.0.0.1 --port 8000
```

Terminal 2 creates an instance and uses a startup-only worker identity so queued
preparation work does not consume its first assignment:

```powershell
Set-Location C:\Projects\mini-cloud-1
$base = "http://127.0.0.1:8000"
function Post-Json($path, $body = $null) {
    $options = @{ Method = 'Post'; Uri = "$base$path" }
    if ($null -ne $body) {
        $options.ContentType = 'application/json'
        $options.Body = $body | ConvertTo-Json -Depth 10
    }
    Invoke-RestMethod @options
}
$app = Post-Json '/applications' @{ name = 'instance-demo' }
$dep = Post-Json "/applications/$($app.id)/deployments" @{ specification = @{ simulated = $true } }
$instance = Post-Json "/deployments/$($dep.id)/instances"
Post-Json "/deployments/$($dep.id)/instances" | Format-List
$worker = Post-Json '/workers/register' @{ identity = 'instance-demo'; capabilities = @('start_instance') }
Post-Json '/scheduling/run' | ConvertTo-Json -Depth 5
Invoke-RestMethod "$base/workers/$($worker.id)/jobs" | ConvertTo-Json -Depth 5
$claim = @{ attempt_number = 1; claim_id = [guid]::NewGuid().ToString() }
Post-Json "/workers/$($worker.id)/heartbeat" | Out-Null
$grant = Post-Json "/workers/$($worker.id)/jobs/$($instance.start_job_id)/claim" $claim
$grant.instance | Format-List
$report = @{} + $claim + @{
    outcome = 'completed'
    result = @{ simulated = $true }
    instance_outcome = @{
        instance_id = $instance.id
        expected_version = $grant.instance.version
        state = 'healthy'
        phase = 'readiness_passed'
    }
}
$accepted = Post-Json "/workers/$($worker.id)/jobs/$($instance.start_job_id)/report" $report
$accepted.instance | Format-List
$stopped = Post-Json "/instances/$($instance.id)/stop" @{ expected_version = $accepted.instance.version }
$stopped | Format-List
# Historical acknowledgment still describes healthy; it does not undo the stop.
Post-Json "/workers/$($worker.id)/jobs/$($instance.start_job_id)/report" $report | ConvertTo-Json -Depth 8
Invoke-RestMethod "$base/instances/$($instance.id)" | Format-List
Invoke-RestMethod "$base/deployments" | Format-Table id, runtime_status
```

Run scheduling and claiming promptly (15-second assignment window), then report within
the 30-second lease or renew it using the existing renewal endpoint. To exercise the
Python handler instead of manual claims, create another deployment and instance,
run an upgraded worker with `--poll-jobs`, and explicitly invoke scheduling. That worker
advertises both kinds; preparation may run first and another explicit scheduling call
may be needed for startup. Scheduling never runs inside the worker or a FastAPI loop.

### Boundaries and remaining limitations

One instance per deployment, one active job per worker, and one startup job per instance
are intentional first-slice limits. Healthy instances do not consume active job slots;
this is not runtime resource accounting. There is no continuous health monitor,
active-start cancellation, restart of stopped instances, desired replica count,
reconciliation loop, or automatic replacement. Those policies need the 1D-B design.

Only localhost development is supported; capabilities and claim UUIDs are not worker
authentication. API fencing cannot prevent stale external side effects once real
execution exists. Existing synchronous HTTP timing, explicit recovery/scheduling,
process-local claims, clock-jump, and history-retention limitations still apply.
