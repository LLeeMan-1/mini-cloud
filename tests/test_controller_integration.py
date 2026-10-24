"""HTTP-only controller against real routes, SQLite, and simulated workers."""

from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
import json
import socket
import subprocess
import sys
from threading import Event, Thread
from uuid import uuid4

import httpx
import pytest
import uvicorn

from backend.controller.client import ControllerClient
from backend.controller.convergence import run_cycle
from backend.control_plane.main import create_app
from backend.control_plane.api.jobs import get_clock
from backend.control_plane.job_policy import JobPolicy
from backend.control_plane.persistence.models import utc_now
from backend.worker.jobs import JobPoller
from test_replicas import setup, deployment, worker, claim


@contextmanager
def bridge(client, after_response=None):
    def transport(request):
        target = request.url.path + ('?' + request.url.query.decode() if request.url.query else '')
        response = client.request(request.method, target,
                                  json=json.loads(request.content) if request.content else None)
        if after_response:
            after_response(request, response)
        return httpx.Response(response.status_code, content=response.content, headers=response.headers)
    with httpx.Client(base_url='http://127.0.0.1', transport=httpx.MockTransport(transport)) as http:
        yield http


def tick(client, **kwargs):
    with bridge(client, **kwargs) as http:
        return run_cycle(ControllerClient(http))


def instances(client, dep):
    return client.get('/instances', params={'deployment_id': dep, 'current': True}).json()


def complete_assigned(client, worker_id):
    poller = JobPoller(clock=lambda: 0)
    with bridge(client) as http:
        for _ in range(5):
            poller.step(http, worker_id)
    assert poller.pending is None


def test_empty_database_controller_cycle(client):
    result = tick(client)
    assert result.outcome == 'success' and result.deployments_seen == 0 and result.jobs_assigned == 0
    assert client.get('/instances').json() == []


def test_controller_and_workers_converge_without_manual_orchestration(setup):
    client = setup[0]
    dep = deployment(setup, 3)
    workers = [worker(client), worker(client)]
    result = tick(client)
    assert result.slots_created == 3 and result.jobs_assigned == 2
    assert len(instances(client, dep)) == 3
    for worker_id in workers:
        complete_assigned(client, worker_id)
    result = tick(client)
    assert result.slots_created == 0 and result.jobs_assigned == 1
    for worker_id in workers:
        if client.get(f'/workers/{worker_id}/jobs').json():
            complete_assigned(client, worker_id)
    assert all(instance['state'] == 'healthy' for instance in instances(client, dep))
    assert tick(client).jobs_assigned == 0
    assert client.get('/deployments').json()[0]['runtime_status'] == 'pending'


def test_deployment_created_during_scan_is_deferred_to_next_cycle(setup):
    client = setup[0]
    first, second = deployment(setup, 1), deployment(setup, 1)
    added = []
    def create_during_scan(request, response):
        if request.url.path == '/deployments/page' and not added:
            added.append(deployment(setup, 1))
    with bridge(client, create_during_scan) as http:
        result = run_cycle(ControllerClient(http), page_size=1)
    assert result.deployments_seen == 2 and result.slots_created == 2
    assert len(instances(client, first)) == len(instances(client, second)) == 1
    assert instances(client, added[0]) == []
    assert tick(client).slots_created == 1


def test_controller_recovery_then_retry_and_uncertain_execution_block(setup):
    client, now, _ = setup
    dep = deployment(setup, 1)
    worker_id = worker(client)
    tick(client)
    item = instances(client, dep)[0]
    for number in range(1, 4):
        claim(client, worker_id, item, number)
        now[0] += timedelta(seconds=30)
        result = tick(client)
        assert result.jobs_recovered == 1 and result.instances_replaced == 0
        if number < 3:
            now[0] += timedelta(seconds=5 * 2 ** (number - 1))
            client.post(f'/workers/{worker_id}/heartbeat')
            assert tick(client).jobs_assigned == 1
    now[0] += timedelta(seconds=31)
    result = tick(client)
    assert result.instances_replaced == 0 and result.blocked_replicas == 1
    assert instances(client, dep)[0]['replacement']['reason'] == 'uncertain_execution'
    assert len(client.get(f"/jobs/{item['start_job_id']}/attempts").json()) == 3


def test_controller_preserves_retry_cooldown_budget_and_historical_receipts(setup):
    client, now, _ = setup
    dep = deployment(setup, 1)
    worker_id = worker(client)
    tick(client)
    item = instances(client, dep)[0]
    reports = []
    for number in range(1, 4):
        prefix, payload = claim(client, worker_id, item, number)
        payload.update(outcome='failed', result=None, error='simulated readiness failure', retryable=True)
        payload['instance_outcome'].update(state='failed', phase='readiness_failed')
        response = client.post(prefix + '/report', json=payload)
        assert response.status_code == 200
        reports.append((prefix, payload, response.content))
        if number < 3:
            assert tick(client).jobs_assigned == 0
            now[0] += timedelta(seconds=5 * 2 ** (number - 1))
            client.post(f'/workers/{worker_id}/heartbeat')
            assert tick(client).jobs_assigned == 1
    assert tick(client).instances_replaced == 0
    now[0] += timedelta(seconds=29, microseconds=999999)
    assert tick(client).instances_replaced == 0
    now[0] += timedelta(microseconds=1)
    client.post(f'/workers/{worker_id}/heartbeat')
    result = tick(client)
    assert result.instances_replaced == 1 and result.jobs_assigned == 1
    replacement = instances(client, dep)[0]
    assert replacement['generation'] == 2 and replacement['id'] != item['id']
    assert tick(client).instances_replaced == 0
    complete_assigned(client, worker_id)
    now[0] += timedelta(seconds=16)
    assert tick(client).instances_replaced == 0
    assert instances(client, dep)[0]['state'] == 'healthy' and not instances(client, dep)[0]['worker_available']
    for prefix, payload, accepted in reports:
        assert client.post(prefix + '/report', json=payload).content == accepted
    observed = client.get(f'/deployments/{dep}/replicas').json()
    assert observed['slots'][0]['automatic_replacements'] == 1
    with client.app.state.engine.connect() as connection:
        assert connection.exec_driver_sql('PRAGMA foreign_key_check').all() == []


def test_controller_preserves_stopped_slots(setup):
    client = setup[0]
    dep = deployment(setup, 1)
    worker_id = worker(client)
    tick(client)
    complete_assigned(client, worker_id)
    healthy = instances(client, dep)[0]
    client.post(f"/instances/{healthy['id']}/stop", json={'expected_version': healthy['version']})
    result = tick(client)
    assert result.blocked_replicas == 1 and result.instances_replaced == 0
    assert instances(client, dep)[0]['state'] == 'stopped'


@pytest.mark.parametrize('stage', ['/jobs/recover', '/deployments/1/reconcile', '/scheduling/run'])
def test_restart_after_committed_mutation_response_is_lost(setup, stage):
    client, now, _ = setup
    dep = deployment(setup, 1)
    worker(client)
    if stage == '/jobs/recover':
        tick(client)
        now[0] += timedelta(seconds=15)
    lost = False
    def drop(request, response):
        nonlocal lost
        if request.url.path == stage and not lost:
            lost = True
            assert response.status_code == 200
            raise httpx.ReadTimeout('Response lost after commit', request=request)
    result = tick(client, after_response=drop)
    assert result.outcome == 'aborted' and lost
    result = tick(client)
    assert result.outcome == 'success'
    assert len(instances(client, dep)) == 1
    with client.app.state.engine.connect() as connection:
        assert connection.exec_driver_sql('SELECT COUNT(*) FROM jobs').scalar_one() == 2
        assert connection.exec_driver_sql('SELECT COUNT(*) FROM job_attempts').scalar_one() <= 1


def test_concurrent_controller_processes_against_disposable_http_api(tmp_path):
    # A real loopback server on an OS-assigned port; never use an existing API.
    now = [datetime.now(timezone.utc)]
    app = create_app(tmp_path / 'controller-processes.db', job_policy=JobPolicy(max_attempts=1))
    app.dependency_overrides[get_clock] = lambda: lambda: now[0]
    app.dependency_overrides[utc_now] = lambda: now[0]
    ready = Event()
    original = app.router.lifespan_context
    @asynccontextmanager
    async def lifespan(application):
        async with original(application):
            ready.set()
            yield
    app.router.lifespan_context = lifespan
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(128)
    base = f'http://127.0.0.1:{listener.getsockname()[1]}'
    server = uvicorn.Server(uvicorn.Config(app, log_level='critical', access_log=False))
    thread = Thread(target=server.run, kwargs={'sockets': [listener]}, daemon=True)
    processes = []
    thread.start()
    try:
        assert ready.wait(10), 'Disposable API did not start'
        with httpx.Client(base_url=base, timeout=10, trust_env=False) as http:
            application = http.post('/applications', json={'name': 'controllers'}).json()
            dep = http.post(f"/applications/{application['id']}/deployments", json={
                'specification': {'simulated': True}, 'desired_replicas': 3}).json()['id']
            for index in range(2):
                http.post('/workers/register', json={'identity': f'worker-{index}', 'capabilities': ['start_instance']}).raise_for_status()
            command = [sys.executable, '-B', '-m', 'backend.controller.agent', '--api-url', base, '--once', '--page-size', '1']
            for _ in range(2):
                processes.append(subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
            for process in processes:
                stdout, stderr = process.communicate(timeout=20)
                assert process.returncode == 0, (stdout, stderr)
            slots = http.get('/instances', params={'deployment_id': dep}).json()
            assert len(slots) == 3 and all(item['generation'] == 1 for item in slots)
            jobs = http.get('/jobs').json()
            assert len(jobs) == 4
            active = [job for job in jobs if job['state'] in ('assigned', 'claimed')]
            assert len(active) == 2 and len({job['worker_id'] for job in active}) == 2
            with app.state.engine.connect() as connection:
                assert connection.exec_driver_sql('SELECT COUNT(*) FROM job_attempts').scalar_one() == 2
                assert connection.exec_driver_sql('PRAGMA foreign_key_check').all() == []
            # Exhaust a reported startup failure, then race two fresh CLI
            # processes at replacement eligibility, without manual orchestration.
            job = active[0]
            prefix = f"/workers/{job['worker_id']}/jobs/{job['id']}"
            ownership = {'claim_id': str(uuid4()), 'attempt_number': 1}
            grant_response = http.post(prefix + '/claim', json=ownership)
            grant_response.raise_for_status()
            incarnation = grant_response.json()['instance']
            http.post(prefix + '/report', json={**ownership, 'outcome': 'failed', 'retryable': True,
                'error': 'simulated failure', 'instance_outcome': {'instance_id': incarnation['id'],
                    'expected_version': incarnation['version'], 'state': 'failed', 'phase': 'startup_failed'}}).raise_for_status()
            now[0] += timedelta(seconds=31)
            for entry in http.get('/workers').json():
                http.post(f"/workers/{entry['id']}/heartbeat").raise_for_status()
            replacements = []
            for _ in range(2):
                process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                processes.append(process)
                replacements.append(process)
            for process in replacements:
                stdout, stderr = process.communicate(timeout=20)
                assert process.returncode == 0, (stdout, stderr)
            history = http.get('/instances', params={'deployment_id': dep}).json()
            assert len(history) == 4 and sum(item['generation'] == 2 for item in history) == 1
            current = http.get('/instances', params={'deployment_id': dep, 'current': 'true'}).json()
            assert len(current) == 3
            target = http.get(f'/deployments/{dep}/replicas').json()
            assert sum(slot['automatic_replacements'] for slot in target['slots']) == 1
            with app.state.engine.connect() as connection:
                assert connection.exec_driver_sql('PRAGMA foreign_key_check').all() == []
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.communicate(timeout=10)
        server.should_exit = True
        thread.join(timeout=10)
        listener.close()
    assert not thread.is_alive()
