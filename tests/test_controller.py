"""Controller wire contracts, partial failure reporting, timing and CLI safety."""

import logging
import signal
import subprocess
import sys
from threading import Event

import httpx
import pytest

from backend.controller import agent
from backend.controller.client import ControllerClient, open_client, validate_api_url
from backend.controller.convergence import run_controller, run_cycle


def page(ids=(), boundary=None, cursor=None):
    return {"items": [{"id": value} for value in ids], "snapshot_max_id": max(ids, default=0) if boundary is None else boundary,
            "next_after_id": cursor}


def reconciled(deployment_id):
    return {"deployment_id": deployment_id, "target_version": 1, "desired_replicas": 0,
            "created_count": 0, "created": [], "replaced_count": 0, "replaced": [], "blocked": []}


def route_response(request, deployments=()):
    if request.url.path == "/jobs/recover":
        return {"recovered_count": 0, "recovered": []}
    if request.url.path == "/deployments/page":
        return page(deployments)
    if request.url.path == "/scheduling/run":
        return {"assigned_count": 0, "assignments": []}
    return reconciled(int(request.url.path.split('/')[2]))


def test_exact_order_bounded_requests_and_once():
    calls = []
    def transport(request):
        calls.append((request.method, request.url.path, dict(request.url.params)))
        if request.url.path == "/deployments/page":
            data = page([1], 2, 1) if request.url.params["after_id"] == "0" else page([2])
        else:
            data = route_response(request)
        return httpx.Response(200, json=data)
    with httpx.Client(base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)) as http:
        assert run_controller(ControllerClient(http), page_size=1, once=True) == 0
    assert [(method, path) for method, path, _ in calls] == [
        ("POST", "/jobs/recover"), ("GET", "/deployments/page"), ("POST", "/deployments/1/reconcile"),
        ("GET", "/deployments/page"), ("POST", "/deployments/2/reconcile"), ("POST", "/scheduling/run")]
    assert calls[0][2] == {"limit": "100"}
    assert calls[3][2] == {"limit": "1", "after_id": "1", "through_id": "2"}
    assert calls[-1][2] == {"limit": "100", "recover_expired": "false"}


@pytest.mark.parametrize("data", [[], {}, page([True]), page([0]), page([2, 1]), page([1, 1]),
    page([2], 1), page([1], 2), page([1], 2, 0), page([], 2, 1), page([1], 2, 2),
    {"items": [], "snapshot_max_id": 0}, page([1], True)])
def test_malformed_first_page_aborts_without_guessed_cursor_or_scheduling(data):
    paths = []
    def transport(request):
        paths.append(request.url.path)
        return httpx.Response(200, json=data if request.url.path == "/deployments/page" else route_response(request))
    with httpx.Client(base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)) as http:
        result = run_cycle(ControllerClient(http), page_size=2)
    assert result.outcome == "aborted" and result.global_failure
    assert paths == ["/jobs/recover", "/deployments/page"]


@pytest.mark.parametrize("second", [page([2], 3), page([1], 2), page([], 2), page([2], 2, 2)])
def test_changed_bounds_and_nonadvancing_continuations_abort(second):
    paths = []
    def transport(request):
        paths.append(request.url.path)
        if request.url.path == "/deployments/page":
            return httpx.Response(200, json=page([1], 2, 1) if request.url.params["after_id"] == "0" else second)
        return httpx.Response(200, json=route_response(request))
    with httpx.Client(base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)) as http:
        result = run_cycle(ControllerClient(http), page_size=1)
    assert result.outcome == "aborted" and result.deployments_reconciled == 1
    assert "/scheduling/run" not in paths


@pytest.mark.parametrize("status,code", [(409, "replica_integrity_conflict"), (404, "not_found"), (500, "server_error")])
def test_one_failed_deployment_isolated_and_later_work_scheduled(status, code):
    paths = []
    def transport(request):
        paths.append(request.url.path)
        if request.url.path == "/deployments/1/reconcile":
            return httpx.Response(status, json={"detail": {"code": code}})
        return httpx.Response(200, json=route_response(request, [1, 2]))
    with httpx.Client(base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)) as http:
        result = run_cycle(ControllerClient(http))
    assert result.outcome == "degraded" and result.deployments_failed == 1 and result.deployments_reconciled == 1
    assert paths[-1] == "/scheduling/run" and not result.global_failure


@pytest.mark.parametrize("operation", ["/jobs/recover", "/deployments/1/reconcile", "/scheduling/run"])
@pytest.mark.parametrize("failure", ["timeout", "503", "invalid_json"])
def test_uncertain_mutation_is_not_immediately_repeated(operation, failure):
    paths = []
    def transport(request):
        paths.append(request.url.path)
        if request.url.path == operation:
            if failure == "timeout":
                raise httpx.ReadTimeout("SECRET_RESPONSE", request=request)
            if failure == "503":
                return httpx.Response(503, json={"detail": {"code": "database_busy"}}, headers={"Retry-After": "999"})
            return httpx.Response(200, content=b'not JSON SECRET_RESPONSE')
        return httpx.Response(200, json=route_response(request, [1]))
    with httpx.Client(base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)) as http:
        result = run_cycle(ControllerClient(http))
    assert paths.count(operation) == 1
    assert result.outcome == ("degraded" if failure == "invalid_json" and "reconcile" in operation else "aborted")
    if failure == "503":
        assert result.retry_after == 30
    if "reconcile" in operation:
        assert result.deployments_failed == 1


@pytest.mark.parametrize("operation,data", [
    ("recover", {"recovered_count": True, "recovered": []}),
    ("recover", {"recovered_count": 1, "recovered": []}),
    ("recover", {"recovered_count": 1, "recovered": [None]}),
    ("schedule", {"assigned_count": 1, "assignments": []}),
    ("schedule", {"assigned_count": 1, "assignments": [{"job_id": 1, "worker_id": True, "attempt_number": 1}]}),
    ("schedule", {"assigned_count": 2, "assignments": [
        {"job_id": 1, "worker_id": 1, "attempt_number": 1}, {"job_id": 2, "worker_id": 1, "attempt_number": 1}]}),
    ("reconcile", {**reconciled(1), "created_count": 1}),
    ("reconcile", {**reconciled(1), "deployment_id": 2}),
])
def test_malformed_mutation_acknowledgments_do_not_produce_false_success(operation, data):
    paths = {"recover": "/jobs/recover", "schedule": "/scheduling/run", "reconcile": "/deployments/1/reconcile"}
    def transport(request):
        return httpx.Response(200, json=data if request.url.path == paths[operation] else route_response(request, [1, 2]))
    with httpx.Client(base_url='http://127.0.0.1', transport=httpx.MockTransport(transport)) as http:
        result = run_cycle(ControllerClient(http))
    assert result.outcome == ('degraded' if operation == 'reconcile' else 'aborted')
    assert result.slots_created == result.jobs_assigned == result.jobs_recovered == 0


@pytest.mark.parametrize("status", [301, 400, 401, 403, 404, 422])
def test_fatal_configuration_and_protocol_status(status):
    def transport(request):
        return httpx.Response(status, json={"detail": "SECRET_RESPONSE"})
    with httpx.Client(base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)) as http:
        result = run_cycle(ControllerClient(http))
        assert result.fatal and result.outcome == "aborted"
        assert run_controller(ControllerClient(http), once=True) == 1


def test_three_consecutive_server_errors_abort_cycle():
    paths = []
    def transport(request):
        paths.append(request.url.path)
        if request.url.path.endswith("/reconcile"):
            return httpx.Response(500, json={"detail": "error"})
        return httpx.Response(200, json=route_response(request, [1, 2, 3, 4]))
    with httpx.Client(base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)) as http:
        result = run_cycle(ControllerClient(http))
    assert result.outcome == "aborted" and result.deployments_failed == 3
    assert not any('/4/' in path or path == '/scheduling/run' for path in paths)


class ControlledStop:
    def __init__(self, waits=1):
        self.elapsed, self.waits, self.stop_after, self.stopped = 0.0, [], waits, False
    def is_set(self): return self.stopped
    def set(self): self.stopped = True
    def wait(self, seconds):
        self.waits.append(seconds)
        self.elapsed += seconds
        self.stopped = len(self.waits) >= self.stop_after
        return self.stopped


def test_slow_cycles_use_fixed_delay_and_do_not_catch_up():
    stop = ControlledStop(2)
    starts = []
    def transport(request):
        if request.url.path == "/jobs/recover": starts.append(stop.elapsed)
        stop.elapsed += 2
        return httpx.Response(200, json=route_response(request))
    with httpx.Client(base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)) as http:
        assert run_controller(ControllerClient(http), interval=5, stop_event=stop, clock=lambda: stop.elapsed) == 0
    assert starts == [0, 11] and stop.waits == [5, 5]


def test_outage_backoff_is_bounded_and_resets_after_recovery():
    stop = ControlledStop(9)
    calls = 0
    def transport(request):
        nonlocal calls
        if request.url.path == "/jobs/recover":
            calls += 1
            if calls <= 7 or calls == 9:
                raise httpx.ConnectError("do not log", request=request)
        return httpx.Response(200, json=route_response(request))
    with httpx.Client(base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)) as http:
        run_controller(ControllerClient(http), interval=0.5, stop_event=stop, clock=lambda: stop.elapsed)
    assert stop.waits == [1, 2, 4, 8, 16, 30, 30, 0.5, 1]


@pytest.mark.parametrize("stage", ["/jobs/recover", "/deployments/page", "/deployments/1/reconcile"])
def test_shutdown_after_inflight_request_starts_no_new_work(stage):
    stop, paths = Event(), []
    def transport(request):
        paths.append(request.url.path)
        if request.url.path == stage: stop.set()
        return httpx.Response(200, json=route_response(request, [1, 2]))
    with httpx.Client(base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)) as http:
        assert run_controller(ControllerClient(http), stop_event=stop) == 0
    assert paths[-1] == stage


def test_logs_have_summary_without_sensitive_payloads(caplog):
    caplog.set_level(logging.DEBUG)
    def transport(request):
        if request.url.path == "/deployments/1/reconcile":
            return httpx.Response(409, json={"detail": {"code": "bad_secret\nSECRET_RESPONSE", "message": "SECRET_RESPONSE"}})
        return httpx.Response(200, json=route_response(request, [1, 2]))
    with httpx.Client(base_url="http://127.0.0.1", transport=httpx.MockTransport(transport)) as http:
        run_cycle(ControllerClient(http))
    assert "SECRET_RESPONSE" not in caplog.text
    assert "event=cycle_end" in caplog.text and "outcome=degraded" in caplog.text


@pytest.mark.parametrize("url", ['http://example.com', 'http://192.168.1.1', 'https://127.0.0.1',
    'http://user:password@localhost', 'http://localhost?q=x', 'http://localhost#x', 'http://localhost?',
    'http://localhost/api', 'http://localhost:0', 'http://localhost:999999'])
def test_nonloopback_or_unsafe_urls_are_rejected(url):
    with pytest.raises(ValueError): validate_api_url(url)


@pytest.mark.parametrize("url", ['http://127.0.0.1:8000', 'http://localhost', 'http://[::1]:8000'])
def test_loopback_client_disables_redirects_and_proxies(url, monkeypatch):
    monkeypatch.setenv('HTTP_PROXY', 'http://proxy.invalid')
    with open_client(url, 2) as http:
        assert not http.follow_redirects and not http.trust_env and http.timeout.read == 2


def test_cli_once_closes_client_and_ctrl_c_stops_between_requests(monkeypatch):
    paths = []
    def transport(request):
        paths.append(request.url.path)
        signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
        return httpx.Response(200, json=route_response(request))
    http = httpx.Client(base_url='http://127.0.0.1', transport=httpx.MockTransport(transport))
    monkeypatch.setattr(agent, 'open_client', lambda *args: http)
    assert agent.main([]) == 0 and http.is_closed
    assert paths == ['/jobs/recover']


def test_cli_subprocess_help_and_argument_validation():
    for args, code in [(['--help'], 0), (['--interval', 'nan'], 2), (['--page-size', '101'], 2),
                       (['--api-url', 'http://example.com'], 2)]:
        response = subprocess.run([sys.executable, '-B', '-m', 'backend.controller.agent', *args], capture_output=True, timeout=10)
        assert response.returncode == code


@pytest.mark.parametrize('status,expected', [(409, 1), (503, 2)])
def test_once_reports_degraded_or_aborted_exit_codes(status, expected):
    def transport(request):
        if request.url.path == '/deployments/1/reconcile':
            return httpx.Response(status, json={'detail': {'code': 'database_busy' if status == 503 else 'replica_integrity_conflict'}})
        return httpx.Response(200, json=route_response(request, [1]))
    with httpx.Client(base_url='http://127.0.0.1', transport=httpx.MockTransport(transport)) as http:
        assert run_controller(ControllerClient(http), once=True) == expected
