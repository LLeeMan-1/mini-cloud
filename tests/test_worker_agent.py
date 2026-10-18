import argparse

import httpx
import pytest

from backend.worker.agent import positive_seconds, run_agent


class StopAfterWaits:
    """Deterministic substitute for Event; records waits without sleeping."""

    def __init__(self, count):
        self.count = count
        self.waits = []

    def is_set(self):
        return len(self.waits) >= self.count

    def wait(self, delay):
        self.waits.append(delay)
        return self.is_set()


def test_registration_then_configurable_heartbeats():
    paths = []

    def handler(request):
        paths.append(request.url.path)
        if request.url.path == "/workers/register":
            assert request.content == b'{"identity":"local-1"}'
        return httpx.Response(200, json={"id": 7})

    stop = StopAfterWaits(3)
    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
        run_agent(client, "local-1", interval=2, stop_event=stop)
    assert paths == ["/workers/register", "/workers/7/heartbeat", "/workers/7/heartbeat"]
    assert stop.waits == [2, 2, 2]


def test_network_failures_and_server_errors_back_off_then_recover():
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if len(calls) == 1 or len(calls) == 5:
            raise httpx.ConnectError("API offline", request=request)
        if len(calls) == 2:
            return httpx.Response(503)
        return httpx.Response(200, json={"id": 3})

    stop = StopAfterWaits(6)
    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
        run_agent(client, "local", stop_event=stop)
    assert stop.waits == [1, 2, 5, 5, 1, 5]
    assert calls[:3] == ["/workers/register"] * 3
    assert calls[3:] == ["/workers/3/heartbeat"] * 3


def test_backoff_is_capped_and_timeout_is_retried():
    def handler(request):
        raise httpx.ReadTimeout("No response", request=request)

    stop = StopAfterWaits(8)
    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
        run_agent(client, "local", stop_event=stop)
    assert stop.waits == [1, 2, 4, 8, 16, 30, 30, 30]


def test_unknown_worker_reregisters():
    paths = []

    def handler(request):
        paths.append(request.url.path)
        if len(paths) == 2:
            return httpx.Response(404)
        return httpx.Response(200, json={"id": 1 if len(paths) == 1 else 2})

    stop = StopAfterWaits(3)
    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)) as client:
        run_agent(client, "local", stop_event=stop)
    assert paths == ["/workers/register", "/workers/1/heartbeat", "/workers/register", "/workers/2/heartbeat"]


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_transient_http_status_is_retried(status):
    stop = StopAfterWaits(2)
    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(
        lambda _: httpx.Response(status)
    )) as client:
        run_agent(client, "local", stop_event=stop)
    assert stop.waits == [1, 2]


def test_permanent_rejection_is_not_retried():
    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(
        lambda _: httpx.Response(422)
    )) as client:
        with pytest.raises(httpx.HTTPStatusError):
            run_agent(client, "local", stop_event=StopAfterWaits(1))


def test_malformed_registration_response_is_retried():
    stop = StopAfterWaits(2)
    with httpx.Client(base_url="http://test", transport=httpx.MockTransport(
        lambda _: httpx.Response(200, json={"id": "bad"})
    )) as client:
        run_agent(client, "local", stop_event=stop)
    assert stop.waits == [1, 2]


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "bad"])
def test_invalid_cli_intervals(value):
    with pytest.raises(argparse.ArgumentTypeError):
        positive_seconds(value)
