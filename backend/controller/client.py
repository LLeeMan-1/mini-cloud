"""Loopback HTTP transport and defensive wire validation; no server imports."""

import ipaddress
import math
import re
from urllib.parse import urlsplit
from uuid import UUID

import httpx


class ControllerError(Exception):
    def __init__(self, operation, code, status=None, retry_after=0):
        super().__init__(code)
        self.operation, self.code, self.status, self.retry_after = operation, code, status, retry_after


class AvailabilityError(ControllerError):
    pass


class DeploymentError(ControllerError):
    pass


class ProtocolError(ControllerError):
    pass


class FatalError(ControllerError):
    pass


def validate_api_url(value):
    try:
        url = urlsplit(value)
        host = url.hostname
        local = host == "localhost" or (host is not None and ipaddress.ip_address(host).is_loopback)
        if (url.scheme != "http" or not local or url.username is not None or url.password is not None
                or url.query or url.fragment or "?" in value or "#" in value
                or url.path not in ("", "/") or url.port == 0):
            raise ValueError
    except ValueError:
        raise ValueError("API URL must be HTTP loopback without credentials, path, query, or fragment") from None
    return value.rstrip("/")


def open_client(api_url, timeout):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Request timeout must be finite and positive")
    return httpx.Client(base_url=validate_api_url(api_url), timeout=httpx.Timeout(timeout),
                        follow_redirects=False, trust_env=False)


def valid_int(value, minimum=0, maximum=None):
    return type(value) is int and value >= minimum and (maximum is None or value <= maximum)


def valid_uuid(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except ValueError:
        return False


def reason_code(value):
    return isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", value) is not None


class ControllerClient:
    def __init__(self, http):
        self.http = http

    def request(self, operation, method, path, *, params=None, deployment=False):
        try:
            response = self.http.request(method, path, params=params)
        except httpx.HTTPError:
            # Exception strings and response bodies may contain sensitive data.
            raise AvailabilityError(operation, "transport_error") from None
        status = response.status_code
        if status != 200:
            code = "http_error"
            try:
                detail = response.json().get("detail")
                if isinstance(detail, dict) and reason_code(detail.get("code")):
                    code = detail["code"]
            except (ValueError, AttributeError):
                pass
            retry_after = 0
            try:
                seconds = float(response.headers.get("Retry-After", "0"))
                if math.isfinite(seconds):
                    retry_after = min(30, max(0, seconds))
            except ValueError:
                pass
            if status in (408, 429, 503):
                raise AvailabilityError(operation, code, status, retry_after)
            if deployment and (status == 409 or (status == 404 and code == "not_found") or status >= 500):
                raise DeploymentError(operation, code, status)
            if status >= 500:
                raise AvailabilityError(operation, code, status, retry_after)
            raise FatalError(operation, "incompatible_api", status)
        try:
            data = response.json()
        except ValueError:
            raise ProtocolError(operation, "invalid_json", status) from None
        if not isinstance(data, dict):
            raise ProtocolError(operation, "invalid_response", status)
        return data

    @staticmethod
    def require(condition, operation):
        if not condition:
            raise ProtocolError(operation, "invalid_response", 200)

    def recover(self):
        data = self.request("recover", "POST", "/jobs/recover", params={"limit": 100})
        rows = data.get("recovered")
        self.require(isinstance(rows, list) and valid_int(data.get("recovered_count"), maximum=100), "recover")
        self.require(data["recovered_count"] == len(rows), "recover")
        ids = []
        for row in rows:
            self.require(isinstance(row, dict) and valid_int(row.get("job_id"), 1)
                         and valid_int(row.get("attempt_number"), 1) and row.get("state") in ("queued", "failed")
                         and row.get("failure_code") in ("assignment_expired", "lease_expired"), "recover")
            ids.append(row["job_id"])
        self.require(len(ids) == len(set(ids)), "recover")
        return data

    def page(self, limit, after_id, through_id=None):
        params = {"limit": limit, "after_id": after_id}
        if through_id is not None:
            params["through_id"] = through_id
        data = self.request("discover", "GET", "/deployments/page", params=params)
        rows, boundary, cursor = data.get("items"), data.get("snapshot_max_id"), data.get("next_after_id")
        self.require(isinstance(rows, list) and len(rows) <= limit and valid_int(boundary)
                     and boundary >= after_id and "next_after_id" in data, "discover")
        self.require(through_id is None or boundary == through_id, "discover")
        previous = after_id
        for row in rows:
            self.require(isinstance(row, dict) and valid_int(row.get("id"), 1)
                         and previous < row["id"] <= boundary, "discover")
            previous = row["id"]
        if cursor is None:
            self.require(previous == boundary, "discover")
        else:
            self.require(len(rows) == limit and valid_int(cursor, 1)
                         and after_id < cursor == previous < boundary, "discover")
        return data

    def reconcile(self, deployment_id):
        data = self.request("reconcile", "POST", f"/deployments/{deployment_id}/reconcile", deployment=True)
        self.require(data.get("deployment_id") == deployment_id
                     and valid_int(data.get("deployment_id"), 1), "reconcile")
        self.require(valid_int(data.get("target_version"), 1)
                     and valid_int(data.get("desired_replicas"), maximum=100), "reconcile")
        for count, key in (("created_count", "created"), ("replaced_count", "replaced")):
            self.require(valid_int(data.get(count), maximum=100) and isinstance(data.get(key), list), "reconcile")
            self.require(data[count] == len(data[key]), "reconcile")
        self.require(isinstance(data.get("blocked"), list), "reconcile")
        indexes = []
        for key in ("created", "replaced", "blocked"):
            for row in data[key]:
                self.require(isinstance(row, dict) and valid_int(row.get("replica_index"), maximum=99)
                             and row["replica_index"] < data["desired_replicas"]
                             and valid_uuid(row.get("instance_id")) and valid_int(row.get("start_job_id"), 1)
                             and valid_int(row.get("generation"), 1), "reconcile")
                if key == "blocked":
                    self.require(reason_code(row.get("reason")), "reconcile")
                if key == "replaced":
                    self.require(valid_uuid(row.get("previous_instance_id"))
                                 and valid_int(row.get("previous_generation"), 1)
                                 and row["generation"] == row["previous_generation"] + 1, "reconcile")
                indexes.append(row["replica_index"])
        self.require(len(indexes) == len(set(indexes)), "reconcile")
        return data

    def schedule(self):
        data = self.request("schedule", "POST", "/scheduling/run",
                            params={"limit": 100, "recover_expired": "false"})
        rows = data.get("assignments")
        self.require(valid_int(data.get("assigned_count"), maximum=100) and isinstance(rows, list), "schedule")
        self.require(data["assigned_count"] == len(rows), "schedule")
        jobs, workers = [], []
        for row in rows:
            self.require(isinstance(row, dict) and all(valid_int(row.get(key), 1)
                         for key in ("job_id", "worker_id", "attempt_number")), "schedule")
            jobs.append(row["job_id"])
            workers.append(row["worker_id"])
        self.require(len(jobs) == len(set(jobs)) and len(workers) == len(set(workers)), "schedule")
        return data
