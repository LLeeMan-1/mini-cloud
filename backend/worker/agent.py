"""Registration and heartbeat client; no database or control-plane imports."""

import argparse
import logging
import math
from threading import Event

import httpx


logger = logging.getLogger(__name__)


def positive_seconds(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("Must be a finite positive number") from None
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("Must be a finite positive number")
    return seconds


def run_agent(
    client: httpx.Client, identity: str, interval: float = 5, *, stop_event: Event | None = None,
    poll_jobs: bool = False,
) -> None:
    if not identity.strip() or len(identity.strip()) > 100:
        raise ValueError("Identity must have 1–100 characters")
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError("Heartbeat interval must be finite and positive")
    stop = stop_event if stop_event is not None else Event()
    if poll_jobs:
        from backend.worker.jobs import run_job_agent
        run_job_agent(client, identity, interval, stop)
        return
    worker_id = None
    retry_delay = 1.0
    while not stop.is_set():
        try:
            if worker_id is None:
                response = client.post("/workers/register", json={"identity": identity.strip()})
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, dict) or type(data.get("id")) is not int or data["id"] <= 0:
                    raise ValueError("Registration response must contain a positive integer ID")
                worker_id = data["id"]
                logger.info("Registered worker %s as ID %s", identity, worker_id)
            else:
                response = client.post(f"/workers/{worker_id}/heartbeat")
                if response.status_code == 404:
                    worker_id = None
                    continue
                response.raise_for_status()
                logger.debug("Heartbeat accepted for worker %s", worker_id)
        except (httpx.HTTPError, ValueError) as error:
            if isinstance(error, httpx.HTTPStatusError):
                status = error.response.status_code
                if status < 500 and status not in (408, 429):
                    raise  # Invalid credentials/configuration need operator intervention.
            logger.warning("Control plane request failed: %s; retry in %ss", error, retry_delay)
            if stop.wait(retry_delay):
                break
            retry_delay = min(retry_delay * 2, 30)
        else:
            retry_delay = 1.0
            if stop.wait(interval):
                break


def main() -> None:
    parser = argparse.ArgumentParser(description="Mini Cloud worker registration and heartbeats")
    parser.add_argument("--identity", required=True, help="Stable unique identity; reuse after restart")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000")
    parser.add_argument("--heartbeat-interval", type=positive_seconds, default=5.0)
    parser.add_argument("--request-timeout", type=positive_seconds, default=5.0)
    parser.add_argument("--poll-jobs", action="store_true", help="Poll and complete simulated jobs only")
    args = parser.parse_args()
    if not args.identity.strip() or len(args.identity.strip()) > 100:
        parser.error("Identity must have 1–100 characters")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        with httpx.Client(base_url=args.api_url, timeout=args.request_timeout) as client:
            run_agent(client, args.identity, args.heartbeat_interval, poll_jobs=args.poll_jobs)
    except KeyboardInterrupt:
        logger.info("Worker stopped")
    except httpx.HTTPStatusError as error:
        parser.exit(1, f"Worker rejected by control plane: {error}\n")


if __name__ == "__main__":
    main()
