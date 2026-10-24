"""CLI entry point for the independent localhost convergence controller."""

import argparse
import logging
import math
import signal
from threading import Event

from backend.controller.client import ControllerClient, open_client, validate_api_url
from backend.controller.convergence import run_controller


logger = logging.getLogger(__name__)


def positive_seconds(value):
    try:
        result = float(value)
        if not math.isfinite(result) or result <= 0:
            raise ValueError
        return result
    except ValueError:
        raise argparse.ArgumentTypeError("Must be a finite positive number") from None


def loopback_url(value):
    try:
        return validate_api_url(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from None


def page_size(value):
    try:
        result = int(value)
        if not 1 <= result <= 100:
            raise ValueError
        return result
    except ValueError:
        raise argparse.ArgumentTypeError("Page size must be from 1 to 100") from None


def main(argv=None):
    parser = argparse.ArgumentParser(description="Mini Cloud automatic convergence controller")
    parser.add_argument("--api-url", type=loopback_url, default="http://127.0.0.1:8000")
    parser.add_argument("--interval", type=positive_seconds, default=5.0)
    parser.add_argument("--request-timeout", type=positive_seconds, default=5.0)
    parser.add_argument("--page-size", type=page_size, default=100)
    parser.add_argument("--once", action="store_true", help="One diagnostic cycle: exit 0 success, 1 degraded/fatal, 2 aborted")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    stop = Event()
    def interrupt(_signal, _frame):
        stop.set()
        logger.info("event=shutdown_requested")
    previous_handler = signal.signal(signal.SIGINT, interrupt)
    try:
        with open_client(args.api_url, args.request_timeout) as http:
            return run_controller(ControllerClient(http), interval=args.interval, page_size=args.page_size,
                                  once=args.once, stop_event=stop)
    except KeyboardInterrupt:
        logger.info("event=controller_stopped reason=keyboard_interrupt")
        return 0
    finally:
        signal.signal(signal.SIGINT, previous_handler)


if __name__ == "__main__":
    raise SystemExit(main())
