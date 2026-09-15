"""Run the dedicated, local-only Bark outbox worker."""

from __future__ import annotations

import argparse
import asyncio
import os

from nexus.notifications.worker import (
    BarkWorkerConfigurationError,
    load_bark_worker_settings,
    run_bark_worker,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--once",
        action="store_true",
        help="claim at most one eligible persisted notification",
    )
    return parser


def main() -> None:
    parser = build_parser()
    arguments = parser.parse_args()
    try:
        settings = load_bark_worker_settings(os.environ)
        asyncio.run(run_bark_worker(settings, run_forever=not arguments.once))
    except BarkWorkerConfigurationError as error:
        parser.error(str(error))
    except KeyboardInterrupt:
        pass
