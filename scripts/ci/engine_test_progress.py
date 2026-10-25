"""Flush synthetic test identity and timing evidence before a CI cancellation."""

from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

OUTPUT_DIRECTORY = Path("test-results").resolve()


def _write(event: str, **fields: Any) -> None:
    group = os.environ.get("ENGINE_TEST_GROUP", "")
    if re.fullmatch(r"(?:fast|pg-[a-g]|full)", group) is None:
        return
    OUTPUT_DIRECTORY.mkdir(exist_ok=True)
    value = {"event": event, "utc": datetime.now(UTC).isoformat(), **fields}
    # Close each line so setup/call/teardown evidence survives a killed process.
    # Never serialize report bodies, credentials or the process environment.
    with (OUTPUT_DIRECTORY / f"engine-{group}.jsonl").open("a", encoding="utf-8") as output:
        output.write(json.dumps(value, sort_keys=True) + "\n")


def pytest_runtest_logstart(nodeid: str, location: tuple[str, int | None, str]) -> None:
    _write("start", nodeid=nodeid)


def pytest_runtest_logreport(report: Any) -> None:
    _write(
        "report",
        nodeid=report.nodeid,
        when=report.when,
        outcome=report.outcome,
        duration=report.duration,
    )


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    _write("finish", exitstatus=int(exitstatus))
