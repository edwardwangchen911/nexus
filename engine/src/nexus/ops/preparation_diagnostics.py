"""Fixed, non-sensitive diagnostics for existing preparation boundaries only."""

import json
import socket
import ssl
import sys
from collections.abc import Awaitable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from nexus.providers.http import HttpPolicyError

PHASES = frozenset({"configuration", "prior_daily", "macro", "current_trends", "research_identity"})
POINTS = frozenset(
    {
        "configuration",
        "operation",
        "candidates",
        "scope",
        "archive",
        "window",
        "collect",
        "import",
        "snapshot",
        "readback",
    }
)
ERROR_CODES = frozenset(
    {
        "TIMEOUT",
        "DATABASE_ERROR",
        "DATABASE_CONSTRAINT_REJECTED",
        "DATABASE_CONNECTION_ERROR",
        "CONNECTION_ERROR",
        "OS_ERROR",
        "HTTP_POLICY_REJECTED",
        "VALIDATION_REJECTED",
        "UNEXPECTED_EXCEPTION",
    }
)
UNAVAILABLE_CODES = frozenset(
    {
        "ATTEMPT_EXISTS",
        "COLLECTION_UNAVAILABLE",
        "VALIDATION_REJECTED",
        "ARCHIVED_TIMEOUT",
        "IMPORT_RESULT_INVALID",
        "READBACK_MISMATCH",
        "SCOPE_UNAVAILABLE",
        "CAPACITY_EXHAUSTED",
        "IDENTITY_BASE_MISSING",
        "CONTACT_MISSING",
        "PRIOR_IMPORT_FAILED",
        "ARCHIVE_INVALID",
        "SCOPE_CHANGED",
        "IMPORT_REJECTED",
        "OBSERVATION_SESSION_MISMATCH",
    }
)


def exception_code(error: BaseException) -> str:
    """Classify types and fixed SQLSTATE values; never serialize exception text."""
    if isinstance(error, TimeoutError):
        return "TIMEOUT"
    if isinstance(error, DBAPIError):
        state = getattr(error.orig, "sqlstate", None)
        if type(state) is str and state in {"23502", "23503", "23505", "23514", "23P01"}:
            return "DATABASE_CONSTRAINT_REJECTED"
        if error.connection_invalidated:
            return "DATABASE_CONNECTION_ERROR"
    if isinstance(error, SQLAlchemyError):
        return "DATABASE_ERROR"
    if isinstance(error, ConnectionError | socket.gaierror | ssl.SSLError):
        return "CONNECTION_ERROR"
    if isinstance(error, OSError):
        return "OS_ERROR"
    if isinstance(error, HttpPolicyError):
        return "HTTP_POLICY_REJECTED"
    if isinstance(error, ValueError):
        return "VALIDATION_REJECTED"
    return "UNEXPECTED_EXCEPTION"


@contextmanager
def preparation_step(phase: str, point: str) -> Iterator[None]:
    try:
        yield
    except Exception as error:
        if phase in PHASES and point in POINTS:
            error._nexus_preparation_diagnostic = (phase, point, exception_code(error))  # type: ignore[attr-defined]
        raise


async def preparation_call[T](phase: str, point: str, operation: Awaitable[T]) -> T:
    with preparation_step(phase, point):
        return await operation


def preparation_failure_fields(error: BaseException | None) -> dict[str, str]:
    raw = getattr(error, "_nexus_preparation_diagnostic", None)
    if (
        type(raw) is tuple
        and len(raw) == 3
        and all(type(value) is str for value in raw)
        and raw[0] in PHASES
        and raw[1] in POINTS
        and raw[2] in ERROR_CODES | UNAVAILABLE_CODES
    ):
        return {"failure_phase": raw[0], "failure_point": raw[1], "failure_code": raw[2]}
    return {}


def preparation_unknown(phase: str, point: str, code: str) -> RuntimeError:
    error = RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    error._nexus_preparation_diagnostic = (phase, point, code)  # type: ignore[attr-defined]
    return error


def preparation_unavailable(
    phase: str, point: str, code: str, *, error: BaseException | None = None
) -> str:
    """Explain an existing unavailable return without changing its control flow."""
    try:
        fields = preparation_failure_fields(preparation_unknown(phase, point, code))
        if fields:
            status = error.http_status if isinstance(error, HttpPolicyError) else None
            http_fields = (
                {"http_status": status} if type(status) is int and 100 <= status <= 599 else {}
            )
            print(
                json.dumps(
                    {
                        "event": "agent_paper_preparation_unavailable",
                        "status": "unavailable",
                        "occurred_at": datetime.now(UTC).isoformat(),
                        **fields,
                        **http_fields,
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
    except Exception:
        pass
    return "unavailable"


def preparation_rejected(phase: str, point: str, error: BaseException) -> str:
    fields = preparation_failure_fields(error)
    return preparation_unavailable(
        phase, fields.get("failure_point", point), exception_code(error), error=error
    )
