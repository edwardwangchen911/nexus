"""Identify an expired host-owned timer after the exact read has been released."""

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from time import monotonic

from nexus.market.object_store import track_object_readers

STAGES = frozenset(
    {
        "manual_requests",
        "candidates",
        "request_events",
        "preparation",
        "independent_proof",
        "native_claims",
    }
)
INDEPENDENT_PROOF_STEPS = frozenset(
    {
        "audit_records",
        "read_scope",
        "request_binding",
        "terminal_proof",
        "fact_digests",
        "single_sol_history",
        "audit_chain",
    }
)
TERMINAL_PROOF_STEPS = frozenset(
    {
        "member_route",
        "terminal_record",
        "owner_binding",
        "job_reservation",
        "lease_attempt",
        "stage_evidence",
        "accounting",
        "closed_output",
        "cleanup",
        "member_disposition",
    }
)


@dataclass(frozen=True)
class ReadProbeDeadline:
    stage: str
    stage_ms: int
    elapsed_ms: int


@dataclass
class ProbeTrace:
    stage: str = "manual_requests"
    since: float = field(default_factory=monotonic)
    closed_cancel: asyncio.CancelledError | None = None
    interrupted: ReadProbeDeadline | None = None
    started: float = field(default_factory=monotonic)
    independent_proof_step: str | None = None
    terminal_proof_step: str | None = None


trace: ContextVar[ProbeTrace | None] = ContextVar("pending_probe", default=None)


def probe_stage(stage: str) -> None:
    current = trace.get()
    if current is not None:
        current.stage = stage
        current.since = monotonic()
        current.independent_proof_step = None
        current.terminal_proof_step = None


def probe_independent_proof_step(step: str) -> None:
    current = trace.get()
    if current is not None and current.stage == "independent_proof":
        current.independent_proof_step = step
        current.terminal_proof_step = None


def probe_terminal_proof_step(step: str) -> None:
    current = trace.get()
    if current is not None and current.independent_proof_step == "terminal_proof":
        current.terminal_proof_step = step


async def bounded_probe(
    read: Callable[[], Awaitable[bool]], timer: asyncio.Timeout
) -> bool | ReadProbeDeadline:
    current = ProbeTrace()
    token = trace.set(current)
    try:
        with track_object_readers() as readers:
            try:
                async with timer:
                    result = await read()
            except TimeoutError as error:
                if (
                    timer.expired()
                    and current.closed_cancel is not None
                    and error.__cause__ is current.closed_cancel
                    and current.interrupted is not None
                    and not any(thread.is_alive() for thread in readers)
                ):
                    return current.interrupted
                raise
            if timer.expired() or not isinstance(result, bool):
                raise RuntimeError("MANUAL_RESULT_UNKNOWN")
            return result
    finally:
        trace.reset(token)
