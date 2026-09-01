"""Freeze broker facts only after a bounded, complete double read reconciles."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from nexus.domain.ids import new_uuid7
from nexus.providers.broker.base import (
    BrokerProvider,
    BrokerProviderError,
    BrokerSyncResult,
    PortfolioSnapshot,
    SnapshotStatus,
)
from nexus.reconciliation.facts import (
    BrokerReadSet,
    DerivedCurrencyNav,
    DuplicateBrokerFact,
    canonical_order_hash,
    canonical_position_hash,
    compare_read_sets,
)
from nexus.reconciliation.read_attempt import (
    AtomicWindowInvalidated,
    AttemptProgress,
    read_atomic_window,
)

__all__ = [
    "BrokerReadSet",
    "DerivedCurrencyNav",
    "DuplicateBrokerFact",
    "canonical_order_hash",
    "canonical_position_hash",
    "compare_read_sets",
    "freeze_broker_snapshot",
]


async def freeze_broker_snapshot(
    account_id: UUID,
    provider: BrokerProvider,
    *,
    portfolio_id: UUID | None = None,
    sync_run_id: UUID | None = None,
    deadline: timedelta = timedelta(seconds=60),
    prior_ready_snapshot: PortfolioSnapshot | None = None,
    max_attempts: Literal[1, 2] = 2,
    monotonic: Callable[[], float] = time.monotonic,
) -> BrokerSyncResult:
    """Run one or two complete attempts under one internal sync-run identity."""
    timeout_seconds = deadline.total_seconds()
    if timeout_seconds <= 0 or timeout_seconds > 60:
        raise ValueError("deadline must be within (0, 60] seconds")
    if max_attempts not in (1, 2):
        raise ValueError("max_attempts must be 1 or 2")
    if prior_ready_snapshot is not None and prior_ready_snapshot.status != "READY":
        raise ValueError("prior snapshot must be READY")
    if prior_ready_snapshot is not None and prior_ready_snapshot.portfolio_id != (
        portfolio_id or account_id
    ):
        raise ValueError("prior READY snapshot must belong to the same portfolio")
    sync_run_id = sync_run_id or new_uuid7()
    traces: list[dict[str, object]] = []
    invalidated_current_window = False
    last_complete_read: BrokerReadSet | None = None
    last_reasons: tuple[str, ...] = ()
    latest_as_of: datetime | None = None

    for attempt_count in range(1, max_attempts + 1):
        progress = AttemptProgress(attempt=attempt_count)
        attempt_deadline = monotonic() + timeout_seconds
        try:
            async with asyncio.timeout(timeout_seconds):
                read_a, read_b = await read_atomic_window(
                    account_id,
                    provider,
                    sync_run_id,
                    progress,
                    attempt_deadline=attempt_deadline,
                    monotonic=monotonic,
                )
            latest_as_of = read_b.completed_at
            last_complete_read = read_b
            if not read_a.complete or not read_b.complete:
                invalidated_current_window = (
                    invalidated_current_window or progress.current_fact_obtained
                )
                last_reasons = last_reasons or ("incomplete_provider_facts",)
                traces.append(progress.trace(completed=True, reason="incomplete_provider_facts"))
                if attempt_count < max_attempts:
                    continue
                if invalidated_current_window:
                    return _reconciliation_required(
                        sync_run_id,
                        portfolio_id or account_id,
                        attempt_count,
                        last_complete_read,
                        last_reasons or ("incomplete_provider_facts",),
                        traces,
                        latest_as_of,
                    )
                return _unavailable_result(
                    sync_run_id,
                    attempt_count,
                    portfolio_id or account_id,
                    prior_ready_snapshot,
                    traces,
                    latest_as_of,
                )
            reconciliation = compare_read_sets(read_a, read_b)
            if reconciliation.consistent:
                traces.append(progress.trace(completed=True, reason="reconciled"))
                snapshot = _snapshot(portfolio_id or account_id, "READY", read_b)
                return BrokerSyncResult(
                    sync_run_id=sync_run_id,
                    attempt_count=attempt_count,
                    status="READY",
                    portfolio_snapshot=snapshot,
                    attempt_traces=tuple(traces),
                )
            invalidated_current_window = True
            last_reasons = reconciliation.reasons
            traces.append(progress.trace(completed=True, reason="reconciliation_mismatch"))
        except DuplicateBrokerFact as error:
            invalidated_current_window = True
            last_reasons = ("duplicate_facts",)
            traces.append(progress.trace(completed=False, reason=str(error)))
        except TimeoutError:
            invalidated_current_window = (
                invalidated_current_window or progress.current_fact_obtained
            )
            last_reasons = last_reasons or ("deadline_expired",)
            traces.append(progress.trace(completed=False, reason="deadline_expired"))
        except BrokerProviderError as error:
            invalidated_current_window = (
                invalidated_current_window
                or progress.current_fact_obtained
                or isinstance(error, AtomicWindowInvalidated)
            )
            last_reasons = last_reasons or ("provider_read_failed",)
            traces.append(progress.trace(completed=False, reason=type(error).__name__))

        latest_as_of = progress.latest_provider_as_of or latest_as_of
        if attempt_count < max_attempts:
            continue
        if invalidated_current_window:
            return _reconciliation_required(
                sync_run_id,
                portfolio_id or account_id,
                attempt_count,
                last_complete_read,
                last_reasons,
                traces,
                latest_as_of,
            )
        return _unavailable_result(
            sync_run_id,
            attempt_count,
            portfolio_id or account_id,
            prior_ready_snapshot,
            traces,
            latest_as_of,
        )
    raise AssertionError("bounded attempt loop must return")


def _reconciliation_required(
    sync_run_id: UUID,
    portfolio_id: UUID,
    attempt_count: int,
    read: BrokerReadSet | None,
    reasons: tuple[str, ...],
    traces: list[dict[str, object]],
    as_of: datetime | None,
) -> BrokerSyncResult:
    snapshot = (
        _snapshot(portfolio_id, "RECONCILIATION_REQUIRED", read)
        if read is not None
        else PortfolioSnapshot(
            id=new_uuid7(),
            portfolio_id=portfolio_id,
            status="RECONCILIATION_REQUIRED",
            cash=(),
            positions=(),
            open_orders=(),
            as_of=as_of,
            source_request_ids=_trace_request_ids(traces),
            source_observations=(),
        )
    )
    return BrokerSyncResult(
        sync_run_id=sync_run_id,
        attempt_count=attempt_count,
        status="RECONCILIATION_REQUIRED",
        portfolio_snapshot=snapshot,
        reconciliation_reasons=reasons,
        attempt_traces=tuple(traces),
    )


def _unavailable_result(
    sync_run_id: UUID,
    attempt_count: int,
    portfolio_id: UUID,
    prior_ready_snapshot: PortfolioSnapshot | None,
    traces: list[dict[str, object]],
    as_of: datetime | None,
) -> BrokerSyncResult:
    if prior_ready_snapshot is not None:
        stale = PortfolioSnapshot(
            id=new_uuid7(),
            portfolio_id=portfolio_id,
            status="STALE",
            cash=prior_ready_snapshot.cash,
            positions=prior_ready_snapshot.positions,
            open_orders=prior_ready_snapshot.open_orders,
            as_of=prior_ready_snapshot.as_of,
            stale_from_snapshot_id=prior_ready_snapshot.id,
            source_request_ids=_trace_request_ids(traces),
            source_observations=(),
        )
        return BrokerSyncResult(
            sync_run_id=sync_run_id,
            attempt_count=attempt_count,
            status="STALE",
            portfolio_snapshot=stale,
            attempt_traces=tuple(traces),
        )
    partial = PortfolioSnapshot(
        id=new_uuid7(),
        portfolio_id=portfolio_id,
        status="PARTIAL",
        cash=(),
        positions=(),
        open_orders=(),
        as_of=as_of,
        source_request_ids=_trace_request_ids(traces),
        source_observations=(),
    )
    return BrokerSyncResult(
        sync_run_id=sync_run_id,
        attempt_count=attempt_count,
        status="PARTIAL",
        portfolio_snapshot=partial,
        attempt_traces=tuple(traces),
    )


def _snapshot(
    portfolio_id: UUID,
    status: SnapshotStatus,
    read: BrokerReadSet,
) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        id=new_uuid7(),
        portfolio_id=portfolio_id,
        status=status,
        cash=read.cash,
        positions=read.positions,
        open_orders=read.open_orders,
        as_of=read.completed_at,
        source_request_ids=read.source_request_ids,
        source_observations=read.source_observations,
    )


def _trace_request_ids(traces: list[dict[str, object]]) -> tuple[str, ...]:
    request_ids: list[str] = []
    for trace in traces:
        value = trace.get("source_request_ids")
        if isinstance(value, tuple | list):
            request_ids.extend(item for item in value if isinstance(item, str))
    return tuple(request_ids)
