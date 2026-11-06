"""Append-only, plan-bound forward coverage; the original history gap is never erased."""

import json
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.disclosure_observations import (
    DisclosureObservation,
    _bound_proposal,
    _capture,
)
from nexus.monitoring.storage import lock_security
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.research.sec_disclosure_index import SecDisclosureIndex
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.research.sec_listed_history import ListedHistoryIndex, SecMetadataEntry
from nexus.research.sec_listed_storage import SOURCE as LISTED_SOURCE
from nexus.research.sec_listed_storage import ListedCapture, read_listed_capture

SOURCE = "nexus_sec_forward_origin"
POLICY: Literal["sec_forward_comparison_v2"] = "sec_forward_comparison_v2"


class ForwardOrigin(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    policy: Literal["sec_forward_comparison_v2"] = POLICY
    plan_id: UUID
    account_id: UUID
    instrument_id: UUID
    condition_hash: str
    source_hash: str
    baseline_snapshot_id: UUID
    baseline_hash: str
    baseline_as_of: datetime
    capture_id: UUID
    capture_hash: str
    original_hashes: tuple[str, ...]
    effective_at: datetime
    old_history_gap: Literal["unknown"] = "unknown"

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


class ForwardObservation(DisclosureObservation):
    policy: Literal["sec_forward_comparison_v2"] = POLICY
    origin_id: UUID
    comparison_snapshot_id: UUID
    comparison_hash: str
    comparison_from: datetime
    old_history_gap: Literal["unknown"] = "unknown"
    added: tuple[SecMetadataEntry, ...] = Field(max_length=1000)


def _entries(index: SecDisclosureIndex) -> dict[str, dict[str, object]]:
    return {e.accession_number: e.model_dump() for e in index.entries}


def bootstrap_entries(
    baseline: SecDisclosureIndex, first: ListedHistoryIndex
) -> tuple[SecMetadataEntry, ...]:
    previous, current = _entries(baseline), _entries(first)
    if not first.history_complete or baseline.history_complete:
        raise ValueError("SEC_INDEX_FORWARD_ORIGIN_INVALID")
    if any(current.get(key) != value for key, value in previous.items()):
        raise ValueError("SEC_INDEX_EVENT_CONFLICT")
    return tuple(
        e
        for e in first.entries
        if e.accession_number in first.recent_accessions and e.accession_number not in previous
    )


def compare_forward(
    baseline: SecDisclosureIndex,
    first: ListedHistoryIndex,
    current: ListedHistoryIndex,
    *,
    is_origin: bool,
) -> tuple[str, str | None, tuple[SecMetadataEntry, ...], str]:
    bootstrap = bootstrap_entries(baseline, first)
    previous, present = _entries(first), _entries(current)
    if not current.history_complete:
        return "unknown", "SEC_INDEX_COVERAGE_UNAVAILABLE", (), "partial"
    if any(present.get(key) != value for key, value in previous.items()):
        return "unknown", "SEC_INDEX_EVENT_CONFLICT", (), "unknown"
    added = tuple(
        e
        for e in current.entries
        if e.accession_number not in previous
        or e.accession_number in {x.accession_number for x in bootstrap}
    )
    if is_origin:
        return (
            ("true", None, added, "partial")
            if added
            else ("unknown", "SEC_INDEX_FORWARD_BASELINE_ESTABLISHED", (), "partial")
        )
    return "true" if added else "false", None, added, "complete"


async def _origins(
    session: AsyncSession, plan: MonitorPlan, now: datetime
) -> tuple[DataSnapshot, ...]:
    return tuple(
        await session.scalars(
            select(DataSnapshot)
            .where(
                DataSnapshot.source == SOURCE,
                DataSnapshot.provider_object_id == str(plan.id),
                DataSnapshot.created_at <= now,
                DataSnapshot.available_at <= now,
                DataSnapshot.observed_at <= now,
                DataSnapshot.received_at <= now,
            )
            .order_by(DataSnapshot.available_at, DataSnapshot.id)
            .limit(2)
        )
    )


async def _read_root_origin(
    session: AsyncSession, plan: MonitorPlan, now: datetime
) -> tuple[DataSnapshot, ForwardOrigin, SecDisclosureIndex, ListedHistoryIndex] | None:
    now = require_utc(now)
    rows = await _origins(session, plan, now)
    if not rows:
        return None
    if len(rows) != 1:
        raise ValueError("SEC_INDEX_FORWARD_ORIGIN_INVALID")
    row = rows[0]
    value = ForwardOrigin.model_validate(row.payload)
    proposal = await _bound_proposal(session, plan, now)
    if (
        row.schema_version != POLICY
        or row.revision != sha256(value.canonical()).hexdigest()
        or (
            value.plan_id,
            value.account_id,
            value.instrument_id,
            value.condition_hash,
            value.source_hash,
        )
        != (plan.id, plan.account_id, plan.instrument_id, plan.condition_hash, plan.source_hash)
        or (value.baseline_snapshot_id, value.baseline_hash, value.baseline_as_of)
        != (proposal.baseline_snapshot_id, proposal.baseline_hash, proposal.baseline_as_of)
        or row.available_at != value.effective_at
        or row.observed_at != value.effective_at
        or row.received_at != value.effective_at
        or row.created_at != value.effective_at
        or not plan.effective_at <= value.effective_at < plan.valid_until
    ):
        raise ValueError("SEC_INDEX_FORWARD_ORIGIN_INVALID")
    store = configured_exhibit_store(session)
    old_row, old = await _capture(
        session, store, snapshot_id=value.baseline_snapshot_id, as_of=value.baseline_as_of
    )
    first_row, first = await read_listed_capture(
        session, store, snapshot_id=value.capture_id, as_of=value.effective_at
    )
    if (
        old_row.revision != value.baseline_hash
        or first_row.revision != value.capture_hash
        or (old.instrument_id, old.issuer_cik) != (first.instrument_id, first.issuer_cik)
        or first.coverage_received_at < proposal.baseline_as_of
        or first.instrument_id != plan.instrument_id
        or first.issuer_cik != proposal.issuer_cik
        or value.original_hashes
        != tuple(x.sha256 for x in ListedCapture.model_validate(first_row.payload).originals)
        or not timedelta(0)
        <= value.effective_at - first.coverage_received_at
        <= timedelta(seconds=300)
    ):
        raise ValueError("SEC_INDEX_FORWARD_ORIGIN_INVALID")
    bootstrap_entries(old, first)
    return row, value, old, first


async def read_origin(
    session: AsyncSession, plan: MonitorPlan, now: datetime
) -> tuple[DataSnapshot, ForwardOrigin, SecDisclosureIndex, ListedHistoryIndex] | None:
    from nexus.monitoring.disclosure_inheritance import read_binding

    binding = await read_binding(session, plan, require_utc(now))
    if binding is None:
        return None
    own, root, root_plan, _ = binding
    original = await _read_root_origin(session, root_plan, now)
    if original is None or (original[0].id, original[0].revision) != (root.id, root.revision):
        raise ValueError("SEC_INDEX_FORWARD_ORIGIN_INVALID")
    return own, original[1], original[2], original[3]


async def establish_origin(
    session: AsyncSession, *, plan_id: UUID, capture_id: UUID, now: datetime
) -> DataSnapshot:
    """Normal refresh publishes once under the same security lock as scanning/replacement."""
    from nexus.monitoring.batches import disclosure_source_current

    now = require_utc(now)
    plan = await session.get(MonitorPlan, plan_id)
    if plan is None or plan.kind != "disclosure":
        raise ValueError("MONITOR_DISCLOSURE_PLAN_INVALID")
    await lock_security(session, account_id=plan.account_id, instrument_id=plan.instrument_id)
    locked_at = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(locked_at, datetime) or locked_at < now:
        raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
    now = require_utc(locked_at)
    previous = await read_origin(session, plan, now)
    if previous is not None:
        return previous[0]
    latest = await session.scalar(
        select(MonitorPlan.id)
        .where(
            MonitorPlan.account_id == plan.account_id,
            MonitorPlan.instrument_id == plan.instrument_id,
            MonitorPlan.kind == "disclosure",
        )
        .order_by(MonitorPlan.generation.desc())
        .limit(1)
    )
    if (
        latest != plan.id
        or not plan.effective_at <= now < plan.valid_until
        or not await disclosure_source_current(session, plan, now)
    ):
        raise ValueError("MONITOR_AWAITING_RECONCILIATION")
    proposal = await _bound_proposal(session, plan, now)
    store = configured_exhibit_store(session)
    old_row, old = await _capture(
        session, store, snapshot_id=proposal.baseline_snapshot_id, as_of=proposal.baseline_as_of
    )
    first_row, first = await read_listed_capture(session, store, snapshot_id=capture_id, as_of=now)
    if (
        old_row.revision != proposal.baseline_hash
        or (old.instrument_id, old.issuer_cik) != (plan.instrument_id, proposal.issuer_cik)
        or (first.instrument_id, first.issuer_cik) != (old.instrument_id, old.issuer_cik)
    ):
        raise ValueError("SEC_BASELINE_INVALID")
    if first.coverage_received_at < proposal.baseline_as_of:
        raise ValueError("SEC_INDEX_FORWARD_ORIGIN_INVALID")
    bootstrap_entries(old, first)
    published = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(published, datetime) or published < now:
        raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
    if not plan.effective_at <= published < plan.valid_until or not timedelta(
        0
    ) <= published - first.coverage_received_at <= timedelta(seconds=300):
        raise ValueError("SEC_INDEX_STALE")
    if not await disclosure_source_current(session, plan, published):
        raise ValueError("MONITOR_AWAITING_RECONCILIATION")
    value = ForwardOrigin(
        plan_id=plan.id,
        account_id=plan.account_id,
        instrument_id=plan.instrument_id,
        condition_hash=plan.condition_hash,
        source_hash=plan.source_hash,
        baseline_snapshot_id=proposal.baseline_snapshot_id,
        baseline_hash=proposal.baseline_hash,
        baseline_as_of=proposal.baseline_as_of,
        capture_id=capture_id,
        capture_hash=first_row.revision,
        original_hashes=tuple(
            x.sha256 for x in ListedCapture.model_validate(first_row.payload).originals
        ),
        effective_at=published,
    )
    row = DataSnapshot(
        created_at=published,
        source=SOURCE,
        provider_object_id=str(plan.id),
        revision=sha256(value.canonical()).hexdigest(),
        schema_version=POLICY,
        observed_at=published,
        available_at=published,
        received_at=published,
        payload=value.model_dump(mode="json"),
    )
    session.add(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.forward_origin_established",
        schema_version=POLICY,
        occurred_at=published,
        payload={
            "plan_id": str(plan.id),
            "origin_id": str(row.id),
            "origin_hash": row.revision,
            "capture_id": str(capture_id),
            "old_history_gap": "unknown",
        },
    )
    return row


async def read_forward_observation(
    session: AsyncSession, *, plan: MonitorPlan, now: datetime
) -> ForwardObservation | None:
    origin = await read_origin(session, plan, now)
    if origin is None:
        return None
    row, value, baseline, first = origin
    values = dict(
        instrument_id=plan.instrument_id,
        plan_id=plan.id,
        baseline_snapshot_id=value.baseline_snapshot_id,
        baseline_hash=value.baseline_hash,
        baseline_as_of=value.baseline_as_of,
        issuer_cik=first.issuer_cik,
        current_snapshot_id=None,
        current_hash=None,
        received_at=None,
        available_at=None,
        history_coverage="unknown",
        added=(),
        origin_id=row.id,
        comparison_snapshot_id=value.capture_id,
        comparison_hash=value.capture_hash,
        comparison_from=value.effective_at,
    )

    def unknown(reason: str) -> ForwardObservation:
        return ForwardObservation.model_validate({**values, "result": "unknown", "reason": reason})

    candidate = await session.scalar(
        select(DataSnapshot)
        .where(
            DataSnapshot.source == LISTED_SOURCE,
            or_(
                DataSnapshot.provider_object_id.startswith(f"{plan.instrument_id}:v2:"),
                DataSnapshot.payload["index"]["instrument_id"].astext == str(plan.instrument_id),
            ),
            DataSnapshot.created_at <= now,
            DataSnapshot.available_at <= now,
            DataSnapshot.observed_at <= now,
            DataSnapshot.received_at <= now,
        )
        .order_by(
            DataSnapshot.observed_at.desc(),
            DataSnapshot.available_at.desc(),
            DataSnapshot.id.desc(),
        )
        .limit(1)
    )
    if candidate is None:
        return unknown("SEC_INDEX_CAPTURE_UNAVAILABLE")
    current_row, current = await read_listed_capture(
        session, configured_exhibit_store(session), snapshot_id=candidate.id, as_of=now
    )
    if (current.instrument_id, current.issuer_cik) != (first.instrument_id, first.issuer_cik):
        return unknown("SEC_INDEX_IDENTITY_INVALID")
    values.update(
        current_snapshot_id=current_row.id,
        current_hash=current_row.revision,
        received_at=current.coverage_received_at,
        available_at=current_row.available_at,
    )
    if not timedelta(0) <= now - current.coverage_received_at <= timedelta(seconds=300):
        return unknown("SEC_INDEX_STALE")
    is_origin = current_row.id == value.capture_id
    if not is_origin and (
        current.coverage_received_at < value.effective_at
        or current_row.available_at <= value.effective_at
    ):
        return unknown("SEC_INDEX_FORWARD_CAPTURE_NOT_NEWER")
    result, reason, added, coverage = compare_forward(baseline, first, current, is_origin=is_origin)
    return ForwardObservation.model_validate(
        {**values, "result": result, "reason": reason, "added": added, "history_coverage": coverage}
    )
