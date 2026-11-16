"""One explicitly opted-in plan may prove seen additions; never absence or coverage."""

import json
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.disclosure_forward import read_origin
from nexus.monitoring.disclosure_inheritance import read_binding
from nexus.monitoring.disclosure_observations import DisclosureObservation, _capture
from nexus.monitoring.storage import lock_security
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan, MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent
from nexus.research.sec_disclosure_index import SecDisclosureIndex, SecIndexEntry
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.research.sec_index_storage import SOURCE as INDEX_SOURCE
from nexus.research.sec_listed_history import ListedHistoryIndex, SecMetadataEntry

POLICY: Literal["sec_recent_positive_once_v1"] = "sec_recent_positive_once_v1"
SOURCE = "nexus_sec_recent_positive_policy"
ACTION = "research.monitor.recent_positive_policy_enabled"
INVALID = "SEC_RECENT_POSITIVE_POLICY_INVALID"


class RecentPositivePolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    policy: Literal["sec_recent_positive_once_v1"] = POLICY
    plan_id: UUID
    root_request_id: UUID
    own_origin_id: UUID
    own_origin_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    account_id: UUID
    instrument_id: UUID
    source_run_id: UUID
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    condition_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_origin_id: UUID
    root_origin_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    comparison_snapshot_id: UUID
    comparison_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    issuer_cik: str = Field(pattern=r"^[0-9]{10}$")
    effective_at: datetime
    authorization_scope: Literal["single_plan_seen_additions_20261116_v1"] = (
        "single_plan_seen_additions_20261116_v1"
    )

    @field_validator("effective_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


class RecentPositiveObservation(DisclosureObservation):
    policy: Literal["sec_recent_positive_once_v1"] = POLICY
    policy_snapshot_id: UUID
    policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    origin_id: UUID
    comparison_snapshot_id: UUID
    comparison_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    comparison_from: datetime
    old_history_gap: Literal["unknown"] = "unknown"
    history_coverage: Literal["partial"] = "partial"
    result: Literal["true", "unknown"]


def seen_additions(
    first: ListedHistoryIndex, current: SecDisclosureIndex
) -> tuple[SecIndexEntry, ...]:
    """All C1 accessions are the reference; visible same-key changes veto additions."""
    if (first.instrument_id, first.issuer_cik) != (current.instrument_id, current.issuer_cik):
        raise ValueError("SEC_INDEX_IDENTITY_INVALID")
    previous = {entry.accession_number: entry.model_dump() for entry in first.entries}
    if any(
        entry.accession_number in previous
        and previous[entry.accession_number] != entry.model_dump()
        for entry in current.entries
    ):
        raise ValueError("SEC_INDEX_EVENT_CONFLICT")
    return tuple(entry for entry in current.entries if entry.accession_number not in previous)


async def read_policy(
    session: AsyncSession, plan: MonitorPlan, now: datetime
) -> tuple[DataSnapshot, RecentPositivePolicy] | None:
    """No inheritance: only this exact plan, hash, root custody and append-only audit."""
    now = require_utc(now)
    rows = tuple(
        await session.scalars(
            select(DataSnapshot)
            .where(
                DataSnapshot.source == SOURCE,
                DataSnapshot.provider_object_id == str(plan.id),
                DataSnapshot.created_at <= now,
            )
            .limit(2)
        )
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise ValueError(INVALID)
    row = rows[0]
    policy = RecentPositivePolicy.model_validate(row.payload)
    binding = await read_binding(session, plan, now)
    if binding is None:
        raise ValueError(INVALID)
    own, root, _, origin = binding
    root_request = await _root_request(session, plan)
    expected = (
        plan.id,
        root_request,
        own.id,
        own.revision,
        plan.account_id,
        plan.instrument_id,
        plan.source_run_id,
        plan.source_hash,
        plan.condition_hash,
        root.id,
        root.revision,
        origin.capture_id,
        origin.capture_hash,
    )
    actual = (
        policy.plan_id,
        policy.root_request_id,
        policy.own_origin_id,
        policy.own_origin_hash,
        policy.account_id,
        policy.instrument_id,
        policy.source_run_id,
        policy.source_hash,
        policy.condition_hash,
        policy.root_origin_id,
        policy.root_origin_hash,
        policy.comparison_snapshot_id,
        policy.comparison_hash,
    )
    audit = await session.scalar(
        select(AuditEvent.id)
        .where(
            AuditEvent.action == ACTION,
            AuditEvent.schema_version == POLICY,
            AuditEvent.payload["snapshot_id"].astext == str(row.id),
            AuditEvent.payload["policy_hash"].astext == row.revision,
            AuditEvent.occurred_at == policy.effective_at,
        )
        .limit(1)
    )
    if (
        row.schema_version != POLICY
        or row.revision != sha256(policy.canonical()).hexdigest()
        or actual != expected
        or policy.issuer_cik != (await _issuer(session, plan, now))
        or any(
            at != policy.effective_at
            for at in (row.created_at, row.observed_at, row.available_at, row.received_at)
        )
        or not plan.effective_at <= policy.effective_at < plan.valid_until
        or audit is None
    ):
        raise ValueError(INVALID)
    return row, policy


async def _issuer(session: AsyncSession, plan: MonitorPlan, now: datetime) -> str:
    from nexus.monitoring.disclosure_observations import _bound_proposal

    return (await _bound_proposal(session, plan, now)).issuer_cik


async def enable_policy(
    session: AsyncSession, *, account_id: UUID, plan_id: UUID, now: datetime
) -> DataSnapshot:
    from nexus.monitoring.batches import disclosure_source_current
    from nexus.monitoring.source_qualification import load_qualified_source

    now = require_utc(now)
    plan = await session.get(MonitorPlan, plan_id)
    if plan is None or plan.account_id != account_id or plan.kind != "disclosure":
        raise ValueError(INVALID)
    await lock_security(session, account_id=account_id, instrument_id=plan.instrument_id)
    root_request = await _root_request(session, plan)
    clock = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(clock, datetime):
        raise ValueError(INVALID)
    now = clock
    existing = await read_policy(session, plan, now)
    if existing is not None:
        return existing[0]
    if not plan.effective_at <= now < plan.valid_until:
        raise ValueError(INVALID)
    latest = await session.scalar(
        select(MonitorPlan.id)
        .where(
            MonitorPlan.account_id == account_id,
            MonitorPlan.instrument_id == plan.instrument_id,
            MonitorPlan.kind == "disclosure",
        )
        .order_by(MonitorPlan.generation.desc())
        .limit(1)
    )
    if latest != plan.id or not await disclosure_source_current(session, plan, now):
        raise ValueError("MONITOR_AWAITING_RECONCILIATION")
    await load_qualified_source(
        session, account_id=account_id, output_id=plan.source_output_id, now=now
    )
    origin = await read_origin(session, plan, now)
    binding = await read_binding(session, plan, now)
    if origin is None or binding is None:
        raise ValueError(INVALID)
    _, basis, _, first = origin
    own, root, _, _ = binding
    published = await session.scalar(select(func.clock_timestamp()))
    if (
        not isinstance(published, datetime)
        or published < now
        or not plan.effective_at <= published < plan.valid_until
        or not await disclosure_source_current(session, plan, published)
    ):
        raise ValueError("MONITOR_AWAITING_RECONCILIATION")
    now = published
    policy = RecentPositivePolicy(
        plan_id=plan.id,
        root_request_id=root_request,
        own_origin_id=own.id,
        own_origin_hash=own.revision,
        account_id=account_id,
        instrument_id=plan.instrument_id,
        source_run_id=plan.source_run_id,
        source_hash=plan.source_hash,
        condition_hash=plan.condition_hash,
        root_origin_id=root.id,
        root_origin_hash=root.revision,
        comparison_snapshot_id=basis.capture_id,
        comparison_hash=basis.capture_hash,
        issuer_cik=first.issuer_cik,
        effective_at=now,
    )
    row = DataSnapshot(
        created_at=now,
        source=SOURCE,
        provider_object_id=str(plan.id),
        revision=sha256(policy.canonical()).hexdigest(),
        schema_version=POLICY,
        observed_at=now,
        available_at=now,
        received_at=now,
        payload=policy.model_dump(mode="json"),
    )
    session.add(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action=ACTION,
        schema_version=POLICY,
        occurred_at=now,
        payload={
            "snapshot_id": str(row.id),
            "policy_hash": row.revision,
            **policy.model_dump(mode="json"),
        },
    )
    return row


async def read_recent_observation(
    session: AsyncSession, *, plan: MonitorPlan, now: datetime
) -> RecentPositiveObservation | None:
    selected: Any = await read_policy(session, plan, now)
    observation_type: type[RecentPositiveObservation] = RecentPositiveObservation
    extra_values: dict[str, Any] = {}
    if selected is None:
        from nexus.monitoring.recent_positive_chain import ChainPositiveObservation, read_member

        membership = await read_member(session, plan, now)
        if membership is None:
            return None
        member_row, member = membership
        selected = member_row, member
        observation_type = ChainPositiveObservation
        extra_values = {
            "scope_snapshot_id": member.scope_snapshot_id,
            "scope_hash": member.scope_hash,
        }
    policy_row, policy = selected
    origin = await read_origin(session, plan, now)
    if origin is None:
        raise ValueError(INVALID)
    own, basis, _, first = origin
    values = dict(
        instrument_id=plan.instrument_id,
        plan_id=plan.id,
        baseline_snapshot_id=basis.baseline_snapshot_id,
        baseline_hash=basis.baseline_hash,
        baseline_as_of=basis.baseline_as_of,
        issuer_cik=policy.issuer_cik,
        current_snapshot_id=None,
        current_hash=None,
        received_at=None,
        available_at=None,
        history_coverage="partial",
        added=(),
        origin_id=own.id,
        comparison_snapshot_id=basis.capture_id,
        comparison_hash=basis.capture_hash,
        comparison_from=basis.effective_at,
        policy_snapshot_id=policy_row.id,
        policy_hash=policy_row.revision,
        **extra_values,
    )

    def result(reason: str, added: tuple[SecIndexEntry, ...] = ()) -> RecentPositiveObservation:
        return observation_type.model_validate(
            {
                **values,
                "result": "true" if added else "unknown",
                "reason": None if added else reason,
                "added": tuple(entry.model_dump() for entry in added),
            }
        )

    candidate = await session.scalar(
        select(DataSnapshot)
        .where(
            DataSnapshot.source == INDEX_SOURCE,
            or_(
                DataSnapshot.provider_object_id.startswith(f"{plan.instrument_id}:"),
                DataSnapshot.payload["index"]["instrument_id"].astext == str(plan.instrument_id),
            ),
            DataSnapshot.created_at <= now,
            DataSnapshot.available_at <= now,
            DataSnapshot.observed_at <= now,
            DataSnapshot.received_at <= now,
            DataSnapshot.available_at >= policy.effective_at,
        )
        .order_by(
            DataSnapshot.observed_at.desc(),
            DataSnapshot.available_at.desc(),
            DataSnapshot.id.desc(),
        )
        .limit(1)
    )
    if candidate is None:
        return result("SEC_INDEX_CAPTURE_UNAVAILABLE")
    row, current = await _capture(
        session, configured_exhibit_store(session), snapshot_id=candidate.id, as_of=now
    )
    values.update(
        current_snapshot_id=row.id,
        current_hash=row.revision,
        received_at=current.received_at,
        available_at=row.available_at,
    )
    if not timedelta(0) <= now - current.received_at <= timedelta(seconds=300):
        return result("SEC_INDEX_STALE")
    try:
        known = await all_c1_accessions(
            session, basis.capture_id, basis.capture_hash, row, plan=plan, now=now
        )
        added = tuple(
            entry for entry in seen_additions(first, current) if entry.accession_number not in known
        )
    except ValueError as error:
        return result(str(error))
    return result("SEC_RECENT_NO_NEW_TARGET_CONFIRMED", added)


async def all_c1_accessions(
    session: AsyncSession,
    first_id: UUID,
    first_hash: str,
    current_row: DataSnapshot,
    *,
    plan: MonitorPlan,
    now: datetime,
) -> set[str]:
    """Compare visible raw metadata for every form, not only parsed target entries.

    Caller has already verified C1 with read_origin and current with read_index_capture.
    Re-read exact hash-bound originals; missing raw evidence fails closed.
    """
    from nexus.research.sec_disclosure_index import _FIELDS, _object
    from nexus.research.sec_index_storage import IndexCapture, _original
    from nexus.research.sec_listed_storage import ListedCapture

    first_row = await session.get(DataSnapshot, first_id)
    if first_row is None:
        raise ValueError(INVALID)
    first = ListedCapture.model_validate(first_row.payload)
    current = IndexCapture.model_validate(current_row.payload)
    if (
        first_row.revision != first_hash
        or sha256(first.canonical()).hexdigest() != first_hash
        or sha256(current.canonical()).hexdigest() != current_row.revision
    ):
        raise ValueError(INVALID)
    store = configured_exhibit_store(session)
    known: dict[str, tuple[Any, ...]] = {}
    for offset in (2, 4, *range(6, len(first.originals), 2)):
        document = _object(await _original(store, first.originals[offset]))
        group = document["filings"]["recent"] if "filings" in document else document
        for row in zip(*(group[key] for key in _FIELDS), strict=True):
            if row[0] in known and known[row[0]] != row:
                raise ValueError("SEC_INDEX_EVENT_CONFLICT")
            known[row[0]] = row
    document = _object(await _original(store, current.originals[2]))
    group = document["filings"]["recent"]
    current_rows = {row[0]: row for row in zip(*(group[key] for key in _FIELDS), strict=True)}
    for accession, raw in current_rows.items():
        if accession in known and known[accession] != raw:
            raise ValueError("SEC_INDEX_EVENT_CONFLICT")
    keys = [f"sec:{current.index.issuer_cik}:{key}" for key in current_rows]
    if keys:
        observations = await session.scalars(
            select(MonitorCheck.observation)
            .join(MonitorTrigger, MonitorTrigger.check_id == MonitorCheck.id)
            .where(
                MonitorTrigger.account_id == plan.account_id,
                MonitorTrigger.instrument_id == plan.instrument_id,
                MonitorTrigger.kind == "disclosure",
                MonitorTrigger.event_key.in_(keys),
                MonitorTrigger.created_at <= now,
                MonitorCheck.created_at <= now,
                MonitorCheck.checked_at <= now,
            )
        )
        for observed in observations:
            if not isinstance(observed, dict):
                raise ValueError("SEC_INDEX_EVENT_CONFLICT")
            entry = SecMetadataEntry.model_validate(observed.get("entry"))
            if not same_visible_metadata(entry, current_rows[entry.accession_number]):
                raise ValueError("SEC_INDEX_EVENT_CONFLICT")
    return set(known)


async def _root_request(session: AsyncSession, plan: MonitorPlan) -> UUID:
    """Only the already authorized original v6 root, never its incremental successors."""
    root = await session.scalar(
        text("""SELECT q.id FROM public.research_request_members m
          JOIN public.research_request_manifests f ON f.id=m.manifest_id
          JOIN public.manual_paper_requests q ON q.id=f.request_id
          WHERE m.run_id=:run AND m.instrument_id=:instrument
            AND q.account_id=:account AND q.contract_version=6
            AND q.mode='research_and_monitor'
            AND q.id=public.single_monitor_root_request(:run)"""),
        {"run": plan.source_run_id, "instrument": plan.instrument_id, "account": plan.account_id},
    )
    if not isinstance(root, UUID):
        raise ValueError("SEC_RECENT_POSITIVE_PLAN_NOT_AUTHORIZED")
    return root


def same_visible_metadata(entry: SecMetadataEntry, raw: tuple[Any, ...]) -> bool:
    """A previously dispatched accession remains bound to its original metadata."""
    from nexus.research.sec_disclosure_index import _timestamp

    return (
        entry.accession_number == raw[0]
        and entry.form == raw[1]
        and entry.accepted_at == _timestamp(raw[2])
        and entry.filed_at.date().isoformat() == raw[3]
        and entry.primary_document == raw[4]
    )
