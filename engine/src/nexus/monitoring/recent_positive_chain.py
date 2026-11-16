"""Explicit seven-day seen-addition scope; append-only per-plan normal custody.

Readers never register members. Scope expiration or revocation ends only this
additional route; it does not rewrite the original one-plan authorization.
"""

import json
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.disclosure_inheritance import read_binding
from nexus.monitoring.recent_positive_policy import (
    RecentPositiveObservation,
    RecentPositivePolicy,
    read_policy,
)
from nexus.monitoring.storage import lock_security
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan
from nexus.persistence.models.runtime import AuditEvent

SCOPE_SOURCE = "nexus_sec_positive_chain_scope"
MEMBER_SOURCE = "nexus_sec_positive_chain_member"
REVOKE_SOURCE = "nexus_sec_positive_chain_revoke"
POLICY: Literal["sec_positive_chain_scope_v1"] = "sec_positive_chain_scope_v1"
MEMBER_SCHEMA: Literal["sec_positive_chain_member_v1"] = "sec_positive_chain_member_v1"
REVOKE_SCHEMA: Literal["sec_positive_chain_revoke_v1"] = "sec_positive_chain_revoke_v1"
SCOPE_ACTION = "research.monitor.positive_chain_scope_enabled"
MEMBER_ACTION = "research.monitor.positive_chain_member_registered"
REVOKE_ACTION = "research.monitor.positive_chain_scope_revoked"
INVALID = "SEC_POSITIVE_CHAIN_SCOPE_INVALID"


class PositiveChainScope(RecentPositivePolicy):
    policy: Literal["sec_positive_chain_scope_v1"] = POLICY  # type: ignore[assignment]
    authorization_scope: Literal["single_c1_seen_additions_7d_v1"] = (
        "single_c1_seen_additions_7d_v1"  # type: ignore[assignment]
    )
    anchor_policy_snapshot_id: UUID
    anchor_policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    expires_at: datetime

    @field_validator("expires_at")
    @classmethod
    def expiry_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class PositiveChainMember(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    policy: Literal["sec_positive_chain_member_v1"] = MEMBER_SCHEMA
    scope_snapshot_id: UUID
    scope_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_id: UUID
    account_id: UUID
    instrument_id: UUID
    source_run_id: UUID
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    condition_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    own_origin_id: UUID
    own_origin_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_origin_id: UUID
    root_origin_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    comparison_snapshot_id: UUID
    comparison_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    issuer_cik: str = Field(pattern=r"^[0-9]{10}$")
    effective_at: datetime

    @field_validator("effective_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


class ChainRevocation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    policy: Literal["sec_positive_chain_revoke_v1"] = REVOKE_SCHEMA
    scope_snapshot_id: UUID
    scope_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    account_id: UUID
    instrument_id: UUID
    effective_at: datetime

    @field_validator("effective_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


class ChainPositiveObservation(RecentPositiveObservation):
    policy: Literal["sec_positive_chain_scope_v1"] = POLICY  # type: ignore[assignment]
    scope_snapshot_id: UUID
    scope_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


async def _visible(
    session: AsyncSession, source: str, object_id: UUID, now: datetime
) -> tuple[DataSnapshot, ...]:
    return tuple(
        await session.scalars(
            select(DataSnapshot)
            .where(
                DataSnapshot.source == source,
                DataSnapshot.provider_object_id == str(object_id),
                DataSnapshot.created_at <= now,
            )
            .limit(2)
        )
    )


async def _verify(
    session: AsyncSession,
    row: DataSnapshot,
    value: PositiveChainScope | PositiveChainMember | ChainRevocation,
    source: str,
    action: str,
    now: datetime,
) -> None:
    if (
        row.source != source
        or row.schema_version != value.policy
        or row.revision != sha256(value.canonical()).hexdigest()
        or value.effective_at > now
        or any(
            at != value.effective_at
            for at in (row.created_at, row.observed_at, row.available_at, row.received_at)
        )
    ):
        raise ValueError(INVALID)
    audits = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action == action,
                AuditEvent.payload["snapshot_id"].astext == str(row.id),
                AuditEvent.occurred_at <= now,
            )
            .limit(2)
        )
    )
    expected = {
        "snapshot_id": str(row.id),
        "policy_hash": row.revision,
        **value.model_dump(mode="json"),
    }
    if (
        len(audits) != 1
        or audits[0].actor_id != "research-monitor"
        or (
            audits[0].schema_version != value.policy
            or audits[0].occurred_at != value.effective_at
            or audits[0].payload != expected
            or audits[0].created_at > now
        )
    ):
        raise ValueError(INVALID)


async def _scope(session: AsyncSession, row: DataSnapshot, now: datetime) -> PositiveChainScope:
    value = PositiveChainScope.model_validate(row.payload)
    await _verify(session, row, value, SCOPE_SOURCE, SCOPE_ACTION, now)
    anchor = await session.get(MonitorPlan, value.plan_id)
    if anchor is None or row.provider_object_id != str(anchor.id):
        raise ValueError(INVALID)
    original = await read_policy(session, anchor, value.effective_at)
    if original is None or (original[0].id, original[0].revision) != (
        value.anchor_policy_snapshot_id,
        value.anchor_policy_hash,
    ):
        raise ValueError(INVALID)
    copied = value.model_dump(
        exclude={
            "policy",
            "authorization_scope",
            "anchor_policy_snapshot_id",
            "anchor_policy_hash",
            "expires_at",
            "effective_at",
        }
    )
    previous = original[1].model_dump(exclude={"policy", "authorization_scope", "effective_at"})
    if copied != previous or value.expires_at != value.effective_at + timedelta(days=7):
        raise ValueError(INVALID)
    return value


async def _active(
    session: AsyncSession, row: DataSnapshot, scope: PositiveChainScope, now: datetime
) -> bool:
    revocations = await _visible(session, REVOKE_SOURCE, row.id, now)
    if len(revocations) > 1:
        raise ValueError(INVALID)
    for revoked in revocations:
        value = ChainRevocation.model_validate(revoked.payload)
        await _verify(session, revoked, value, REVOKE_SOURCE, REVOKE_ACTION, now)
        if (value.scope_snapshot_id, value.scope_hash, value.account_id, value.instrument_id) != (
            row.id,
            row.revision,
            scope.account_id,
            scope.instrument_id,
        ) or value.effective_at < scope.effective_at:
            raise ValueError(INVALID)
    return scope.effective_at <= now < scope.expires_at and not revocations


async def _descends(
    session: AsyncSession, plan: MonitorPlan, scope: PositiveChainScope, now: datetime
) -> bool:
    current = plan
    seen: set[UUID] = set()
    while True:
        if current.id in seen or max(current.created_at, current.effective_at) > now:
            raise ValueError(INVALID)
        seen.add(current.id)
        if (current.account_id, current.instrument_id, current.kind, current.condition_hash) != (
            scope.account_id,
            scope.instrument_id,
            "disclosure",
            scope.condition_hash,
        ):
            return False
        if current.id == scope.plan_id:
            return True
        if current.predecessor_id is None:
            return False
        prior = await session.get(MonitorPlan, current.predecessor_id)
        if prior is None or current.generation != prior.generation + 1:
            raise ValueError(INVALID)
        current = prior


async def _binding_values(
    session: AsyncSession, plan: MonitorPlan, now: datetime
) -> dict[str, Any]:
    binding = await read_binding(session, plan, now)
    if binding is None:
        raise ValueError(INVALID)
    own, root, _, origin = binding
    from nexus.monitoring.recent_positive_policy import _issuer

    return dict(
        plan_id=plan.id,
        account_id=plan.account_id,
        instrument_id=plan.instrument_id,
        source_run_id=plan.source_run_id,
        source_hash=plan.source_hash,
        condition_hash=plan.condition_hash,
        own_origin_id=own.id,
        own_origin_hash=own.revision,
        root_origin_id=root.id,
        root_origin_hash=root.revision,
        comparison_snapshot_id=origin.capture_id,
        comparison_hash=origin.capture_hash,
        issuer_cik=await _issuer(session, plan, now),
    )


async def _plan_active_at(session: AsyncSession, plan: MonitorPlan, now: datetime) -> bool:
    if not max(plan.created_at, plan.effective_at) <= now < plan.valid_until:
        return False
    replacement = await session.scalar(
        select(MonitorPlan.id)
        .where(
            MonitorPlan.account_id == plan.account_id,
            MonitorPlan.instrument_id == plan.instrument_id,
            MonitorPlan.kind == plan.kind,
            MonitorPlan.generation > plan.generation,
            MonitorPlan.created_at <= now,
            MonitorPlan.effective_at <= now,
        )
        .limit(1)
    )
    return replacement is None


async def read_member(
    session: AsyncSession, plan: MonitorPlan, now: datetime
) -> tuple[DataSnapshot, PositiveChainMember] | None:
    """Select historical visible membership, never install or extend it on a read."""
    now = require_utc(now)
    members = await _visible(session, MEMBER_SOURCE, plan.id, now)
    if len(members) > 1:
        raise ValueError(INVALID)
    if members:
        row = members[0]
        member = PositiveChainMember.model_validate(row.payload)
        await _verify(session, row, member, MEMBER_SOURCE, MEMBER_ACTION, now)
        scope_row = await session.get(DataSnapshot, member.scope_snapshot_id)
        if scope_row is None or scope_row.revision != member.scope_hash:
            raise ValueError(INVALID)
        scope = await _scope(session, scope_row, now)
        if not await _descends(session, plan, scope, now):
            raise ValueError(INVALID)
        expected = await _binding_values(session, plan, now)
        if any(getattr(member, key) != value for key, value in expected.items()) or (
            member.root_origin_id != scope.root_origin_id
            or member.root_origin_hash != scope.root_origin_hash
            or member.comparison_snapshot_id != scope.comparison_snapshot_id
            or member.comparison_hash != scope.comparison_hash
            or member.issuer_cik != scope.issuer_cik
            or not scope.effective_at <= member.effective_at < scope.expires_at
            or not max(plan.created_at, plan.effective_at) <= member.effective_at < plan.valid_until
        ):
            raise ValueError(INVALID)
        return (
            (row, member)
            if (
                await _active(session, scope_row, scope, now)
                and await _plan_active_at(session, plan, now)
            )
            else None
        )
    # An active applicable scope with a missing registration is unknown, not permission.
    scopes = tuple(
        await session.scalars(
            select(DataSnapshot).where(
                DataSnapshot.source == SCOPE_SOURCE,
                DataSnapshot.payload["account_id"].astext == str(plan.account_id),
                DataSnapshot.payload["instrument_id"].astext == str(plan.instrument_id),
                DataSnapshot.created_at <= now,
            )
        )
    )
    for row in scopes:
        scope = await _scope(session, row, now)
        if (
            await _descends(session, plan, scope, now)
            and await _active(session, row, scope, now)
            and await _plan_active_at(session, plan, now)
        ):
            raise ValueError(INVALID)
    return None


async def _clock(session: AsyncSession, *, plan: MonitorPlan | None = None) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError(INVALID)
    now = require_utc(now)
    if plan is not None:
        last = await session.scalar(
            select(func.max(MonitorCheck.checked_at)).where(MonitorCheck.plan_id == plan.id)
        )
        if last is not None and now <= last:
            raise ValueError(INVALID)
    return now


async def _append(
    session: AsyncSession,
    value: PositiveChainScope | PositiveChainMember | ChainRevocation,
    *,
    source: str,
    object_id: UUID,
    action: str,
) -> DataSnapshot:
    row = DataSnapshot(
        source=source,
        provider_object_id=str(object_id),
        revision=sha256(value.canonical()).hexdigest(),
        schema_version=value.policy,
        created_at=value.effective_at,
        observed_at=value.effective_at,
        available_at=value.effective_at,
        received_at=value.effective_at,
        payload=value.model_dump(mode="json"),
    )
    session.add(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action=action,
        schema_version=value.policy,
        occurred_at=value.effective_at,
        payload={
            "snapshot_id": str(row.id),
            "policy_hash": row.revision,
            **value.model_dump(mode="json"),
        },
    )
    return row


async def _current(session: AsyncSession, plan: MonitorPlan, now: datetime) -> None:
    from nexus.monitoring.batches import disclosure_source_current
    from nexus.monitoring.source_qualification import load_qualified_source

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
    await load_qualified_source(
        session, account_id=plan.account_id, output_id=plan.source_output_id, now=now
    )
    installed = await session.scalar(
        select(AuditEvent.id)
        .where(
            AuditEvent.action == "research.monitor.disclosure_plan_installed",
            AuditEvent.payload["plan_id"].astext == str(plan.id),
            AuditEvent.payload["source_output_id"].astext == str(plan.source_output_id),
            AuditEvent.occurred_at <= now,
        )
        .limit(1)
    )
    if installed is None:
        raise ValueError(INVALID)


async def enable_scope(
    session: AsyncSession, *, account_id: UUID, plan_id: UUID, now: datetime
) -> DataSnapshot:
    """One explicit anchor grant, immutable expiry; existing grants never renew."""
    plan = await session.get(MonitorPlan, plan_id)
    if plan is None or plan.account_id != account_id or plan.kind != "disclosure":
        raise ValueError(INVALID)
    await lock_security(session, account_id=account_id, instrument_id=plan.instrument_id)
    now = await _clock(session)
    existing = await _visible(session, SCOPE_SOURCE, plan.id, now)
    if existing:
        if len(existing) != 1:
            raise ValueError(INVALID)
        await _scope(session, existing[0], now)
        return existing[0]
    original = await read_policy(session, plan, now)
    if original is None:
        raise ValueError(INVALID)
    current = await session.scalar(
        select(MonitorPlan)
        .where(
            MonitorPlan.account_id == account_id,
            MonitorPlan.instrument_id == plan.instrument_id,
            MonitorPlan.kind == "disclosure",
        )
        .order_by(MonitorPlan.generation.desc())
        .limit(1)
    )
    if current is None:
        raise ValueError(INVALID)
    await _current(session, current, now)
    row, previous = original
    effective = await _clock(session, plan=current)
    await _current(session, current, effective)
    value = PositiveChainScope(
        **previous.model_dump(exclude={"policy", "authorization_scope", "effective_at"}),
        effective_at=effective,
        expires_at=effective + timedelta(days=7),
        anchor_policy_snapshot_id=row.id,
        anchor_policy_hash=row.revision,
    )
    if not await _descends(session, current, value, effective):
        raise ValueError(INVALID)
    bindings = await _binding_values(session, current, effective)
    if any(
        bindings[key] != getattr(value, key)
        for key in (
            "root_origin_id",
            "root_origin_hash",
            "comparison_snapshot_id",
            "comparison_hash",
            "issuer_cik",
        )
    ):
        raise ValueError(INVALID)
    result = await _append(
        session, value, source=SCOPE_SOURCE, object_id=plan.id, action=SCOPE_ACTION
    )
    await ensure_member(session, plan=current, now=effective)
    return result


async def ensure_member(
    session: AsyncSession, *, plan: MonitorPlan, now: datetime
) -> DataSnapshot | None:
    """Normal installation or explicit ops only, under the shared security lock."""
    await lock_security(session, account_id=plan.account_id, instrument_id=plan.instrument_id)
    scopes = tuple(
        await session.scalars(
            select(DataSnapshot).where(
                DataSnapshot.source == SCOPE_SOURCE,
                DataSnapshot.payload["account_id"].astext == str(plan.account_id),
                DataSnapshot.payload["instrument_id"].astext == str(plan.instrument_id),
            )
        )
    )
    if not scopes:
        return None
    now = await _clock(session, plan=plan)
    existing = await _visible(session, MEMBER_SOURCE, plan.id, now)
    if existing:
        await read_member(session, plan, now)
        return existing[0]
    selected = []
    for row in scopes:
        value = await _scope(session, row, now)
        if await _active(session, row, value, now) and await _descends(session, plan, value, now):
            selected.append((row, value))
    if not selected:
        return None
    if len(selected) != 1:
        raise ValueError(INVALID)
    row, scope = selected[0]
    await _current(session, plan, now)
    bindings = await _binding_values(session, plan, now)
    if any(
        bindings[key] != getattr(scope, key)
        for key in (
            "root_origin_id",
            "root_origin_hash",
            "comparison_snapshot_id",
            "comparison_hash",
            "issuer_cik",
        )
    ):
        raise ValueError(INVALID)
    member_value = PositiveChainMember(
        **bindings, scope_snapshot_id=row.id, scope_hash=row.revision, effective_at=now
    )
    return await _append(
        session, member_value, source=MEMBER_SOURCE, object_id=plan.id, action=MEMBER_ACTION
    )


async def revoke_scope(
    session: AsyncSession, *, account_id: UUID, plan_id: UUID, now: datetime
) -> DataSnapshot:
    plan = await session.get(MonitorPlan, plan_id)
    if plan is None or plan.account_id != account_id:
        raise ValueError(INVALID)
    await lock_security(session, account_id=account_id, instrument_id=plan.instrument_id)
    now = await _clock(session)
    rows = await _visible(session, SCOPE_SOURCE, plan.id, now)
    if len(rows) != 1:
        raise ValueError(INVALID)
    row = rows[0]
    scope = await _scope(session, row, now)
    existing = await _visible(session, REVOKE_SOURCE, row.id, now)
    await _active(session, row, scope, now)
    if existing:
        return existing[0]
    member_plans = select(DataSnapshot.payload["plan_id"].astext).where(
        DataSnapshot.source == MEMBER_SOURCE,
        DataSnapshot.payload["scope_snapshot_id"].astext == str(row.id),
    )
    from sqlalchemy import String, cast

    last = await session.scalar(
        select(func.max(MonitorCheck.checked_at)).where(
            cast(MonitorCheck.plan_id, String).in_(member_plans)
        )
    )
    if last is not None and now <= last:
        raise ValueError(INVALID)
    value = ChainRevocation(
        scope_snapshot_id=row.id,
        scope_hash=row.revision,
        account_id=account_id,
        instrument_id=plan.instrument_id,
        effective_at=now,
    )
    return await _append(
        session, value, source=REVOKE_SOURCE, object_id=row.id, action=REVOKE_ACTION
    )
