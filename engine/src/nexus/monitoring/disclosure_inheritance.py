"""Atomic successor custody of an unchanged SEC forward comparison origin."""

import json
from datetime import datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.monitoring.disclosure_forward import POLICY, SOURCE, ForwardOrigin, _origins
from nexus.monitoring.disclosure_observations import _bound_proposal
from nexus.monitoring.storage import plan_recorded_at
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.research.sec_listed_storage import SCHEMA as CAPTURE_SCHEMA
from nexus.research.sec_listed_storage import SOURCE as CAPTURE_SOURCE
from nexus.research.sec_listed_storage import ListedCapture

INHERITANCE: Literal["sec_forward_inheritance_v1"] = "sec_forward_inheritance_v1"
INVALID = "SEC_INDEX_FORWARD_ORIGIN_INVALID"


class ForwardInheritance(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    policy: Literal["sec_forward_inheritance_v1"] = INHERITANCE
    plan_id: UUID
    account_id: UUID
    instrument_id: UUID
    source_hash: str
    condition_hash: str
    baseline_snapshot_id: UUID
    baseline_hash: str
    baseline_as_of: datetime
    issuer_cik: str
    predecessor_plan_id: UUID
    predecessor_origin_id: UUID
    predecessor_origin_hash: str
    root_origin_id: UUID
    root_origin_hash: str
    effective_at: datetime

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


async def read_binding(
    session: AsyncSession, plan: MonitorPlan, now: datetime
) -> tuple[DataSnapshot, DataSnapshot, MonitorPlan, ForwardOrigin] | None:
    """Verify saved custody only; API reads do not fetch archives or create origins."""
    rows = await _origins(session, plan, now)
    if not rows:
        # A missing link is not permission to bootstrap or fall back to old false.
        prior_id = plan.predecessor_id
        visited = {plan.id}
        while prior_id is not None:
            if prior_id in visited:
                raise ValueError(INVALID)
            visited.add(prior_id)
            prior = await session.get(MonitorPlan, prior_id)
            if prior is None or await _origins(session, prior, now):
                raise ValueError(INVALID)
            prior_id = prior.predecessor_id
        return None
    own = rows[0]
    current = plan
    root_binding: tuple[UUID, str] | None = None
    seen: set[UUID] = set()
    cutoff = now
    while True:
        if len(rows) != 1 or current.id in seen:
            raise ValueError(INVALID)
        seen.add(current.id)
        row = rows[0]
        value = (
            ForwardOrigin.model_validate(row.payload)
            if row.schema_version == POLICY
            else ForwardInheritance.model_validate(row.payload)
            if row.schema_version == INHERITANCE
            else None
        )
        proposal = await _bound_proposal(session, current, cutoff)
        if value is None or (
            row.revision != sha256(value.canonical()).hexdigest()
            or row.provider_object_id != str(current.id)
            or (
                value.plan_id,
                value.account_id,
                value.instrument_id,
                value.source_hash,
                value.condition_hash,
            )
            != (
                current.id,
                current.account_id,
                current.instrument_id,
                current.source_hash,
                current.condition_hash,
            )
            or (value.baseline_snapshot_id, value.baseline_hash, value.baseline_as_of)
            != (proposal.baseline_snapshot_id, proposal.baseline_hash, proposal.baseline_as_of)
            or any(
                t != value.effective_at
                for t in (row.created_at, row.observed_at, row.available_at, row.received_at)
            )
            or not max(current.effective_at, current.created_at)
            <= value.effective_at
            < current.valid_until
            or value.effective_at > cutoff
        ):
            raise ValueError(INVALID)
        if isinstance(value, ForwardOrigin):
            if root_binding is not None and root_binding != (row.id, row.revision):
                raise ValueError(INVALID)
            capture_row = await session.get(DataSnapshot, value.capture_id)
            if capture_row is None or (
                capture_row.source != CAPTURE_SOURCE
                or capture_row.schema_version != CAPTURE_SCHEMA
                or capture_row.revision != value.capture_hash
                or max(
                    capture_row.created_at,
                    capture_row.observed_at,
                    capture_row.available_at,
                    capture_row.received_at,
                )
                > value.effective_at
            ):
                raise ValueError(INVALID)
            capture = ListedCapture.model_validate(capture_row.payload)
            if (
                sha256(capture.canonical()).hexdigest() != value.capture_hash
                or tuple(x.sha256 for x in capture.originals) != value.original_hashes
                or (capture.index.instrument_id, capture.index.issuer_cik)
                != (current.instrument_id, proposal.issuer_cik)
            ):
                raise ValueError(INVALID)
            return own, row, current, value
        if value.issuer_cik != proposal.issuer_cik:
            raise ValueError(INVALID)
        root = (value.root_origin_id, value.root_origin_hash)
        if root_binding is not None and root != root_binding:
            raise ValueError(INVALID)
        root_binding = root
        prior = await session.get(MonitorPlan, value.predecessor_plan_id)
        if prior is None or (
            current.predecessor_id != prior.id
            or current.generation != prior.generation + 1
            or (current.account_id, current.instrument_id, current.kind, current.condition_hash)
            != (prior.account_id, prior.instrument_id, prior.kind, prior.condition_hash)
        ):
            raise ValueError(INVALID)
        prior_proposal = await _bound_proposal(session, prior, value.effective_at)
        if prior_proposal.issuer_cik != proposal.issuer_cik:
            raise ValueError(INVALID)
        rows = await _origins(session, prior, value.effective_at)
        if len(rows) != 1 or (rows[0].id, rows[0].revision) != (
            value.predecessor_origin_id,
            value.predecessor_origin_hash,
        ):
            raise ValueError(INVALID)
        current, cutoff = prior, value.effective_at


async def inherit_origin(
    session: AsyncSession, *, plan: MonitorPlan, prior: MonitorPlan | None, now: datetime
) -> None:
    """Called inside the normal installation transaction under its security lock."""
    if prior is None:
        return
    from nexus.monitoring.disclosure_forward import read_origin

    published = await plan_recorded_at(session, requested_at=now)
    original = await read_origin(session, prior, published)
    if original is None:
        return
    binding = await read_binding(session, prior, published)
    if binding is None:
        raise ValueError(INVALID)
    previous, root, _, basis = binding
    proposal = await _bound_proposal(session, plan, published)
    prior_proposal = await _bound_proposal(session, prior, published)
    if (
        plan.predecessor_id != prior.id
        or plan.generation != prior.generation + 1
        or (
            plan.account_id,
            plan.instrument_id,
            plan.kind,
            plan.condition_hash,
            proposal.issuer_cik,
        )
        != (
            prior.account_id,
            prior.instrument_id,
            prior.kind,
            prior.condition_hash,
            prior_proposal.issuer_cik,
        )
        or not plan.effective_at <= published < plan.valid_until
    ):
        raise ValueError(INVALID)
    value = ForwardInheritance(
        plan_id=plan.id,
        account_id=plan.account_id,
        instrument_id=plan.instrument_id,
        source_hash=plan.source_hash,
        condition_hash=plan.condition_hash,
        baseline_snapshot_id=proposal.baseline_snapshot_id,
        baseline_hash=proposal.baseline_hash,
        baseline_as_of=proposal.baseline_as_of,
        issuer_cik=proposal.issuer_cik,
        predecessor_plan_id=prior.id,
        predecessor_origin_id=previous.id,
        predecessor_origin_hash=previous.revision,
        root_origin_id=root.id,
        root_origin_hash=root.revision,
        effective_at=published,
    )
    row = DataSnapshot(
        created_at=published,
        source=SOURCE,
        provider_object_id=str(plan.id),
        revision=sha256(value.canonical()).hexdigest(),
        schema_version=INHERITANCE,
        observed_at=published,
        received_at=published,
        available_at=published,
        payload=value.model_dump(mode="json"),
    )
    session.add(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.forward_origin_inherited",
        schema_version=INHERITANCE,
        occurred_at=published,
        payload={
            "plan_id": str(plan.id),
            "origin_id": str(row.id),
            "origin_hash": row.revision,
            "predecessor_origin_id": str(previous.id),
            "root_origin_id": str(root.id),
            "comparison_from": basis.effective_at.isoformat(),
            "old_history_gap": "unknown",
        },
    )
