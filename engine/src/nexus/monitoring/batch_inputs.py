"""Freeze exact parent theses and filing originals before mixed job admission.

A material package grants no budget, lease, model, accounting or execution authority.
"""

import json
from datetime import datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.filing_archive import SOURCE as FILING_SOURCE
from nexus.monitoring.filing_archive import VerifiedFilingContent, read_filing_content
from nexus.monitoring.storage import lock_security
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorObservationBatch,
    MonitorPlan,
    MonitorTrigger,
)

SOURCE = "nexus_monitor_batch_inputs"
SCHEMA: Literal["monitor_batch_inputs_v1"] = "monitor_batch_inputs_v1"


class BatchInputs(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["monitor_batch_inputs_v1"] = SCHEMA
    account_id: UUID
    instrument_id: UUID
    batch_id: UUID
    prepared_at: datetime
    parents: tuple[dict[str, object], ...] = Field(min_length=1, max_length=2)
    members: tuple[dict[str, object], ...] = Field(min_length=1, max_length=32)
    contents: tuple[VerifiedFilingContent, ...] = Field(max_length=32)
    impact_review: Literal["not_performed"] = "not_performed"
    execution_locked: Literal[True] = True

    def canonical(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


class MaterialGap(ValueError):
    def __init__(self, reason: str, missing: int = 0, event_ids: tuple[str, ...] = ()):
        super().__init__(reason)
        self.missing = missing
        self.event_ids = event_ids


async def _inputs(
    session: AsyncSession,
    batch: MonitorObservationBatch,
    now: datetime,
    *,
    current_eligibility: bool = True,
) -> BatchInputs:
    from nexus.monitoring.batches import disclosure_source_current
    from nexus.monitoring.disclosure_observations import read_disclosure_observation
    from nexus.monitoring.reconciliation import plan_covered
    from nexus.monitoring.source_qualification import load_qualified_source
    from nexus.research.sec_disclosure_index import SecIndexEntry

    readonly = await session.scalar(text("SELECT current_setting('transaction_read_only')"))
    validator = (
        "nexus_monitor_validate_batch_readonly"
        if readonly == "on"
        else "nexus_monitor_validate_batch"
    )
    await session.execute(text(f"SELECT {validator}(:id)"), {"id": batch.id})
    if not 1 <= len(batch.trigger_ids) <= 32:
        raise MaterialGap("MONITOR_PENDING_OVERFLOW")
    if max(batch.created_at, batch.sealed_at, batch.checked_at) > now:
        raise MaterialGap("MONITOR_BATCH_NOT_VISIBLE")
    members: list[dict[str, object]] = []
    parents = {}
    contents = []
    missing: list[str] = []
    for trigger_id in batch.trigger_ids:
        trigger = await session.get(MonitorTrigger, UUID(trigger_id))
        check = None if trigger is None else await session.get(MonitorCheck, trigger.check_id)
        plan = None if check is None else await session.get(MonitorPlan, check.plan_id)
        if (
            trigger is None
            or check is None
            or plan is None
            or trigger.batch_id != batch.id
            or check.result != "true"
            or trigger.account_id != batch.account_id
            or trigger.instrument_id != batch.instrument_id
            or max(trigger.created_at, check.created_at, check.checked_at) > now
        ):
            raise MaterialGap("MONITOR_BATCH_MEMBERS_INVALID")
        if not plan.effective_at <= now < plan.valid_until:
            raise MaterialGap("MONITOR_AWAITING_RECONCILIATION")
        if current_eligibility:
            latest = await session.scalar(
                select(MonitorPlan.id)
                .where(
                    MonitorPlan.account_id == batch.account_id,
                    MonitorPlan.instrument_id == batch.instrument_id,
                    MonitorPlan.kind == plan.kind,
                )
                .order_by(MonitorPlan.generation.desc())
                .limit(1)
            )
            current = await (
                plan_covered(session, plan=plan, now=now)
                if plan.kind == "price"
                else disclosure_source_current(session, plan, now)
            )
            if latest != plan.id or not plan.effective_at <= now < plan.valid_until or not current:
                raise MaterialGap("MONITOR_AWAITING_RECONCILIATION")
        if plan.id not in parents:
            qualified = await load_qualified_source(
                session, account_id=batch.account_id, output_id=plan.source_output_id, now=now
            )
            if (
                qualified.run.id != plan.source_run_id
                or qualified.output.content_hash != plan.source_hash
            ):
                raise MaterialGap("MONITOR_SOURCE_INVALID")
            parents[plan.id] = {
                "kind": plan.kind,
                "plan_id": str(plan.id),
                "generation": plan.generation,
                "run_id": str(qualified.run.id),
                "output_id": str(qualified.output.id),
                "output_hash": qualified.output.content_hash,
                "intent": qualified.intent.model_dump(mode="json"),
                "role_outputs": [row.model_dump(mode="json") for row in qualified.role_outputs],
            }
        members.append(
            {
                "trigger_id": str(trigger.id),
                "check_id": str(check.id),
                "plan_id": str(plan.id),
                "kind": trigger.kind,
                "event_key": trigger.event_key,
                "cause": trigger.cause,
                "checked_at": check.checked_at.isoformat(),
                "observation": check.observation,
            }
        )
        if trigger.kind == "price":
            from nexus.monitoring.contracts import PriceObservation, PriceReviewCondition
            from nexus.monitoring.evaluation import evaluate_price
            from nexus.monitoring.observations import read_latest_price

            quote = PriceObservation.model_validate(check.observation)
            condition = PriceReviewCondition.model_validate(plan.condition)
            if (
                condition.semantic_hash != plan.condition_hash
                or trigger.condition_hash != plan.condition_hash
                or evaluate_price(
                    condition, quote, instrument_id=batch.instrument_id, now=check.checked_at
                ).result
                != "true"
                or await read_latest_price(
                    session, instrument_id=batch.instrument_id, now=check.checked_at
                )
                != quote
            ):
                raise MaterialGap("MONITOR_CONTEXT_INVALID")
            continue
        if not isinstance(check.observation, dict):
            raise MaterialGap("MONITOR_CONTEXT_INVALID")
        entry = SecIndexEntry.model_validate(check.observation["entry"])
        observation = await read_disclosure_observation(session, plan=plan, now=check.checked_at)
        if (
            observation.result != "true"
            or entry not in observation.added
            or trigger.event_key != f"sec:{observation.issuer_cik}:{entry.accession_number}"
            or {
                **{k: v for k, v in observation.model_dump(mode="json").items() if k != "added"},
                "entry": entry.model_dump(mode="json"),
            }
            != check.observation
        ):
            raise MaterialGap("MONITOR_CONTEXT_INVALID")
        candidates = tuple(
            await session.scalars(
                select(DataSnapshot)
                .where(
                    DataSnapshot.source == FILING_SOURCE,
                    DataSnapshot.provider_object_id
                    == f"{observation.issuer_cik}:{entry.accession_number}",
                    DataSnapshot.created_at <= now,
                    DataSnapshot.available_at <= now,
                    DataSnapshot.received_at <= now,
                )
                .order_by(DataSnapshot.created_at, DataSnapshot.id)
                .limit(33)
            )
        )
        if not candidates:
            missing.append(trigger.event_key)
            continue
        if len(candidates) > 32:
            raise MaterialGap("MONITOR_CONTENT_CONFLICT")
        valid = []
        for candidate in candidates:
            content = await read_filing_content(session, snapshot_id=candidate.id, as_of=now)
            if (
                content is None
                or content.archive.instrument_id != batch.instrument_id
                or content.archive.issuer_cik != observation.issuer_cik
                or content.archive.entry != entry
            ):
                raise MaterialGap("MONITOR_CONTENT_INVALID")
            valid.append(content)
        identities = {
            tuple((d.filename, d.document_type, d.raw_hash) for d in row.archive.documents)
            for row in valid
        }
        if len(identities) != 1:
            raise MaterialGap("MONITOR_CONTENT_CONFLICT")
        contents.append(valid[0])
    if missing:
        raise MaterialGap("MONITOR_CONTENT_MISSING", len(missing), tuple(sorted(missing)))
    return BatchInputs(
        account_id=batch.account_id,
        instrument_id=batch.instrument_id,
        batch_id=batch.id,
        prepared_at=now,
        parents=tuple(parents[key] for key in sorted(parents, key=str)),
        members=tuple(members),
        contents=tuple(sorted(contents, key=lambda row: row.archive.entry.accession_number)),
    )


async def prepare_batch_inputs(
    session: AsyncSession, *, account_id: UUID, batch_id: UUID, now: datetime
) -> dict[str, object]:
    now = require_utc(now)
    batch = await session.get(MonitorObservationBatch, batch_id)
    if batch is None or batch.account_id != account_id:
        raise ValueError("MONITOR_BATCH_UNAVAILABLE")
    await lock_security(session, account_id=account_id, instrument_id=batch.instrument_id)
    fixed = {
        "batch_id": str(batch.id),
        "model_requests": 0,
        "provider_requests": 0,
        "job_created": False,
        "execution_locked": True,
        "admission": "not_checked",
        "impact_review": "not_performed",
    }
    prior = await session.scalar(
        select(DataSnapshot)
        .where(DataSnapshot.source == SOURCE, DataSnapshot.provider_object_id == str(batch.id))
        .order_by(DataSnapshot.created_at, DataSnapshot.id)
        .limit(1)
    )
    if prior is not None:
        value = await read_batch_inputs(session, snapshot_id=prior.id, as_of=now)
        if value is None:
            return {**fixed, "status": "blocked", "reason": "MONITOR_PREPARED_INPUTS_INVALID"}
        try:
            current = await _inputs(session, batch, now)
        except MaterialGap as error:
            return {
                **fixed,
                "status": "blocked",
                "reason": str(error),
                "missing_content": error.missing,
                "missing_event_ids": list(error.event_ids),
            }
        if (current.parents, current.members, current.contents) != (
            value.parents,
            value.members,
            value.contents,
        ):
            return {**fixed, "status": "blocked", "reason": "MONITOR_PREPARED_INPUTS_CHANGED"}
        return {
            **fixed,
            "status": "prepared",
            "snapshot_id": str(prior.id),
            "trigger_count": len(value.members),
            "content_count": len(value.contents),
        }
    try:
        value = await _inputs(session, batch, now)
    except MaterialGap as error:
        return {
            **fixed,
            "status": "blocked",
            "reason": str(error),
            "missing_content": error.missing,
            "missing_event_ids": list(error.event_ids),
        }
    raw = value.canonical()
    # Freeze all verified members or none. Oversize is explicit, never truncation.
    if len(raw) > 256 * 1024:
        return {**fixed, "status": "blocked", "reason": "MONITOR_CONTENT_CAPACITY"}
    published = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(published, datetime) or published < now:
        raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
    row = DataSnapshot(
        created_at=published,
        source=SOURCE,
        provider_object_id=str(batch.id),
        revision=sha256(raw).hexdigest(),
        observed_at=now,
        available_at=published,
        received_at=published,
        schema_version=SCHEMA,
        payload=value.model_dump(mode="json"),
    )
    session.add(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.batch_inputs_prepared",
        schema_version=SCHEMA,
        occurred_at=published,
        payload={
            "batch_id": str(batch.id),
            "snapshot_id": str(row.id),
            "content_hash": row.revision,
        },
    )
    return {
        **fixed,
        "status": "prepared",
        "snapshot_id": str(row.id),
        "trigger_count": len(value.members),
        "content_count": len(value.contents),
    }


async def read_batch_inputs(
    session: AsyncSession, *, snapshot_id: UUID, as_of: datetime
) -> BatchInputs | None:
    as_of = require_utc(as_of)
    row = await session.get(DataSnapshot, snapshot_id)
    if (
        row is None
        or row.source != SOURCE
        or row.schema_version != SCHEMA
        or max(row.created_at, row.observed_at, row.available_at, row.received_at) > as_of
    ):
        return None
    try:
        original = BatchInputs.model_validate_json(json.dumps(row.payload))
        if (
            row.revision != sha256(original.canonical()).hexdigest()
            or row.provider_object_id != str(original.batch_id)
            or original.prepared_at != row.observed_at
        ):
            return None
        batch = await session.get(MonitorObservationBatch, original.batch_id)
        if (
            batch is None
            or batch.account_id != original.account_id
            or batch.instrument_id != original.instrument_id
        ):
            return None
        rebuilt = await _inputs(session, batch, original.prepared_at, current_eligibility=False)
        return original if rebuilt == original else None
    except (MaterialGap, ValidationError, ValueError, TypeError, KeyError):
        return None
