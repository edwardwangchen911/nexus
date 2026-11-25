"""Research-only SEC bridge through the existing typed batch/native research path."""

import json
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any
from uuid import UUID

from sqlalchemy import exists, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.monitoring.durable_sec_storage import _lock, _time, rule_state
from nexus.monitoring.source_qualification import load_qualified_source
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorIncrement,
    MonitorObservationBatch,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.sec_conflicts import SecDisclosureKeyConflict
from nexus.persistence.models.sec_monitor import (
    SecDiscoveryCheck,
    SecDiscoveryEvent,
    SecDiscoveryEventRevision,
    SecMonitorRule,
)
from nexus.persistence.models.sec_research import (
    SecEventConsumptionReceipt,
    SecEventResearchBinding,
)
from nexus.research.sec_disclosure_index import SecIndexEntry
from nexus.research.sec_exhibit_storage import configured_exhibit_store

POLICY = "durable_sec_research_binding_v1"
ROUTING = "durable_research_v2"
INVALID = "DURABLE_SEC_RESEARCH_EVIDENCE_INVALID"
_CAPTURE_MAX_AGE = timedelta(seconds=300)


class CurrentSecCaptureExpired(ValueError):
    """The latest verified capture still has the exact event but is too old."""

    def __init__(self, observation: dict[str, object], *, capture_id: UUID, rule_id: UUID) -> None:
        super().__init__(INVALID)
        self.observation = observation
        self.capture_id = capture_id
        self.rule_id = rule_id


def verify_event_evidence(event: Any, capture: Any, *, now: datetime) -> SecIndexEntry:
    """A fresh independent source must reproduce the exact saved event; no set difference."""
    now = require_utc(now)
    if isinstance(event, SecDiscoveryEvent) and event.observation_receipt_id is not None:
        raise ValueError(INVALID)
    entry = SecIndexEntry.model_validate(event.entry)
    current = capture.index
    if (
        current.instrument_id != event.instrument_id
        or event.event_key != f"sec:{current.issuer_cik}:{entry.accession_number}"
        or not timedelta(0) <= now - require_utc(current.received_at) <= _CAPTURE_MAX_AGE
        or any(issue.accession_number == entry.accession_number for issue in capture.issues)
        or tuple(
            item for item in current.entries if item.accession_number == entry.accession_number
        )
        != (entry,)
    ):
        raise ValueError(INVALID)
    return entry


async def _audit(
    session: AsyncSession, row: SecEventResearchBinding | SecEventConsumptionReceipt
) -> None:
    await session.flush()
    binding = await session.scalar(
        text(f"SELECT to_jsonb(x)-'created_at' FROM {row.__tablename__} x WHERE id=:id"),
        {"id": row.id},
    )
    if not isinstance(binding, dict):
        raise ValueError(INVALID)
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.sec_research_fact",
        schema_version=POLICY,
        occurred_at=row.created_at,
        payload={"table": row.__tablename__, "row_id": str(row.id), "binding": binding},
    )


async def read_binding_observation(
    session: AsyncSession,
    *,
    check: MonitorCheck,
    plan: MonitorPlan,
    current_eligibility: bool = True,
    now: datetime | None = None,
) -> dict[str, object]:
    binding = await session.scalar(
        select(SecEventResearchBinding).where(SecEventResearchBinding.check_id == check.id)
    )
    if binding is None or (
        binding.plan_id,
        binding.account_id,
        binding.instrument_id,
        binding.source_run_id,
        binding.source_output_id,
        binding.source_hash,
    ) != (
        plan.id,
        plan.account_id,
        plan.instrument_id,
        plan.source_run_id,
        plan.source_output_id,
        plan.source_hash,
    ):
        raise ValueError(INVALID)
    if binding.condition_hash != plan.condition_hash:
        raise ValueError(INVALID)
    event = await session.get(SecDiscoveryEvent, binding.event_id)
    if (
        event is None
        or event.observation_receipt_id is not None
        or binding.rule_id != event.rule_id
        or binding.metadata_hash != event.metadata_hash
        or check.checked_at != binding.bound_at
        or event.event_key != check.event_key
    ):
        raise ValueError(INVALID)
    from nexus.research.durable_sec_index_storage import read_durable_index_capture

    row, capture = await read_durable_index_capture(
        session,
        configured_exhibit_store(session),
        snapshot_id=binding.capture_id,
        as_of=binding.bound_at,
    )
    if row.revision != binding.capture_hash:
        raise ValueError(INVALID)
    entry = verify_event_evidence(event, capture, now=binding.bound_at)
    observation: dict[str, object] = {
        "policy": POLICY,
        "binding_id": str(binding.id),
        "event_id": str(event.id),
        "rule_id": str(event.rule_id),
        "event_key": event.event_key,
        "current_snapshot_id": str(row.id),
        "current_hash": row.revision,
        "received_at": capture.index.received_at.isoformat(),
        "entry": entry.model_dump(mode="json"),
    }
    if current_eligibility:
        from nexus.monitoring.durable_sec_host import current_identity, latest_capture
        from nexus.monitoring.durable_sec_refreeze import require_current_batch

        rule = await session.get(SecMonitorRule, event.rule_id)
        if rule is None:
            raise ValueError(INVALID)
        await _lock(session, rule)
        await require_current_batch(session, binding.batch_id)
        checked_now = await _time(session)
        if (
            await rule_state(session, rule) != "active"
            or await session.scalar(
                select(SecDiscoveryEventRevision.id)
                .where(SecDiscoveryEventRevision.event_id == event.id)
                .limit(1)
            )
            is not None
        ):
            raise ValueError(INVALID)
        await current_identity(session, rule, checked_now)
        latest = await latest_capture(session, rule, checked_now)
        if latest is None:
            raise ValueError(INVALID)
        _, current_capture = await read_durable_index_capture(
            session, configured_exhibit_store(session), snapshot_id=latest.id, as_of=checked_now
        )
        # Raw-store awaits can outlive freshness or expose a newer capture.
        checked_now = await _time(session)
        newest = await latest_capture(session, rule, checked_now)
        if newest is None or newest.id != latest.id:
            raise ValueError(INVALID)
        received_at = require_utc(current_capture.index.received_at)
        if checked_now - received_at > _CAPTURE_MAX_AGE:
            # Validate the archived content at its own receipt time to
            # distinguish an exact stale capture from changed evidence.
            verify_event_evidence(event, current_capture, now=received_at)
            if (
                await session.scalar(
                    select(SecDisclosureKeyConflict.id)
                    .where(
                        SecDisclosureKeyConflict.account_id == event.account_id,
                        SecDisclosureKeyConflict.instrument_id == event.instrument_id,
                        SecDisclosureKeyConflict.event_key == event.event_key,
                    )
                    .limit(1)
                )
                is not None
            ):
                raise ValueError(INVALID)
            raise CurrentSecCaptureExpired(observation, capture_id=latest.id, rule_id=rule.id)
        verify_event_evidence(event, current_capture, now=checked_now)
    return observation


async def prepare_durable_sec_research_batch(
    session: AsyncSession, *, account_id: UUID, rule_id: UUID
) -> MonitorObservationBatch | None:
    """Material producer only: no outbox, job, budget hold or model requests."""
    rule = await session.get(SecMonitorRule, rule_id)
    if rule is None or rule.account_id != account_id:
        raise ValueError(INVALID)
    await _lock(session, rule)
    if await rule_state(session, rule) != "active":
        raise ValueError("DURABLE_SEC_RULE_INACTIVE")
    now = await _time(session)
    plan = await session.scalar(
        select(MonitorPlan)
        .where(
            MonitorPlan.account_id == account_id,
            MonitorPlan.instrument_id == rule.instrument_id,
            MonitorPlan.kind == "disclosure",
        )
        .order_by(MonitorPlan.generation.desc())
        .limit(1)
    )
    if plan is None or not plan.effective_at <= now < plan.valid_until:
        raise ValueError("MONITOR_AWAITING_RECONCILIATION")
    from nexus.monitoring.batches import disclosure_source_current

    if not await disclosure_source_current(session, plan, now):
        raise ValueError("MONITOR_AWAITING_RECONCILIATION")
    from nexus.monitoring.disclosure_observations import _bound_proposal

    proposal = await _bound_proposal(session, plan, now)
    if proposal.issuer_cik != rule.issuer_cik:
        raise ValueError(INVALID)
    qualified = await load_qualified_source(
        session, account_id=account_id, output_id=plan.source_output_id, now=now
    )
    if qualified.run.id != plan.source_run_id or qualified.output.content_hash != plan.source_hash:
        raise ValueError(INVALID)
    from nexus.research.durable_sec_index_storage import SOURCE, read_durable_index_capture

    latest = await session.scalar(
        select(DataSnapshot)
        .where(
            DataSnapshot.source == SOURCE,
            or_(
                DataSnapshot.provider_object_id.startswith(f"{rule.instrument_id}:"),
                DataSnapshot.payload["index"]["instrument_id"].astext == str(rule.instrument_id),
            ),
            DataSnapshot.created_at <= now,
            DataSnapshot.available_at <= now,
            DataSnapshot.received_at <= now,
            DataSnapshot.observed_at <= now,
        )
        .order_by(
            DataSnapshot.observed_at.desc(),
            DataSnapshot.available_at.desc(),
            DataSnapshot.id.desc(),
        )
        .limit(1)
    )
    if latest is None:
        raise ValueError(INVALID)
    row, capture = await read_durable_index_capture(
        session, configured_exhibit_store(session), snapshot_id=latest.id, as_of=now
    )
    # 0132 owns the addressed-only unbound daily receipt and mutual-exclusion guard.
    from nexus.persistence.models.sec_daily import SecDurableDailyEventReceipt

    events = tuple(
        await session.scalars(
            select(SecDiscoveryEvent)
            .where(
                SecDiscoveryEvent.account_id == account_id,
                SecDiscoveryEvent.observation_receipt_id.is_(None),
                SecDiscoveryEvent.instrument_id == rule.instrument_id,
                ~exists(
                    select(SecDiscoveryEventRevision.id).where(
                        SecDiscoveryEventRevision.event_id == SecDiscoveryEvent.id
                    )
                ),
                ~exists(
                    select(SecEventConsumptionReceipt.id).where(
                        SecEventConsumptionReceipt.event_id == SecDiscoveryEvent.id
                    )
                ),
                ~exists(
                    select(SecDurableDailyEventReceipt.id).where(
                        SecDurableDailyEventReceipt.event_id == SecDiscoveryEvent.id
                    )
                ),
            )
            .order_by(SecDiscoveryEvent.first_seen_at, SecDiscoveryEvent.id)
        )
    )
    from nexus.monitoring.durable_sec_refreeze import batch_unsubmitted
    from nexus.persistence.models.sec_refreeze import SecResearchBatchSupersession

    # Shared receipt/identity freshness is not a per-event defect or an empty queue.
    if (capture.index.instrument_id, capture.index.issuer_cik) != (
        rule.instrument_id,
        rule.issuer_cik,
    ) or not timedelta(0) <= now - capture.index.received_at <= _CAPTURE_MAX_AGE:
        raise ValueError(INVALID)
    superseded = set(
        await session.scalars(
            select(SecResearchBatchSupersession.old_batch_id)
            .join(
                MonitorObservationBatch,
                MonitorObservationBatch.id == SecResearchBatchSupersession.old_batch_id,
            )
            .where(
                MonitorObservationBatch.account_id == account_id,
                MonitorObservationBatch.instrument_id == rule.instrument_id,
            )
        )
    )
    prior_rows = tuple(
        await session.scalars(
            select(SecEventResearchBinding).where(
                SecEventResearchBinding.account_id == account_id,
                SecEventResearchBinding.instrument_id == rule.instrument_id,
            )
        )
    )
    pending_batches: set[UUID] = set()
    unsubmitted_by_batch: dict[UUID, bool] = {}
    unsafe_events: set[UUID] = set()
    for previous in prior_rows:
        if previous.batch_id in superseded:
            continue
        if previous.batch_id not in unsubmitted_by_batch:
            unsubmitted_by_batch[previous.batch_id] = await batch_unsubmitted(
                session, previous.batch_id
            )
        if unsubmitted_by_batch[previous.batch_id]:
            pending_batches.add(previous.batch_id)
        else:
            unsafe_events.add(previous.event_id)
    selected = []
    for event in events:
        prior = [binding for binding in prior_rows if binding.event_id == event.id]
        if event.id in unsafe_events:
            continue
        submitted = False
        for binding in prior:
            if (
                await session.scalar(
                    select(MonitorIncrement.id).where(MonitorIncrement.batch_id == binding.batch_id)
                )
                is not None
            ):
                submitted = True
        if submitted:
            continue
        try:
            entry = verify_event_evidence(event, capture, now=now)
        except ValueError:
            continue
        selected.append((event, entry))
    if len(pending_batches) == 1:
        candidate_id = next(iter(pending_batches))
        old_bindings = [binding for binding in prior_rows if binding.batch_id == candidate_id]
        if {binding.event_id for binding in old_bindings} == {
            event.id for event, _ in selected
        } and all(binding.plan_id == plan.id for binding in old_bindings):
            return await session.get(MonitorObservationBatch, candidate_id)
    if not selected:
        return None
    if len(selected) > 32:
        raise ValueError("MONITOR_PENDING_OVERFLOW")
    published = await _time(session)
    if published < now or not plan.effective_at <= published < plan.valid_until:
        raise ValueError("MONITOR_AWAITING_RECONCILIATION")
    if not await disclosure_source_current(session, plan, published):
        raise ValueError("MONITOR_AWAITING_RECONCILIATION")
    now = published
    # Original source bytes may have taken time to read; recheck the publication cutoff.
    for event, _entry in selected:
        verify_event_evidence(event, capture, now=now)
    batch_id = new_uuid7()
    checks = []
    trigger_ids = []
    for event, entry in selected:
        discovery_check = await session.scalar(
            select(SecDiscoveryCheck)
            .where(
                SecDiscoveryCheck.rule_id == rule.id,
                SecDiscoveryCheck.capture_id == event.capture_id,
                SecDiscoveryCheck.result == "true",
                SecDiscoveryCheck.observation["events"].contains(
                    [{"event_key": event.event_key, "classification": "new", "entry": event.entry}]
                ),
            )
            .order_by(SecDiscoveryCheck.checked_at, SecDiscoveryCheck.id)
            .limit(1)
        )
        if discovery_check is None:
            raise ValueError(INVALID)
        binding = SecEventResearchBinding(
            id=new_uuid7(),
            created_at=now,
            event_id=event.id,
            rule_id=rule.id,
            discovery_check_id=discovery_check.id,
            metadata_hash=event.metadata_hash,
            condition_hash=plan.condition_hash,
            account_id=account_id,
            instrument_id=rule.instrument_id,
            plan_id=plan.id,
            source_run_id=plan.source_run_id,
            source_output_id=plan.source_output_id,
            source_hash=plan.source_hash,
            capture_id=row.id,
            capture_hash=row.revision,
            check_id=new_uuid7(),
            trigger_id=new_uuid7(),
            batch_id=batch_id,
            bound_at=now,
        )
        session.add(binding)
        # Deferred SQL custody links allow an explicit write order; never rely on mapper cycles.
        await session.flush([binding])
        check = MonitorCheck(
            id=binding.check_id,
            created_at=now,
            account_id=account_id,
            instrument_id=rule.instrument_id,
            plan_id=plan.id,
            kind="disclosure",
            event_key=event.event_key,
            scheduled_at=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
            checked_at=now,
            result="true",
            reason=None,
            observation={
                "policy": POLICY,
                "binding_id": str(binding.id),
                "event_id": str(event.id),
                "rule_id": str(rule.id),
                "event_key": event.event_key,
                "current_snapshot_id": str(row.id),
                "current_hash": row.revision,
                "received_at": capture.index.received_at.isoformat(),
                "entry": entry.model_dump(mode="json"),
            },
        )
        session.add(check)
        await session.flush([check])
        session.add(
            MonitorTrigger(
                id=binding.trigger_id,
                created_at=now,
                account_id=account_id,
                instrument_id=rule.instrument_id,
                condition_hash=plan.condition_hash,
                kind="disclosure",
                event_key=event.event_key,
                check_id=check.id,
                cause="disclosure_added",
                status="pending",
                batch_id=batch_id,
                outbox_id=None,
                durable_binding_id=binding.id,
            )
        )
        checks.append(str(check.id))
        trigger_ids.append(str(binding.trigger_id))
        await _audit(session, binding)
    batch = MonitorObservationBatch(
        id=batch_id,
        created_at=now,
        account_id=account_id,
        instrument_id=rule.instrument_id,
        scheduled_at=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
        checked_at=now,
        sealed_at=now,
        observation_hash=sha256(
            json.dumps(sorted(trigger_ids), separators=(",", ":")).encode()
        ).hexdigest(),
        plan_ids=[str(plan.id)],
        check_ids=sorted(checks),
        trigger_ids=sorted(trigger_ids),
        outbox_id=None,
        routing_version=ROUTING,
    )
    session.add(batch)
    await session.flush()
    from nexus.monitoring.durable_sec_refreeze import append_supersession

    superseded_at = await _time(session)
    for old_batch_id in sorted(pending_batches, key=str):
        await append_supersession(
            session, old_batch_id=old_batch_id, new_batch_id=batch.id, now=superseded_at
        )
    return batch


async def mirror_consumption_receipts(session: AsyncSession, *, run_id: UUID) -> int:
    """Copy only already validated per-event addressed proofs; never infer from run completion."""
    from nexus.persistence.models.monitoring import (
        MonitorDailyEventReceipt,
        MonitorMixedEventReceipt,
    )

    count = 0
    proof_models: tuple[tuple[Any, str], ...] = (
        (MonitorMixedEventReceipt, "mixed_receipt_id"),
        (MonitorDailyEventReceipt, "daily_receipt_id"),
    )
    for model, field in proof_models:
        proofs = tuple(await session.scalars(select(model).where(model.run_id == run_id)))
        for proof in proofs:
            binding = await session.scalar(
                select(SecEventResearchBinding).where(
                    SecEventResearchBinding.trigger_id == proof.trigger_id
                )
            )
            if binding is None:
                continue
            if (
                await session.scalar(
                    select(SecEventConsumptionReceipt.id).where(
                        getattr(SecEventConsumptionReceipt, field) == proof.id
                    )
                )
                is not None
            ):
                continue
            row = SecEventConsumptionReceipt(
                id=new_uuid7(),
                created_at=await _time(session),
                binding_id=binding.id,
                event_id=binding.event_id,
                mixed_receipt_id=proof.id if field == "mixed_receipt_id" else None,
                daily_receipt_id=proof.id if field == "daily_receipt_id" else None,
            )
            session.add(row)
            await _audit(session, row)
            count += 1
    return count
