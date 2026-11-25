"""Explicit durable discovery facts; research authority remains a separate gate."""

import json
from datetime import datetime
from hashlib import sha256
from typing import cast
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.monitoring.disclosure_observations import _bound_proposal, _capture
from nexus.monitoring.durable_sec_claims import audit_fact, claim_notice
from nexus.monitoring.durable_sec_contracts import (
    ACCEPTED_CUTOFF_EVALUATION,
    DurableSecPolicy,
    RuleState,
)
from nexus.monitoring.durable_sec_evaluation import evaluate_discovery, stable_event_key
from nexus.monitoring.sec_index_reader import read_monitor_index
from nexus.monitoring.storage import lock_security
from nexus.notifications.durable_sec import DURABLE_SEC_EVENT, DurableSecNotification
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan, MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent, OutboxEvent
from nexus.persistence.models.sec_conflicts import SecDisclosureKeyConflict
from nexus.persistence.models.sec_monitor import (
    SecDiscoveryCheck,
    SecDiscoveryEvent,
    SecDiscoveryEventRevision,
    SecMonitorRule,
    SecMonitorRuleState,
    SecNotificationClaim,
)
from nexus.research.durable_sec_index import SecMetadataFact
from nexus.research.durable_sec_index_storage import (
    SOURCE as DURABLE_SOURCE,
)
from nexus.research.durable_sec_index_storage import (
    read_durable_index_capture,
)
from nexus.research.sec_disclosure_index import SecIndexEntry
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.research.sec_index_storage import SOURCE as INDEX_SOURCE


async def _time(session: AsyncSession) -> datetime:
    at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
    if not isinstance(at, datetime):
        raise ValueError("SEC_DURABLE_CLOCK_UNAVAILABLE")
    return require_utc(at)


async def _lock(session: AsyncSession, rule: SecMonitorRule) -> None:
    await lock_security(session, account_id=rule.account_id, instrument_id=rule.instrument_id)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
        {"key": f"sec-durable-rule:{rule.id}"},
    )


async def rule_state(session: AsyncSession, rule: SecMonitorRule) -> RuleState:
    state = await session.scalar(
        select(SecMonitorRuleState.state)
        .where(SecMonitorRuleState.rule_id == rule.id)
        .order_by(SecMonitorRuleState.sequence.desc())
        .limit(1)
    )
    if state not in ("active", "paused", "revoked"):
        raise ValueError("SEC_DURABLE_STATE_INVALID")
    return cast(RuleState, state)


async def enable_rule(session: AsyncSession, *, account_id: UUID, plan_id: UUID) -> SecMonitorRule:
    plan = await session.get(MonitorPlan, plan_id)
    if plan is None or plan.account_id != account_id or plan.kind != "disclosure":
        raise ValueError("SEC_DURABLE_SCOPE_INVALID")
    await lock_security(session, account_id=account_id, instrument_id=plan.instrument_id)
    existing = await session.scalar(
        select(SecMonitorRule).where(
            SecMonitorRule.account_id == account_id,
            SecMonitorRule.instrument_id == plan.instrument_id,
        )
    )
    if existing is not None:
        if existing.anchor_plan_id != plan_id:
            raise ValueError("SEC_DURABLE_SCOPE_ALREADY_BOUND")
        await rule_state(session, existing)
        return existing
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
    if latest != plan_id:
        raise ValueError("SEC_DURABLE_SCOPE_INVALID")
    now = await _time(session)
    proposal = await _bound_proposal(session, plan, now)
    _, baseline = await read_monitor_index(
        session,
        configured_exhibit_store(session),
        snapshot_id=proposal.baseline_snapshot_id,
        as_of=proposal.baseline_as_of,
    )
    if (baseline.instrument_id, baseline.issuer_cik) != (plan.instrument_id, proposal.issuer_cik):
        raise ValueError("SEC_DURABLE_SCOPE_INVALID")
    # The historical research identity limits initial scope; its TTL is not extended.
    now = await _time(session)
    rule = SecMonitorRule(
        id=new_uuid7(),
        created_at=now,
        account_id=account_id,
        instrument_id=plan.instrument_id,
        issuer_cik=proposal.issuer_cik,
        identity_snapshot_id=baseline.identity_snapshot_id,
        anchor_plan_id=plan.id,
        effective_at=now,
        forms=list(proposal.condition.forms),
        policy_version="durable_sec_v1",
    )
    session.add(rule)
    await session.flush()
    await audit_fact(session, rule)
    state = SecMonitorRuleState(
        id=new_uuid7(),
        created_at=now,
        rule_id=rule.id,
        sequence=0,
        state="active",
        effective_at=now,
    )
    session.add(state)
    await session.flush()
    await audit_fact(session, state)
    await import_legacy_notice_claims(session, rule, now)
    return rule


async def import_legacy_notice_claims(
    session: AsyncSession, rule: SecMonitorRule, now: datetime
) -> None:
    account_id = rule.account_id
    # Reference old attempts exactly. This occupies identity; it does not imply delivery.
    old = tuple(
        await session.scalars(
            select(MonitorTrigger).where(
                MonitorTrigger.account_id == account_id,
                MonitorTrigger.instrument_id == rule.instrument_id,
                MonitorTrigger.kind == "disclosure",
                MonitorTrigger.outbox_id.is_not(None),
            )
        )
    )
    for trigger in old:
        if (
            await session.scalar(
                select(SecNotificationClaim.id).where(
                    SecNotificationClaim.account_id == account_id,
                    SecNotificationClaim.instrument_id == rule.instrument_id,
                    SecNotificationClaim.event_key == trigger.event_key,
                )
            )
            is not None
        ):
            continue
        claim = SecNotificationClaim(
            id=new_uuid7(),
            created_at=now,
            account_id=account_id,
            instrument_id=rule.instrument_id,
            event_key=trigger.event_key,
            outbox_id=trigger.outbox_id,
            legacy_trigger_id=trigger.id,
        )
        session.add(claim)
        await session.flush()
        await audit_fact(session, claim)


async def change_state(
    session: AsyncSession, *, account_id: UUID, rule_id: UUID, state: RuleState
) -> SecMonitorRuleState:
    rule = await session.get(SecMonitorRule, rule_id)
    if (
        rule is None
        or rule.account_id != account_id
        or state not in ("active", "paused", "revoked")
    ):
        raise ValueError("SEC_DURABLE_SCOPE_INVALID")
    await _lock(session, rule)
    previous = await session.scalar(
        select(SecMonitorRuleState)
        .where(SecMonitorRuleState.rule_id == rule.id)
        .order_by(SecMonitorRuleState.sequence.desc())
        .limit(1)
    )
    if previous is None:
        raise ValueError("SEC_DURABLE_STATE_INVALID")
    if previous.state == state:
        return previous
    if previous.state == "revoked":
        raise ValueError("SEC_DURABLE_RULE_REVOKED")
    now = await _time(session)
    current = SecMonitorRuleState(
        id=new_uuid7(),
        created_at=now,
        rule_id=rule.id,
        sequence=previous.sequence + 1,
        state=state,
        effective_at=now,
    )
    session.add(current)
    await session.flush()
    await audit_fact(session, current)
    return current


def _metadata(entry: SecIndexEntry | SecMetadataFact) -> str:
    return sha256(
        json.dumps(entry.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


async def record_discovery(
    session: AsyncSession,
    *,
    account_id: UUID,
    rule_id: UUID,
    capture_id: UUID | None,
    scheduled_at: datetime,
) -> SecDiscoveryCheck:
    check, discovered = await _record_discovery_facts(
        session,
        account_id=account_id,
        rule_id=rule_id,
        capture_id=capture_id,
        scheduled_at=scheduled_at,
        receipt=None,
    )
    for event in discovered:
        await _publish_discovery_notice(session, event)
    return check


async def _record_discovery_facts(
    session: AsyncSession,
    *,
    account_id: UUID,
    rule_id: UUID,
    capture_id: UUID | None,
    scheduled_at: datetime,
    receipt: AuditEvent | None,
) -> tuple[SecDiscoveryCheck, list[SecDiscoveryEvent]]:
    rule = await session.get(SecMonitorRule, rule_id)
    if rule is None or rule.account_id != account_id:
        raise ValueError("SEC_DURABLE_SCOPE_INVALID")
    await _lock(session, rule)
    if await rule_state(session, rule) != "active":
        raise ValueError("SEC_DURABLE_RULE_INACTIVE")
    old = await session.scalar(
        select(SecDiscoveryCheck).where(
            SecDiscoveryCheck.rule_id == rule.id, SecDiscoveryCheck.scheduled_at == scheduled_at
        )
    )
    if old is not None:
        if receipt is not None and old.observation.get("observation_receipt_id") != str(receipt.id):
            raise ValueError("SEC_OBSERVATION_BUCKET_OCCUPIED")
        return old, []
    now = await _time(session)
    if scheduled_at != now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0):
        raise ValueError("SEC_DURABLE_BUCKET_UNAVAILABLE")
    rows = tuple(
        await session.scalars(
            select(SecDiscoveryEvent).where(
                SecDiscoveryEvent.account_id == account_id,
                SecDiscoveryEvent.instrument_id == rule.instrument_id,
            )
        )
    )
    known = {v.event_key: SecIndexEntry.model_validate(v.entry) for v in rows}
    legacy = tuple(
        (
            await session.execute(
                select(MonitorTrigger.event_key, MonitorCheck.observation, MonitorTrigger.id)
                .join(MonitorCheck, MonitorCheck.id == MonitorTrigger.check_id)
                .where(
                    MonitorTrigger.account_id == account_id,
                    MonitorTrigger.instrument_id == rule.instrument_id,
                    MonitorTrigger.kind == "disclosure",
                )
            )
        ).all()
    )
    for key, document, _trigger_id in legacy:
        if isinstance(document, dict) and isinstance(document.get("entry"), dict):
            known[key] = SecIndexEntry.model_validate(document["entry"])
    policy = DurableSecPolicy.model_validate(
        dict(
            account_id=account_id,
            instrument_id=rule.instrument_id,
            issuer_cik=rule.issuer_cik,
            forms=rule.forms,
            effective_at=rule.effective_at,
        )
    )
    observation: dict[str, object]
    discovered: list[SecDiscoveryEvent] = []
    result, coverage = "unknown", "unknown"
    reason: str | None = "DURABLE_SEC_SOURCE_INVALID"
    capture = None
    index = None
    evidence = None
    source_details: dict[str, object] = {}
    try:
        if capture_id is not None:
            capture = await session.get(DataSnapshot, capture_id)
            if capture is None:
                raise ValueError("DURABLE_SEC_SOURCE_INVALID")
            if capture.source == DURABLE_SOURCE:
                capture, evidence = await read_durable_index_capture(
                    session,
                    configured_exhibit_store(session),
                    snapshot_id=capture_id,
                    as_of=now,
                    coverage_details_out=source_details,
                )
                index = evidence.index
            elif capture.source == INDEX_SOURCE:
                capture, index = await _capture(
                    session, configured_exhibit_store(session), snapshot_id=capture_id, as_of=now
                )
            else:
                raise ValueError("DURABLE_SEC_SOURCE_INVALID")
    except (OSError, ValueError):
        index = None
    if index is None or capture is None:
        if receipt is not None and receipt.payload.get("status") == "failed":
            reason = "SEC_INDEX_REFRESH_FAILED"
        observation = {"result": result, "coverage": coverage, "reason": reason}
    else:
        decision = evaluate_discovery(
            policy, index, capture.available_at, now, known, frozenset(key for key, _, _ in legacy)
        )
        observation = decision.model_dump(mode="json")
        if source_details:
            observation["coverage_details"] = source_details
        result, coverage = decision.result, decision.coverage
        reason = decision.reason if result == "unknown" else None
        by_key = {v.event_key: v for v in rows}
        legacy_by_key = {key: (document, trigger_id) for key, document, trigger_id in legacy}
        # Generic metadata preserves conflicts even when the form changed out of
        # scope. Invalid rows remain explicit unknowns; they never erase originals.
        facts = evidence.metadata_entries if evidence is not None else index.entries
        conflicts = []
        if evidence is not None:
            observation["row_issues"] = [v.model_dump(mode="json") for v in evidence.issues]
        for fact in facts:
            key = stable_event_key(rule.issuer_cik, fact.accession_number)
            original = by_key.get(key)
            metadata = _metadata(fact)
            legacy_original = legacy_by_key.get(key)
            old_entry = (
                original.entry
                if original is not None
                else (legacy_original[0].get("entry") if legacy_original is not None else None)
            )
            if not isinstance(old_entry, dict) or metadata == _metadata(
                SecIndexEntry.model_validate(old_entry)
            ):
                continue
            from nexus.monitoring.sec_key_conflicts import record_key_conflict

            await record_key_conflict(
                session,
                rule=rule,
                capture_id=capture.id,
                event_key=key,
                original_event_id=original.id if original is not None else None,
                legacy_trigger_id=legacy_original[1]
                if original is None and legacy_original is not None
                else None,
                entry=fact.model_dump(mode="json"),
                metadata_hash=metadata,
                now=now,
            )
            conflicts.append(key)
            if original is None:
                continue
            exists = await session.scalar(
                select(SecDiscoveryEventRevision.id).where(
                    SecDiscoveryEventRevision.event_id == original.id,
                    SecDiscoveryEventRevision.metadata_hash == metadata,
                )
            )
            if exists is None:
                revision = SecDiscoveryEventRevision(
                    id=new_uuid7(),
                    created_at=now,
                    event_id=original.id,
                    capture_id=capture.id,
                    entry=fact.model_dump(mode="json"),
                    metadata_hash=metadata,
                    observed_at=now,
                )
                session.add(revision)
                await session.flush()
                await audit_fact(session, revision)
        # A later exact-looking row does not erase an already established conflict.
        prior_conflicts = await session.scalars(
            select(SecDisclosureKeyConflict.event_key).where(
                SecDisclosureKeyConflict.account_id == account_id,
                SecDisclosureKeyConflict.instrument_id == rule.instrument_id,
            )
        )
        conflicts = sorted(set(conflicts) | set(prior_conflicts))
        if conflicts:
            observation["metadata_conflicts"] = conflicts
            if reason == "DURABLE_SEC_ACCEPTED_CUTOFF_UNPROVEN":
                reason = "DURABLE_SEC_COVERAGE_CONFLICT"
            observation["reason"] = reason
        for item in decision.events:
            if item.classification != "new":
                continue
            event = SecDiscoveryEvent(
                id=new_uuid7(),
                created_at=now,
                account_id=account_id,
                instrument_id=rule.instrument_id,
                event_key=item.event_key,
                rule_id=rule.id,
                capture_id=capture.id,
                entry=item.entry.model_dump(mode="json"),
                metadata_hash=_metadata(item.entry),
                first_seen_at=now,
                observation_receipt_id=receipt.id if receipt is not None else None,
            )
            session.add(event)
            await session.flush()
            await audit_fact(session, event)
            discovered.append(event)
    if receipt is not None:
        observation["observation_receipt_id"] = str(receipt.id)
    observation["evaluation_version"] = ACCEPTED_CUTOFF_EVALUATION
    observation["accepted_complete_through_at"] = None
    check = SecDiscoveryCheck(
        id=new_uuid7(),
        created_at=now,
        rule_id=rule.id,
        scheduled_at=scheduled_at,
        checked_at=now,
        capture_id=capture.id if capture is not None else None,
        result=result,
        coverage=coverage,
        reason=reason,
        observation=observation,
    )
    session.add(check)
    await session.flush()
    await audit_fact(session, check)
    return check, discovered


async def _publish_discovery_notice(session: AsyncSession, event: SecDiscoveryEvent) -> None:
    entry = SecIndexEntry.model_validate(event.entry)
    now = event.first_seen_at
    outbox_id = new_uuid7()
    if not await claim_notice(
        session,
        account_id=event.account_id,
        instrument_id=event.instrument_id,
        event_key=event.event_key,
        outbox_id=outbox_id,
        legacy_trigger_id=None,
        now=now,
    ):
        return
    symbols = tuple(
        await session.scalars(
            select(InstrumentAlias.symbol)
            .where(
                InstrumentAlias.instrument_id == event.instrument_id,
                InstrumentAlias.valid_from <= now.date(),
                (InstrumentAlias.valid_to.is_(None) | (InstrumentAlias.valid_to >= now.date())),
            )
            .distinct()
        )
    )
    notice = DurableSecNotification(
        discovery_id=event.id,
        instrument_id=event.instrument_id,
        rule_id=event.rule_id,
        symbol=symbols[0] if len(symbols) == 1 else None,
        event_key=event.event_key,
        form=entry.form,
        accepted_at=entry.accepted_at,
        discovered_at=now,
    )
    session.add(
        OutboxEvent(
            id=outbox_id,
            created_at=now,
            event_type=DURABLE_SEC_EVENT[0],
            aggregate_id=event.id,
            schema_version=DURABLE_SEC_EVENT[1],
            payload=notice.model_dump(mode="json"),
            status="queued",
            attempt_count=0,
            max_attempts=1,
            available_at=now,
        )
    )
    await session.flush()
