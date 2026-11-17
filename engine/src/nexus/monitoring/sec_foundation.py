"""Initialize existing authorized SEC scope independently of research qualification."""

import json
from datetime import timedelta
from hashlib import sha256
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.monitoring.auto_increment_policy import SOURCE as LIMIT_SOURCE
from nexus.monitoring.durable_sec_claims import audit_fact
from nexus.monitoring.durable_sec_host import current_identity
from nexus.monitoring.durable_sec_storage import (
    _time,
    import_legacy_notice_claims,
    rule_state,
)
from nexus.monitoring.sec_index_reader import read_monitor_index
from nexus.monitoring.storage import lock_security
from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.sec_monitor import SecMonitorRule, SecMonitorRuleState
from nexus.research.sec_exhibit_storage import configured_exhibit_store

SOURCE = "nexus_sec_foundation_scope"
SCHEMA = "sec_foundation_scope_v1"
ACTION = "research.monitor.foundation_scope"
INVALID = "SEC_FOUNDATION_SCOPE_INVALID"
FORMS = ["8-K", "8-K/A"]


async def enable_foundation(
    session: AsyncSession,
    *,
    account_id: UUID,
    candidate_batch_id: UUID | None,
    instrument_id: UUID,
    identity_snapshot_id: UUID,
    capture_id: UUID,
) -> SecMonitorRule:
    await lock_security(session, account_id=account_id, instrument_id=instrument_id)
    now = await _time(session)
    candidate = (
        None
        if candidate_batch_id is None
        else await load_agent_paper_candidate_batch(session, batch_id=candidate_batch_id)
    )
    scope = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == LIMIT_SOURCE,
            DataSnapshot.provider_object_id == str(account_id),
        )
    )
    if (
        candidate is None
        or candidate.account_id != account_id
        or candidate.created_at > now
        or instrument_id not in candidate.instrument_ids
        or scope is None
        or scope.payload.get("candidate_batch_id") != str(candidate_batch_id)
        or scope.payload.get("candidate_hash") != candidate.batch_hash
        or max(scope.created_at, scope.available_at, scope.received_at, scope.observed_at) > now
    ):
        raise ValueError(INVALID)
    # An existing immutable frequency scope is an upper bound, never paid authority.
    await session.scalar(
        text("SELECT sec_auto_increment_scoped(:a,:i)"), {"a": account_id, "i": instrument_id}
    )
    old = await session.scalar(
        select(SecMonitorRule).where(
            SecMonitorRule.account_id == account_id,
            SecMonitorRule.instrument_id == instrument_id,
        )
    )
    if old is not None:
        if old.anchor_plan_id is not None:
            raise ValueError("SEC_DURABLE_SCOPE_ALREADY_BOUND")
        origin = await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == SOURCE,
                DataSnapshot.provider_object_id == str(old.id),
            )
        )
        if (
            origin is None
            or origin.payload.get("capture_id") != str(capture_id)
            or (
                old.identity_snapshot_id != identity_snapshot_id
                or origin.payload.get("scope_id") != str(scope.id)
            )
        ):
            raise ValueError("SEC_DURABLE_SCOPE_ALREADY_BOUND")
        await session.scalar(
            text(
                "SELECT sec_foundation_require(to_jsonb(r)) FROM sec_monitor_rules r WHERE id=:id"
            ),
            {"id": old.id},
        )
        await rule_state(session, old)
        return old
    store = configured_exhibit_store(session)
    capture, index = await read_monitor_index(session, store, snapshot_id=capture_id, as_of=now)
    if index.instrument_id != instrument_id or index.identity_snapshot_id != identity_snapshot_id:
        raise ValueError(INVALID)
    rule = SecMonitorRule(
        id=new_uuid7(),
        account_id=account_id,
        instrument_id=instrument_id,
        issuer_cik=index.issuer_cik,
        identity_snapshot_id=identity_snapshot_id,
        anchor_plan_id=None,
        forms=list(FORMS),
        policy_version="durable_sec_v1",
    )
    latest_id = await current_identity(session, rule, now)
    identity = await session.get(DataSnapshot, identity_snapshot_id)
    latest = await session.get(DataSnapshot, latest_id)
    assert identity is not None and latest is not None
    now = await _time(session)
    if not timedelta(0) <= now - index.received_at <= timedelta(seconds=300):
        raise ValueError("SEC_FOUNDATION_SOURCE_STALE")
    rule.created_at = rule.effective_at = now
    payload = dict(
        schema_version=SCHEMA,
        rule_id=str(rule.id),
        account_id=str(account_id),
        instrument_id=str(instrument_id),
        issuer_cik=index.issuer_cik,
        forms=list(FORMS),
        candidate_batch_id=str(candidate_batch_id),
        candidate_hash=candidate.batch_hash,
        scope_id=str(scope.id),
        scope_hash=scope.revision,
        identity_snapshot_id=str(identity.id),
        identity_hash=identity.revision,
        latest_identity_snapshot_id=str(latest.id),
        latest_identity_hash=latest.revision,
        capture_id=str(capture.id),
        capture_hash=capture.revision,
        effective_at=now.isoformat(),
    )
    digest = sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    origin = DataSnapshot(
        id=new_uuid7(),
        source=SOURCE,
        schema_version=SCHEMA,
        provider_object_id=str(rule.id),
        revision=digest,
        payload=payload,
        created_at=now,
        observed_at=now,
        available_at=now,
        received_at=now,
    )
    session.add(origin)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action=ACTION,
        schema_version=SCHEMA,
        occurred_at=now,
        payload={"snapshot_id": str(origin.id), "scope_hash": digest},
    )
    session.add(rule)
    await audit_fact(session, rule)
    state = SecMonitorRuleState(
        id=new_uuid7(),
        rule_id=rule.id,
        sequence=0,
        state="active",
        created_at=now,
        effective_at=now,
    )
    session.add(state)
    await audit_fact(session, state)
    await import_legacy_notice_claims(session, rule, now)
    return rule
