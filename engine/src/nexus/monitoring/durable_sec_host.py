"""Host adapters for registered SEC scopes; no research authority or TTL extension."""

from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.market.current_identity import SOURCE as IDENTITY_SOURCE
from nexus.market.current_identity import identity_parse_scope
from nexus.market.current_listing_revision import SOURCE as LISTING_SOURCE
from nexus.monitoring.durable_sec_storage import _lock, record_discovery, rule_state
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.models.sec_monitor import SecDiscoveryCheck, SecMonitorRule
from nexus.research.artifact_read_scope import artifact_read_scope
from nexus.research.sec_exhibit_storage import _read_identity, configured_exhibit_store
from nexus.workbench.evidence_read_scope import evidence_read_scope

if TYPE_CHECKING:
    from nexus.settings import EngineSettings

SOURCE = "sec_durable_disclosure_index"


async def registered_rule(
    session: AsyncSession, account_id: UUID, instrument_id: UUID
) -> SecMonitorRule | None:
    row: SecMonitorRule | None = await session.scalar(
        select(SecMonitorRule)
        .where(
            SecMonitorRule.account_id == account_id,
            SecMonitorRule.instrument_id == instrument_id,
        )
        .execution_options(populate_existing=True)
    )

    return row


async def current_identity(session: AsyncSession, rule: SecMonitorRule, now: datetime) -> UUID:
    store = configured_exhibit_store(session)
    original = await _read_identity(
        session, store, snapshot_id=rule.identity_snapshot_id, as_of=now
    )
    if (original.instrument_id, original.identity.cik) != (
        rule.instrument_id,
        rule.issuer_cik,
    ) or original.checked_at > now:
        raise ValueError("SEC_DURABLE_IDENTITY_INVALID")
    latest = await session.scalar(
        select(DataSnapshot)
        .where(
            DataSnapshot.source.in_((IDENTITY_SOURCE, LISTING_SOURCE)),
            DataSnapshot.payload["instrument_id"].astext == str(rule.instrument_id),
            DataSnapshot.created_at <= now,
            DataSnapshot.available_at <= now,
            DataSnapshot.observed_at <= now,
            DataSnapshot.received_at <= now,
        )
        .order_by(
            DataSnapshot.observed_at.desc(), DataSnapshot.created_at.desc(), DataSnapshot.id.desc()
        )
        .limit(1)
    )
    if latest is None:
        raise ValueError("SEC_DURABLE_IDENTITY_INVALID")
    proof = await _read_identity(session, store, snapshot_id=latest.id, as_of=now)
    if (proof.instrument_id, proof.identity.cik) != (
        rule.instrument_id,
        rule.issuer_cik,
    ) or proof.checked_at > now:
        raise ValueError("SEC_DURABLE_IDENTITY_INVALID")
    return latest.id


async def latest_capture(
    session: AsyncSession, rule: SecMonitorRule, now: datetime
) -> DataSnapshot | None:
    row: DataSnapshot | None = await session.scalar(
        select(DataSnapshot)
        .where(
            DataSnapshot.source == SOURCE,
            or_(
                DataSnapshot.provider_object_id.startswith(f"{rule.instrument_id}:"),
                DataSnapshot.payload["index"]["instrument_id"].astext == str(rule.instrument_id),
            ),
            DataSnapshot.created_at <= now,
            DataSnapshot.available_at <= now,
            DataSnapshot.observed_at <= now,
            DataSnapshot.received_at <= now,
        )
        .order_by(
            DataSnapshot.observed_at.desc(),
            DataSnapshot.available_at.desc(),
            DataSnapshot.received_at.desc(),
            DataSnapshot.created_at.desc(),
            DataSnapshot.id.desc(),
        )
        .limit(1)
    )

    return row


async def claim_refresh(
    session: AsyncSession,
    *,
    rule: SecMonitorRule,
    now: datetime,
    scheduled: datetime,
    repo_root: Path,
    contact: str | None,
    candidate_keys: tuple[str, ...] = (),
) -> dict[str, object] | None:
    from nexus.ops.monitor_batch_prepare import _archive_root
    from nexus.ops.monitor_disclosure_refresh import (
        CLAIM,
        FEED,
        REUSE_MAX_AGE_SECONDS,
        SCHEMA,
        _bucket,
    )
    from nexus.research.durable_sec_index_storage import read_durable_index_capture

    await _lock(session, rule)
    if await rule_state(session, rule) != "active":
        return {"status": "paused", "reason": "SEC_DURABLE_RULE_INACTIVE"}
    if now < rule.effective_at or now < rule.created_at:
        return {"status": "unknown", "reason": "SEC_DURABLE_SCOPE_NOT_EFFECTIVE"}
    identity_id = await current_identity(session, rule, now)
    latest = await latest_capture(session, rule, now)
    if latest is not None:
        try:
            _, capture = await read_durable_index_capture(
                session, configured_exhibit_store(session), snapshot_id=latest.id, as_of=now
            )
            index = capture.index
            if (
                (index.instrument_id, index.issuer_cik, index.identity_snapshot_id)
                == (
                    rule.instrument_id,
                    rule.issuer_cik,
                    identity_id,
                )
                and timedelta(0)
                <= now - index.received_at
                <= timedelta(seconds=REUSE_MAX_AGE_SECONDS)
                and not set(candidate_keys).difference(e.accession_number for e in index.entries)
            ):
                return None
        except (ValueError, OSError):
            # A bad latest source is never replaced by an older cached source.
            # A new bucket may collect new originals through the normal once gate.
            pass
    previous = await session.scalar(
        select(AuditEvent.id)
        .where(
            AuditEvent.action == CLAIM,
            AuditEvent.payload["account_id"].astext == str(rule.account_id),
            AuditEvent.payload["instrument_id"].astext == str(rule.instrument_id),
            AuditEvent.payload["feed"].astext == FEED,
            AuditEvent.payload["scheduled_at"].astext == scheduled.isoformat(),
        )
        .limit(1)
    )
    if previous is not None:
        return {"status": "already_claimed"}
    if contact is None:
        return {"status": "blocked", "reason": "SEC_MONITOR_CONTACT_REQUIRED"}
    at = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(at, datetime) or _bucket(at) != scheduled:
        raise ValueError("MONITOR_REFRESH_BUCKET_UNAVAILABLE")
    operation = uuid4()
    claim: dict[str, object] = dict(
        operation_id=str(operation),
        account_id=str(rule.account_id),
        instrument_id=str(rule.instrument_id),
        rule_id=str(rule.id),
        durable_sec=True,
        identity_snapshot_id=str(identity_id),
        issuer_cik=rule.issuer_cik,
        feed=FEED,
        listed_history=False,
        scheduled_at=scheduled.isoformat(),
        archive_dir=str(_archive_root(repo_root) / f"index-{rule.issuer_cik}-{operation}"),
    )
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action=CLAIM,
        schema_version=SCHEMA,
        occurred_at=at,
        payload=claim,
    )
    return claim


async def scan_registered(
    session: AsyncSession, rule: SecMonitorRule, now: datetime
) -> tuple[SecDiscoveryCheck | None, int]:
    await _lock(session, rule)
    if await rule_state(session, rule) != "active" or now < max(rule.effective_at, rule.created_at):
        return None, 0
    scheduled = now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0)
    prior = await session.scalar(
        select(SecDiscoveryCheck.id).where(
            SecDiscoveryCheck.rule_id == rule.id,
            SecDiscoveryCheck.scheduled_at == scheduled,
        )
    )
    # The current identity and frozen capture can refer to the same originals.
    # Reuse only immutable inputs, while each reader checks its own binding and time.
    with identity_parse_scope(), evidence_read_scope(session), artifact_read_scope(session):
        try:
            await current_identity(session, rule, now)
            capture = await latest_capture(session, rule, now)
        except (ValueError, OSError):
            capture = None
        checked = await record_discovery(
            session,
            account_id=rule.account_id,
            rule_id=rule.id,
            capture_id=None if capture is None else capture.id,
            scheduled_at=scheduled,
        )
    events = checked.observation.get("events")
    notices = (
        sum(isinstance(e, dict) and e.get("classification") == "new" for e in events)
        if prior is None and isinstance(events, list)
        else 0
    )
    return checked, notices


class _BridgeGap(Exception):
    """Only a known pre-publication bridge rejection, never a commit exception."""


async def prepare_registered_research(
    settings: "EngineSettings",
    *,
    account_id: UUID,
    calendar_authority_id: UUID,
    scheduled_at: datetime,
) -> dict[str, object]:
    """Prepare saved earlier-bucket events within host preparation, never dispatch a job.

    Each instrument owns a transaction. Expected source/TTL gaps cannot roll back
    already persisted discovery or other instruments. Commit errors propagate;
    subsequent readback uses the bridge's existing exact event/plan bindings.
    """
    from nexus.agents.research_maintenance import require_research_open
    from nexus.monitoring.durable_sec_research import prepare_durable_sec_research_batch
    from nexus.ops.monitor_disclosure_refresh import _scope
    from nexus.paper.account_transaction_turn import account_transaction_turn
    from nexus.paper.host_probe_state import require_host_admitting
    from nexus.persistence.models.sec_monitor import SecDiscoveryEvent
    from nexus.persistence.session import session_factory

    require_research_open()
    if (
        not settings.agent_paper_cycle_enabled
        or settings.agent_paper_formal_model_config_id is None
        or settings.agent_paper_account_id != account_id
    ):
        raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
    factory = session_factory(settings)
    results: list[dict[str, object]] = []
    expected_gaps = frozenset(
        {
            "MONITOR_AWAITING_RECONCILIATION",
            "DURABLE_SEC_RULE_INACTIVE",
            "DURABLE_SEC_PENDING_EVENTS_DEFERRED",
            "MONITOR_PENDING_OVERFLOW",
            "DURABLE_SEC_RESEARCH_EVIDENCE_INVALID",
        }
    )
    try:
        async with account_transaction_turn(settings), factory() as session, session.begin():
            _, instruments = await _scope(
                session, settings, account_id, calendar_authority_id, scheduled_at
            )
        for instrument_id in instruments:
            try:
                async with (
                    account_transaction_turn(settings),
                    factory() as session,
                    session.begin(),
                ):
                    now, current = await _scope(
                        session, settings, account_id, calendar_authority_id, scheduled_at
                    )
                    if instrument_id not in current:
                        raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
                    await require_host_admitting(session, account_id)
                    rule = await registered_rule(session, account_id, instrument_id)
                    if rule is None:
                        continue
                    await _lock(session, rule)
                    if await rule_state(session, rule) != "active":
                        results.append({"rule_id": str(rule.id), "status": "inactive"})
                        continue
                    pending = await session.scalar(
                        select(SecDiscoveryEvent.id)
                        .where(
                            SecDiscoveryEvent.rule_id == rule.id,
                            SecDiscoveryEvent.first_seen_at < scheduled_at,
                            SecDiscoveryEvent.created_at <= now,
                        )
                        .limit(1)
                    )
                    if pending is None:
                        continue
                    try:
                        batch = await prepare_durable_sec_research_batch(
                            session, account_id=account_id, rule_id=rule.id
                        )
                    except ValueError as error:
                        if str(error) not in expected_gaps:
                            raise
                        raise _BridgeGap(str(error)) from error
                    result: dict[str, object] = {
                        "rule_id": str(rule.id),
                        "status": "empty" if batch is None else "sealed",
                        "batch_id": None if batch is None else str(batch.id),
                    }
                # Record success only after transaction exit confirms the commit.
                results.append(result)
            except _BridgeGap as error:
                results.append(
                    {"instrument_id": str(instrument_id), "status": "blocked", "reason": str(error)}
                )
        return {
            "items": results,
            "model_requests": 0,
            "provider_requests": 0,
            "job_created": False,
            "execution_locked": True,
        }
    finally:
        await factory.kw["bind"].dispose()
