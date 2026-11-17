"""Read immutable discovery facts independently of the old research plan's TTL."""

from datetime import datetime

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.research_disclosure_monitoring import _account
from nexus.api.schemas.durable_sec import DurableSecCheckView, DurableSecEventView, DurableSecView
from nexus.domain.time import require_utc
from nexus.monitoring.durable_sec_contracts import DurableSecPolicy
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.sec_monitor import (
    SecDiscoveryCheck,
    SecDiscoveryEvent,
    SecDiscoveryEventRevision,
    SecMonitorRule,
    SecMonitorRuleState,
)
from nexus.research.sec_disclosure_index import SecIndexEntry

type ViewFact = SecMonitorRule | SecMonitorRuleState | SecDiscoveryCheck | SecDiscoveryEvent


async def _audited(session: AsyncSession, row: ViewFact, now: datetime) -> bool:
    # Table names are supplied only by these concrete mapped classes, never user input.
    if type(row) not in (SecMonitorRule, SecMonitorRuleState, SecDiscoveryCheck, SecDiscoveryEvent):
        return False
    if row.created_at > now:
        return False
    return bool(
        await session.scalar(
            text(
                "SELECT EXISTS (SELECT 1 FROM audit_events a JOIN " + row.__tablename__ + " f "
                "ON f.id=:id WHERE a.actor_id='research-monitor' "
                "AND a.action='research.monitor.durable_fact' "
                "AND a.schema_version='durable_sec_v1' "
                "AND a.occurred_at=f.created_at AND a.created_at<=:now "
                "AND a.payload->>'table'=:table AND a.payload->>'row_id'=:row_id "
                "AND a.payload->'binding'=to_jsonb(f)-'created_at')"
            ),
            {"id": row.id, "row_id": str(row.id), "table": row.__tablename__, "now": now},
        )
    )


def project_check(row: SecDiscoveryCheck, now: datetime) -> DurableSecCheckView:
    if not row.scheduled_at <= row.checked_at <= now or row.created_at > now:
        raise ValueError("DURABLE_SEC_VIEW_INVALID")
    if row.result == "false" and row.coverage != "complete":
        raise ValueError("DURABLE_SEC_VIEW_INVALID")
    observation = row.observation or {}
    start, end = observation.get("interval_start"), observation.get("interval_end")
    if start is not None or end is not None:
        if not isinstance(start, str) or not isinstance(end, str):
            raise ValueError("DURABLE_SEC_VIEW_INVALID")
        start = require_utc(datetime.fromisoformat(start))
        end = require_utc(datetime.fromisoformat(end))
        if not start <= end <= row.checked_at:
            raise ValueError("DURABLE_SEC_VIEW_INVALID")
    if row.result == "false" and (start is None or end is None):
        raise ValueError("DURABLE_SEC_VIEW_INVALID")
    return DurableSecCheckView.model_validate(
        {
            "scheduled_at": row.scheduled_at,
            "checked_at": row.checked_at,
            "result": row.result,
            "coverage": row.coverage,
            "interval_start": start,
            "interval_end": end,
            "reason": "evidence_unverified" if row.result == "unknown" else None,
        }
    )


def project_event(
    row: SecDiscoveryEvent, policy: DurableSecPolicy, now: datetime, *, conflict: bool
) -> DurableSecEventView:
    entry = SecIndexEntry.model_validate(row.entry)
    if (
        row.account_id != policy.account_id
        or row.instrument_id != policy.instrument_id
        or row.event_key != f"sec:{policy.issuer_cik}:{entry.accession_number}"
        or entry.form not in policy.forms
        or not policy.effective_at < entry.accepted_at <= row.first_seen_at <= now
        or row.created_at > now
    ):
        raise ValueError("DURABLE_SEC_VIEW_INVALID")
    return DurableSecEventView.model_validate(
        {
            "event_key": row.event_key,
            "form": entry.form,
            "accepted_at": entry.accepted_at,
            "first_seen_at": row.first_seen_at,
            "metadata_status": "conflict" if conflict else "recorded",
        }
    )


async def read_durable_sec_view(
    session: AsyncSession, run: AgentResearchRunRecord, *, now: datetime
) -> DurableSecView | None:
    now = require_utc(now)
    account_id = await _account(session, run, now)
    if account_id is None or max(run.created_at, run.decision_time) > now:
        return None
    rule = await session.scalar(
        select(SecMonitorRule).where(
            SecMonitorRule.account_id == account_id,
            SecMonitorRule.instrument_id == run.instrument_id,
            SecMonitorRule.created_at <= now,
        )
    )
    if rule is None:
        return None
    unknown = DurableSecView(observed_at=now, rule_id=rule.id, state="unknown")
    state = await session.scalar(
        select(SecMonitorRuleState)
        .where(
            SecMonitorRuleState.rule_id == rule.id,
        )
        .order_by(SecMonitorRuleState.sequence.desc())
        .limit(1)
    )
    if (
        state is None
        or state.effective_at > now
        or not await _audited(session, rule, now)
        or not await _audited(session, state, now)
    ):
        return unknown
    try:
        policy = DurableSecPolicy.model_validate(
            {
                "account_id": account_id,
                "instrument_id": run.instrument_id,
                "issuer_cik": rule.issuer_cik,
                "forms": rule.forms,
                "effective_at": rule.effective_at,
                "policy_version": rule.policy_version,
            }
        )
        if policy.effective_at > now:
            return unknown
        check = await session.scalar(
            select(SecDiscoveryCheck)
            .where(
                SecDiscoveryCheck.rule_id == rule.id,
                SecDiscoveryCheck.created_at <= now,
            )
            .order_by(SecDiscoveryCheck.checked_at.desc(), SecDiscoveryCheck.id.desc())
            .limit(1)
        )
        if check is not None and not await _audited(session, check, now):
            return unknown
        event_scope = (SecDiscoveryEvent.rule_id == rule.id, SecDiscoveryEvent.created_at <= now)
        count = await session.scalar(
            select(func.count()).select_from(SecDiscoveryEvent).where(*event_scope)
        )
        rows = list(
            (
                await session.scalars(
                    select(SecDiscoveryEvent)
                    .where(*event_scope)
                    .order_by(SecDiscoveryEvent.first_seen_at.desc(), SecDiscoveryEvent.id.desc())
                    .limit(5)
                )
            ).all()
        )
        conflicts = (
            set(
                (
                    await session.scalars(
                        select(SecDiscoveryEventRevision.event_id).where(
                            SecDiscoveryEventRevision.event_id.in_([row.id for row in rows]),
                            SecDiscoveryEventRevision.created_at <= now,
                        )
                    )
                ).all()
            )
            if rows
            else set()
        )
        events = []
        for row in rows:
            if not await _audited(session, row, now):
                return unknown
            events.append(project_event(row, policy, now, conflict=row.id in conflicts))
        return DurableSecView.model_validate(
            {
                "observed_at": now,
                "rule_id": rule.id,
                "state": state.state,
                "effective_at": policy.effective_at,
                "forms": policy.forms,
                "latest_check": project_check(check, now) if check else None,
                "event_count": count or 0,
                "events": events,
            }
        )
    except ValueError:
        return unknown
