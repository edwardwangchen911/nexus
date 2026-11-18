"""Read immutable discovery facts independently of the old research plan's TTL."""

from datetime import datetime

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.research_disclosure_monitoring import _account
from nexus.api.schemas.durable_sec import (
    DurableSecCheckView,
    DurableSecEventView,
    DurableSecNoticeView,
    DurableSecView,
)
from nexus.domain.time import require_utc
from nexus.monitoring.durable_sec_contracts import DurableSecPolicy
from nexus.notifications.durable_sec import DURABLE_SEC_EVENT, DurableSecNotification
from nexus.outbox.failure import FailureReason, read_delivery_failure
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.runtime import OutboxDeliveryAttempt, OutboxEvent
from nexus.persistence.models.sec_monitor import (
    SecDiscoveryCheck,
    SecDiscoveryEvent,
    SecDiscoveryEventRevision,
    SecMonitorRule,
    SecMonitorRuleState,
    SecNotificationClaim,
)
from nexus.research.sec_disclosure_index import SecIndexEntry

type ViewFact = (
    SecMonitorRule
    | SecMonitorRuleState
    | SecDiscoveryCheck
    | SecDiscoveryEvent
    | SecNotificationClaim
)


async def _audited(session: AsyncSession, row: ViewFact, now: datetime) -> bool:
    # Table names are supplied only by these concrete mapped classes, never user input.
    if type(row) not in (
        SecMonitorRule,
        SecMonitorRuleState,
        SecDiscoveryCheck,
        SecDiscoveryEvent,
        SecNotificationClaim,
    ):
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
    details = observation.get("coverage_details")
    if (
        row.coverage == "complete"
        and isinstance(details, dict)
        and (
            details.get("unread_history_files") != 0
            or details.get("invalid_rows") != 0
            or details.get("coverage_start_known") is not True
        )
    ):
        raise ValueError("DURABLE_SEC_VIEW_INVALID")
    return DurableSecCheckView.model_validate(
        {
            "scheduled_at": row.scheduled_at,
            "checked_at": row.checked_at,
            "result": row.result,
            "coverage": row.coverage,
            "interval_start": start,
            "interval_end": end,
            "reason": {
                "DURABLE_SEC_COVERAGE_INCOMPLETE": "coverage_incomplete",
                "DURABLE_SEC_COVERAGE_CONFLICT": "coverage_conflict",
                "DURABLE_SEC_SOURCE_STALE": "source_stale",
                "DURABLE_SEC_SOURCE_INVALID": "source_invalid",
                "DURABLE_SEC_TIME_INVALID": "time_invalid",
                "DURABLE_SEC_HISTORY_INVALID": "history_invalid",
                "DURABLE_SEC_RULE_INACTIVE": "rule_inactive",
            }.get(row.reason or "", "evidence_unverified")
            if row.result == "unknown"
            else None,
            "coverage_details": details,
        }
    )


def project_event(
    row: SecDiscoveryEvent,
    policy: DurableSecPolicy,
    now: datetime,
    *,
    conflict: bool,
    notice: DurableSecNoticeView | None = None,
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
            "notice": notice,
        }
    )


async def _read_notice(
    session: AsyncSession, event: SecDiscoveryEvent, now: datetime
) -> DurableSecNoticeView | None:
    rows = (
        await session.execute(
            select(SecNotificationClaim, OutboxEvent, OutboxDeliveryAttempt)
            .select_from(SecNotificationClaim)
            .join(OutboxEvent, OutboxEvent.id == SecNotificationClaim.outbox_id)
            .outerjoin(
                OutboxDeliveryAttempt, OutboxDeliveryAttempt.outbox_event_id == OutboxEvent.id
            )
            .where(
                SecNotificationClaim.account_id == event.account_id,
                SecNotificationClaim.instrument_id == event.instrument_id,
                SecNotificationClaim.event_key == event.event_key,
                SecNotificationClaim.legacy_trigger_id.is_(None),
                SecNotificationClaim.created_at <= now,
                OutboxEvent.created_at <= now,
                OutboxEvent.event_type == DURABLE_SEC_EVENT[0],
                OutboxEvent.schema_version == DURABLE_SEC_EVENT[1],
                OutboxEvent.aggregate_id == event.id,
                OutboxEvent.max_attempts == 1,
            )
            .limit(2)
        )
    ).all()
    if len(rows) != 1:
        return None
    claim, outbox, attempt = rows[0]
    if not await _audited(session, claim, now):
        return None
    try:
        frozen = DurableSecNotification.model_validate(outbox.payload)
        entry = SecIndexEntry.model_validate(event.entry)
    except ValueError:
        return None
    if (
        frozen.discovery_id != event.id
        or frozen.instrument_id != event.instrument_id
        or frozen.rule_id != event.rule_id
        or frozen.event_key != event.event_key
        or frozen.form != entry.form
        or frozen.accepted_at != entry.accepted_at
        or frozen.discovered_at != event.first_seen_at
        or not event.created_at <= outbox.created_at <= claim.created_at <= now
    ):
        return None
    if outbox.status == "queued" and outbox.attempt_count == 0 and attempt is None:
        return DurableSecNoticeView(outbox_id=outbox.id, status="queued", attempt_count=0)
    if (
        outbox.attempt_count != 1
        or attempt is None
        or attempt.attempt_no != 1
        or not attempt.worker_id.startswith("bark-notifications-")
        or attempt.started_at < outbox.created_at
    ):
        return None
    finished = attempt.finished_at
    delivering = (
        outbox.status == "delivering" and attempt.outcome == "delivering" and finished is None
    )
    published = (
        outbox.status == "published"
        and finished is not None
        and outbox.published_at == finished
        and attempt.outcome == "published"
        and attempt.error_code is None
        and outbox.last_error_code is None
    )
    dead_letter = (
        outbox.status == "dead_letter"
        and finished is not None
        and outbox.dead_lettered_at == finished
        and attempt.outcome in {"failed", "lost"}
        and attempt.error_code is not None
        and outbox.last_error_code == attempt.error_code
    )
    if not (delivering or published or dead_letter) or (
        finished is not None and finished < attempt.started_at
    ):
        return None
    if max(attempt.started_at, attempt.created_at) > now:
        return DurableSecNoticeView(outbox_id=outbox.id, status="queued", attempt_count=0)
    if finished is None or finished > now:
        return DurableSecNoticeView(
            outbox_id=outbox.id,
            status="delivering",
            attempt_count=1,
            attempted_at=attempt.started_at,
        )
    if published:
        return DurableSecNoticeView(
            outbox_id=outbox.id,
            status="published",
            attempt_count=1,
            attempted_at=attempt.started_at,
            finished_at=finished,
            server_result="accepted",
        )
    detail = (
        await read_delivery_failure(
            session, event_id=outbox.id, attempt_no=1, as_of=now, finished_at=finished
        )
        if attempt.error_code == "PUBLISHER_FAILED"
        else None
    )
    reason = FailureReason(detail["reason"]) if detail is not None else None
    return DurableSecNoticeView(
        outbox_id=outbox.id,
        status="dead_letter",
        attempt_count=1,
        attempted_at=attempt.started_at,
        finished_at=finished,
        failure_reason=reason,
        server_result="http_error" if reason is FailureReason.HTTP_REJECTED else "unconfirmed",
    )


async def read_durable_sec_view(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    now: datetime,
    include_notice: bool = False,
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
            events.append(
                project_event(
                    row,
                    policy,
                    now,
                    conflict=row.id in conflicts,
                    notice=await _read_notice(session, row, now) if include_notice else None,
                )
            )
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
