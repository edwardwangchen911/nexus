"""Redacted operational read model for the private risk-audit console."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, cast

from sqlalchemy import desc, func, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.schemas.risk_audit import (
    DrawdownLockSummary,
    FailedJobSummary,
    RetryableJobSummary,
    RiskAuditEventResponse,
    RiskAuditPageResponse,
    RiskAuditSummary,
)
from nexus.audit.cursor import RiskAuditCursorCodec, RiskAuditCursorState
from nexus.audit.export import audit_kind_condition, redacted_audit_document
from nexus.audit.redaction import AuditKind
from nexus.domain.time import require_utc
from nexus.persistence.models.paper import (
    PaperAccountRecord,
    PaperDrawdownLockEventRecord,
    PaperPerformanceSnapshotRecord,
)
from nexus.persistence.models.runtime import AuditEvent, Job

_MAX_RANGE = timedelta(days=31)


class RiskAuditReadService:
    def __init__(
        self,
        session: AsyncSession,
        now: datetime,
        *,
        cursor_signing_secrets: tuple[str, ...],
    ) -> None:
        self._session = session
        self._now = require_utc(now)
        self._cursor = RiskAuditCursorCodec(cursor_signing_secrets, self._now)

    async def page(
        self,
        *,
        kind: AuditKind | None,
        from_at: datetime | None,
        to_at: datetime | None,
        cursor: str | None,
        limit: int,
    ) -> RiskAuditPageResponse:
        if not 1 <= limit <= 100:
            raise ValueError("risk-audit limit is invalid")
        cursor_state = (
            self._cursor.decode(
                cursor,
                requested_from=from_at,
                requested_to=to_at,
                requested_kind=kind,
            )
            if cursor is not None
            else None
        )
        decision_time = cursor_state.decision_time if cursor_state else self._now
        selected_from = (
            cursor_state.from_at
            if cursor_state
            else require_utc(from_at)
            if from_at is not None
            else decision_time - timedelta(days=7)
        )
        selected_to = (
            cursor_state.to_at
            if cursor_state
            else require_utc(to_at)
            if to_at is not None
            else decision_time + timedelta(microseconds=1)
        )
        if selected_to <= selected_from or selected_to - selected_from > _MAX_RANGE:
            raise ValueError("risk-audit range is invalid")
        conditions = [
            AuditEvent.occurred_at >= selected_from,
            AuditEvent.occurred_at < selected_to,
            AuditEvent.occurred_at <= decision_time,
        ]
        if kind is not None:
            conditions.append(audit_kind_condition(kind))
        if cursor_state is not None:
            conditions.append(
                tuple_(AuditEvent.occurred_at, AuditEvent.id)
                < (cursor_state.after_occurred_at, cursor_state.after_event_id)
            )
        rows = tuple(
            (
                await self._session.scalars(
                    select(AuditEvent)
                    .where(*conditions)
                    .order_by(desc(AuditEvent.occurred_at), desc(AuditEvent.id))
                    .limit(limit + 1)
                )
            ).all()
        )
        visible = rows[:limit]
        next_cursor = None
        if len(rows) > limit and visible:
            last = visible[-1]
            next_cursor = self._cursor.encode(
                RiskAuditCursorState(
                    decision_time=decision_time,
                    from_at=selected_from,
                    to_at=selected_to,
                    kind=kind,
                    after_occurred_at=last.occurred_at,
                    after_event_id=last.id,
                )
            )
        return RiskAuditPageResponse(
            summary=await self._summary(),
            items=[
                RiskAuditEventResponse.model_validate(redacted_audit_document(event))
                for event in visible
            ],
            next_cursor=next_cursor,
        )

    async def _summary(self) -> RiskAuditSummary:
        return RiskAuditSummary(
            data_quality=await self._data_quality(),
            drawdown_lock=await self._drawdown_lock(),
            failed_jobs=await self._failed_jobs(),
        )

    async def _data_quality(self) -> Literal["PASS", "PARTIAL", "FAIL", "UNKNOWN"]:
        events = tuple(
            (
                await self._session.scalars(
                    select(AuditEvent)
                    .where(
                        or_(
                            AuditEvent.action.startswith("quality."),
                            AuditEvent.action.startswith("source."),
                        ),
                        AuditEvent.occurred_at <= self._now,
                    )
                    .order_by(desc(AuditEvent.occurred_at), desc(AuditEvent.id))
                    .limit(20)
                )
            ).all()
        )
        for event in events:
            value = event.payload.get("quality_status", event.payload.get("status"))
            if isinstance(value, str) and value.upper() in {"PASS", "PARTIAL", "FAIL"}:
                return cast(Literal["PASS", "PARTIAL", "FAIL"], value.upper())
        return "UNKNOWN"

    async def _drawdown_lock(self) -> DrawdownLockSummary:
        accounts = tuple(
            (
                await self._session.scalars(
                    select(PaperAccountRecord)
                    .where(PaperAccountRecord.capability == "simulation_write_internal")
                    .order_by(PaperAccountRecord.id)
                    .limit(2)
                )
            ).all()
        )
        if len(accounts) != 1:
            return DrawdownLockSummary(state="unknown", event_seq=None, acknowledge_allowed=False)
        account = accounts[0]
        event = await self._session.scalar(
            select(PaperDrawdownLockEventRecord)
            .where(PaperDrawdownLockEventRecord.account_id == account.id)
            .order_by(desc(PaperDrawdownLockEventRecord.event_seq))
            .limit(1)
        )
        if event is None:
            return DrawdownLockSummary(state="unknown", event_seq=None, acknowledge_allowed=False)
        performance = await self._session.scalar(
            select(PaperPerformanceSnapshotRecord)
            .where(PaperPerformanceSnapshotRecord.account_id == account.id)
            .order_by(
                desc(PaperPerformanceSnapshotRecord.as_of), desc(PaperPerformanceSnapshotRecord.id)
            )
            .limit(1)
        )
        allowed = (
            event.to_state == "recovery_eligible"
            and performance is not None
            and performance.drawdown_ratio < Decimal("0.06")
        )
        return DrawdownLockSummary(
            state=cast(Literal["unlocked", "locked", "recovery_eligible"], event.to_state),
            event_seq=event.event_seq,
            acknowledge_allowed=allowed,
        )

    async def _failed_jobs(self) -> FailedJobSummary:
        terminal = ("failed", "dead_letter")
        count = int(
            await self._session.scalar(
                select(func.count()).select_from(Job).where(Job.status.in_(terminal))
            )
            or 0
        )
        rows = tuple(
            (
                await self._session.scalars(
                    select(Job)
                    .where(
                        Job.status.in_(terminal),
                        Job.retry_policy == "manual_safe",
                        Job.manual_retry_count < 2,
                    )
                    .order_by(desc(Job.finished_at), desc(Job.id))
                    .limit(20)
                )
            ).all()
        )
        return FailedJobSummary(
            count=count,
            retryable=[
                RetryableJobSummary(
                    job_id=row.id,
                    job_type=row.job_type,
                    status=cast(Literal["failed", "dead_letter"], row.status),
                    version=row.version,
                    manual_retry_count=row.manual_retry_count,
                    last_error_code=row.last_error_code,
                    finished_at=cast(datetime, row.finished_at),
                )
                for row in rows
            ],
        )
