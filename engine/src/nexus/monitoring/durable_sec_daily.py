"""Unbound SEC events in normal daily input; no synthetic trigger or permanent plan."""

from datetime import datetime
from typing import TYPE_CHECKING, Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import exists, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.monitoring.filing_archive import SOURCE as FILING_SOURCE
from nexus.monitoring.filing_archive import VerifiedFilingContent, read_filing_content
from nexus.monitoring.mixed_contracts import canonical_hash
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.sec_daily import SecDurableDailyEventReceipt
from nexus.persistence.models.sec_monitor import (
    SecDiscoveryEvent,
    SecDiscoveryEventRevision,
    SecMonitorRule,
)
from nexus.persistence.models.sec_research import (
    SecEventConsumptionReceipt,
    SecEventResearchBinding,
)

if TYPE_CHECKING:
    from nexus.monitoring.source_qualification import QualifiedSource

type SecGapReason = Literal[
    "DURABLE_SEC_PENDING_OVERFLOW",
    "DURABLE_SEC_PENDING_EVIDENCE_INVALID",
    "DURABLE_SEC_PENDING_CAPACITY",
]


class PendingSecEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    event_id: UUID
    rule_id: UUID
    event_key: str
    metadata_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    capture_id: UUID
    capture_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    entry: dict[str, object]
    first_seen_at: datetime
    content: VerifiedFilingContent


class PendingSecIssue(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    event_id: UUID
    event_key: str
    reason: Literal["DURABLE_SEC_EVENT_CONFLICT", "DURABLE_SEC_EVENT_EVIDENCE_UNAVAILABLE"]


class DurableSecPendingContext(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    purpose: Literal["durable_sec_pending_v1"] = "durable_sec_pending_v1"
    cycle_id: UUID
    account_id: UUID
    instrument_id: UUID
    decision_time: datetime
    events: tuple[PendingSecEvent, ...] = Field(min_length=1, max_length=32)

    issues: tuple[PendingSecIssue, ...] = Field(default=(), max_length=32)

    @model_validator(mode="after")
    def closed_members(self) -> "DurableSecPendingContext":
        ids = [x.event_id for x in self.events] + [x.event_id for x in self.issues]
        if len(ids) > 32 or len(set(ids)) != len(ids):
            raise ValueError("DURABLE_SEC_PENDING_CONTEXT_INVALID")
        return self

    @property
    def content_hash(self) -> str:
        return canonical_hash(self.model_dump(mode="json"))


class DurableSecPendingGap(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    purpose: Literal["durable_sec_pending_gap_v1"] = "durable_sec_pending_gap_v1"
    observed_at: datetime
    pending_count: int = Field(ge=1)
    issues: tuple[PendingSecIssue, ...] = ()
    reason: Literal[
        "DURABLE_SEC_PENDING_OVERFLOW",
        "DURABLE_SEC_PENDING_EVIDENCE_INVALID",
        "DURABLE_SEC_PENDING_CAPACITY",
    ]


async def freeze_durable_sec_pending(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    instrument_id: UUID,
    as_of: datetime,
    omit_bundle: bool = False,
) -> tuple[DurableSecPendingContext | None, DurableSecPendingGap | None]:
    from nexus.monitoring.durable_sec_host import current_identity, latest_capture
    from nexus.monitoring.durable_sec_research import verify_event_evidence
    from nexus.monitoring.durable_sec_storage import _lock, rule_state
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.research.durable_sec_index_storage import read_durable_index_capture
    from nexus.research.sec_exhibit_storage import configured_exhibit_store

    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if cycle is None:
        raise ValueError("DURABLE_SEC_PENDING_CONTEXT_INVALID")
    events = tuple(
        await session.scalars(
            select(SecDiscoveryEvent)
            .where(
                SecDiscoveryEvent.account_id == cycle.account_id,
                SecDiscoveryEvent.instrument_id == instrument_id,
                SecDiscoveryEvent.first_seen_at <= as_of,
                ~exists(
                    select(SecEventResearchBinding.id).where(
                        SecEventResearchBinding.event_id == SecDiscoveryEvent.id
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
    if not events:
        return None, None
    reason: SecGapReason | None = (
        "DURABLE_SEC_PENDING_CAPACITY"
        if omit_bundle
        else ("DURABLE_SEC_PENDING_OVERFLOW" if len(events) > 32 else None)
    )
    selected: list[PendingSecEvent] = []
    issues: list[PendingSecIssue] = []
    if reason is None:
        try:
            scopes = {}
            for event in events:
                if event.rule_id in scopes:
                    continue
                rule = await session.get(SecMonitorRule, event.rule_id)
                if rule is None:
                    raise ValueError("DURABLE_SEC_PENDING_EVIDENCE_INVALID")
                await _lock(session, rule)
                if await rule_state(session, rule) != "active":
                    raise ValueError("DURABLE_SEC_PENDING_EVIDENCE_INVALID")
                await current_identity(session, rule, as_of)
                latest = await latest_capture(session, rule, as_of)
                if latest is None:
                    raise ValueError("DURABLE_SEC_PENDING_EVIDENCE_INVALID")
                row, capture = await read_durable_index_capture(
                    session, configured_exhibit_store(session), snapshot_id=latest.id, as_of=as_of
                )
                scopes[event.rule_id] = (rule, row, capture)
            for event in events:
                rule, row, capture = scopes[event.rule_id]
                conflict = await session.scalar(
                    select(SecDiscoveryEventRevision.id)
                    .where(SecDiscoveryEventRevision.event_id == event.id)
                    .limit(1)
                )
                if conflict:
                    issues.append(
                        PendingSecIssue(
                            event_id=event.id,
                            event_key=event.event_key,
                            reason="DURABLE_SEC_EVENT_CONFLICT",
                        )
                    )
                    continue
                try:
                    entry = verify_event_evidence(event, capture, now=as_of)
                    sources = tuple(
                        await session.scalars(
                            select(DataSnapshot)
                            .where(
                                DataSnapshot.source == FILING_SOURCE,
                                DataSnapshot.provider_object_id
                                == f"{rule.issuer_cik}:{entry.accession_number}",
                                DataSnapshot.created_at <= as_of,
                                DataSnapshot.available_at <= as_of,
                                DataSnapshot.received_at <= as_of,
                            )
                            .order_by(DataSnapshot.created_at, DataSnapshot.id)
                            .limit(33)
                        )
                    )
                    if not sources or len(sources) > 32:
                        raise ValueError("DURABLE_SEC_EVENT_EVIDENCE_UNAVAILABLE")
                    contents = [
                        await read_filing_content(session, snapshot_id=x.id, as_of=as_of)
                        for x in sources
                    ]
                    if any(
                        c is None
                        or c.archive.instrument_id != instrument_id
                        or c.archive.issuer_cik != rule.issuer_cik
                        or c.archive.entry != entry
                        for c in contents
                    ):
                        raise ValueError("DURABLE_SEC_EVENT_EVIDENCE_UNAVAILABLE")
                    valid_contents = cast(list[VerifiedFilingContent], contents)
                    hashes = {
                        tuple(
                            (d.filename, d.document_type, d.raw_hash) for d in c.archive.documents
                        )
                        for c in valid_contents
                    }
                    if len(hashes) != 1:
                        raise ValueError("DURABLE_SEC_EVENT_EVIDENCE_UNAVAILABLE")
                    selected.append(
                        PendingSecEvent(
                            event_id=event.id,
                            rule_id=event.rule_id,
                            event_key=event.event_key,
                            metadata_hash=event.metadata_hash,
                            capture_id=row.id,
                            capture_hash=row.revision,
                            entry=event.entry,
                            first_seen_at=event.first_seen_at,
                            content=valid_contents[0],
                        )
                    )
                except ValueError:
                    issues.append(
                        PendingSecIssue(
                            event_id=event.id,
                            event_key=event.event_key,
                            reason="DURABLE_SEC_EVENT_EVIDENCE_UNAVAILABLE",
                        )
                    )
        except ValueError:
            reason = "DURABLE_SEC_PENDING_EVIDENCE_INVALID"
    if not selected and reason is None:
        reason = "DURABLE_SEC_PENDING_EVIDENCE_INVALID"
    if reason:
        return None, DurableSecPendingGap(
            observed_at=as_of, pending_count=len(events), reason=reason, issues=tuple(issues)
        )
    context = DurableSecPendingContext(
        cycle_id=cycle_id,
        account_id=cycle.account_id,
        instrument_id=instrument_id,
        decision_time=as_of,
        events=tuple(selected),
        issues=tuple(issues),
    )
    if len(__import__("json").dumps(context.model_dump(mode="json")).encode()) > 256 * 1024:
        return None, DurableSecPendingGap(
            observed_at=as_of, pending_count=len(events), reason="DURABLE_SEC_PENDING_CAPACITY"
        )
    return context, None


async def validate_durable_sec_pending(
    session: AsyncSession, context: DurableSecPendingContext
) -> bool:
    """Historical raw custody, without rewriting today's state into the old input."""
    from nexus.monitoring.durable_sec_research import verify_event_evidence
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.research.durable_sec_index_storage import read_durable_index_capture
    from nexus.research.sec_exhibit_storage import configured_exhibit_store

    cycle = await session.get(AgentPaperCycleRecord, context.cycle_id)
    if (
        cycle is None
        or cycle.account_id != context.account_id
        or len({x.event_id for x in context.events}) != len(context.events)
    ):
        return False
    for issue in context.issues:
        original = await session.get(SecDiscoveryEvent, issue.event_id)
        if (
            original is None
            or (original.account_id, original.instrument_id, original.event_key)
            != (context.account_id, context.instrument_id, issue.event_key)
            or original.first_seen_at > context.decision_time
        ):
            return False
    for item in context.events:
        event = await session.get(SecDiscoveryEvent, item.event_id)
        if event is None or (
            event.account_id,
            event.instrument_id,
            event.rule_id,
            event.event_key,
            event.metadata_hash,
            event.entry,
            event.first_seen_at,
        ) != (
            context.account_id,
            context.instrument_id,
            item.rule_id,
            item.event_key,
            item.metadata_hash,
            item.entry,
            item.first_seen_at,
        ):
            return False
        try:
            row, capture = await read_durable_index_capture(
                session,
                configured_exhibit_store(session),
                snapshot_id=item.capture_id,
                as_of=context.decision_time,
            )
            verify_event_evidence(event, capture, now=context.decision_time)
            content = await read_filing_content(
                session, snapshot_id=item.content.snapshot_id, as_of=context.decision_time
            )
            if row.revision != item.capture_hash or content != item.content:
                return False
        except ValueError:
            return False
    return True


async def record_durable_sec_daily_consumption(
    session: AsyncSession, *, qualified: "QualifiedSource", account_id: UUID, now: datetime
) -> int:
    from nexus.agents.paper_context import AgentPaperResearchContext
    from nexus.audit.service import append_audit_event
    from nexus.domain.ids import new_uuid7
    from nexus.monitoring.durable_sec_daily_contracts import compile_durable_sec_pending_proposal
    from nexus.monitoring.durable_sec_storage import _lock, rule_state

    paper = qualified.context
    if (
        not isinstance(paper, AgentPaperResearchContext)
        or paper.pending_durable_sec is None
        or qualified.intent.durable_sec_pending_proposal is None
    ):
        return 0
    context = paper.pending_durable_sec
    if context.account_id != account_id or not await validate_durable_sec_pending(session, context):
        raise ValueError("DURABLE_SEC_DAILY_CONSUMPTION_INVALID")
    roles = {r.role: r for r in qualified.role_outputs}
    proposal = qualified.intent.durable_sec_pending_proposal
    if roles["orchestrator"].durable_sec_pending_proposal != proposal:
        raise ValueError("DURABLE_SEC_DAILY_CONSUMPTION_INVALID")
    bear_review = roles["bear"].durable_sec_pending_review
    audit_review = roles["evidence_audit"].durable_sec_pending_review
    if bear_review is None or audit_review is None:
        raise ValueError("DURABLE_SEC_DAILY_CONSUMPTION_INVALID")
    reviews = {"bear": bear_review, "evidence_audit": audit_review}
    compile_durable_sec_pending_proposal(
        proposal,
        reviews,
        context_hash=context.content_hash,
        event_ids=tuple(x.event_id for x in context.events),
        evidence_refs=qualified.evidence,
        audit_vote=roles["evidence_audit"].vote,
        adoption_hash=roles["portfolio"].durable_sec_pending_adoption_hash,
    )
    count = 0
    for item in proposal.items:
        if item.status != "addressed":
            continue
        event = await session.get(SecDiscoveryEvent, item.event_id)
        if event is None:
            raise ValueError("DURABLE_SEC_DAILY_CONSUMPTION_INVALID")
        rule = await session.get(SecMonitorRule, event.rule_id)
        if rule is None:
            raise ValueError("DURABLE_SEC_DAILY_CONSUMPTION_INVALID")
        await _lock(session, rule)
        if await rule_state(session, rule) != "active" or await session.scalar(
            select(SecDiscoveryEventRevision.id)
            .where(SecDiscoveryEventRevision.event_id == event.id)
            .limit(1)
        ):
            continue
        old = await session.scalar(
            select(SecDurableDailyEventReceipt).where(
                SecDurableDailyEventReceipt.event_id == event.id
            )
        )
        if old is not None:
            if old.run_id != qualified.run.id:
                raise ValueError("DURABLE_SEC_ALREADY_CONSUMED")
            continue
        if await session.scalar(
            select(SecEventResearchBinding.id)
            .where(SecEventResearchBinding.event_id == event.id)
            .limit(1)
        ):
            raise ValueError("DURABLE_SEC_ALREADY_BOUND")
        row = SecDurableDailyEventReceipt(
            id=new_uuid7(),
            created_at=now,
            event_id=event.id,
            cycle_id=paper.cycle_id,
            run_id=qualified.run.id,
            output_id=qualified.output.id,
            snapshot_id=qualified.run.data_snapshot_id,
            context_hash=context.content_hash,
            proposal_hash=proposal.content_hash,
            review_hash=canonical_hash({k: v.model_dump(mode="json") for k, v in reviews.items()}),
            observed_at=now,
        )
        session.add(row)
        await session.flush()
        binding = await session.scalar(
            text(
                "SELECT to_jsonb(x)-'created_at' "
                "FROM sec_durable_daily_event_receipts x WHERE id=:id"
            ),
            {"id": row.id},
        )
        await append_audit_event(
            session,
            actor_id="research-monitor",
            action="research.monitor.sec_daily_consumed",
            schema_version="durable_sec_pending_v1",
            occurred_at=now,
            payload={"row_id": str(row.id), "binding": binding},
        )
        count += 1
    return count
