"""Create and read bounded explicit research scopes for Agent paper cycles."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.persistence import select_authoritative_snapshot
from nexus.domain.ids import new_uuid7
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.paper.agent_cycle_authority import select_agent_paper_service_calendar_authority
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_V1
from nexus.paper.universe_scope import is_formal_agent_paper_universe
from nexus.persistence.idempotency import IdempotencyClaimError, claim_idempotency
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.data import PriceBarRecord, PriceBarSetAuthority, PriceSeriesManifest
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    SectorExposure,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    AgentPaperCandidateBatchRecord,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperAccountRecord,
)
from nexus.persistence.models.portfolio import Account
from nexus.persistence.models.runtime import IdempotencyRecord

_ROUTE = "agent-paper-candidate-batch-v1"
_SCHEMA = "agent_paper_candidate_batch_v1"
_SOURCE = "explicit_research_scope"
_SUPPORTED_ASSET_TYPES = frozenset({"ETF", "common_stock"})


class CandidateBatchInput(BaseModel):
    """Canonical UUID-only candidate scope."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    instrument_ids: tuple[UUID, ...]

    @field_validator("instrument_ids", mode="before")
    @classmethod
    def _canonical_ids(cls, value: object) -> tuple[UUID, ...]:
        if not isinstance(value, list | tuple):
            raise ValueError("CANDIDATE_BATCH_INVALID")
        instrument_ids = tuple(value)
        if (
            not 1 <= len(instrument_ids) <= 5
            or any(not isinstance(item, UUID) for item in instrument_ids)
            or len(set(instrument_ids)) != len(instrument_ids)
        ):
            raise ValueError("CANDIDATE_BATCH_INVALID")
        return tuple(sorted(instrument_ids, key=lambda item: item.int))


class CandidateInstrumentIdentity(BaseModel):
    """Human-readable identity resolved from the approved local instrument master."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    universe_snapshot_id: UUID | None = Field(default=None, exclude=True)
    symbol: str
    asset_type: str
    exchange: str
    currency: str


class CandidateBatchResult(BaseModel):
    """Exact persisted candidate-batch result used for replay and readback."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    batch_id: UUID
    account_id: UUID
    universe_snapshot_id: UUID
    instrument_ids: tuple[UUID, ...]
    source: str
    batch_hash: str
    created_at: datetime


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("CANDIDATE_BATCH_INVALID")
    return value


def _canonical_hash(
    *, account_id: UUID, universe_snapshot_id: UUID, instrument_ids: tuple[UUID, ...]
) -> str:
    payload = {
        "account_id": str(account_id),
        "candidate_instrument_ids": [str(item) for item in instrument_ids],
        "schema": _SCHEMA,
        "source": _SOURCE,
        "universe_snapshot_id": str(universe_snapshot_id),
    }
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


async def resolve_agent_paper_candidate_identities(
    session: AsyncSession,
    *,
    universe_snapshot_id: UUID,
    instrument_ids: tuple[UUID, ...],
    created_at: datetime,
) -> tuple[CandidateInstrumentIdentity, ...]:
    """Resolve one approved point-in-time universe scope without external calls."""
    created_at = _require_utc(created_at)
    canonical = CandidateBatchInput(instrument_ids=instrument_ids).instrument_ids
    universe = await session.scalar(
        select(UniverseSnapshot).where(
            UniverseSnapshot.id == universe_snapshot_id,
            UniverseSnapshot.effective_from <= created_at.date(),
            UniverseSnapshot.published_at.is_not(None),
            UniverseSnapshot.published_at <= created_at,
            UniverseSnapshot.created_at <= created_at,
        )
    )
    if universe is None or not is_formal_agent_paper_universe(universe):
        raise ValueError("CANDIDATE_BATCH_INVALID")
    rows = tuple(
        (
            await session.execute(
                select(Instrument, InstrumentAlias)
                .join(
                    UniverseMember,
                    UniverseMember.instrument_id == Instrument.id,
                )
                .join(
                    InstrumentAlias,
                    InstrumentAlias.instrument_id == Instrument.id,
                )
                .where(
                    UniverseMember.universe_snapshot_id == universe_snapshot_id,
                    UniverseMember.instrument_id.in_(canonical),
                    UniverseMember.created_at <= created_at,
                    Instrument.id.in_(canonical),
                    Instrument.created_at <= created_at,
                    InstrumentAlias.alias_type == "ticker",
                    InstrumentAlias.valid_from <= created_at.date(),
                    (InstrumentAlias.valid_to.is_(None))
                    | (InstrumentAlias.valid_to >= created_at.date()),
                    InstrumentAlias.created_at <= created_at,
                )
                .order_by(Instrument.id, InstrumentAlias.symbol)
            )
        ).all()
    )
    by_instrument: dict[UUID, list[tuple[Instrument, InstrumentAlias]]] = {}
    for instrument, alias in rows:
        by_instrument.setdefault(instrument.id, []).append((instrument, alias))
    if set(by_instrument) != set(canonical) or any(
        len(entries) != 1 for entries in by_instrument.values()
    ):
        raise ValueError("CANDIDATE_BATCH_INVALID")
    identities = tuple(
        CandidateInstrumentIdentity(
            instrument_id=instrument_id,
            universe_snapshot_id=universe_snapshot_id,
            symbol=by_instrument[instrument_id][0][1].symbol,
            asset_type=by_instrument[instrument_id][0][0].asset_type,
            exchange=by_instrument[instrument_id][0][0].exchange,
            currency=by_instrument[instrument_id][0][0].currency,
        )
        for instrument_id in canonical
    )
    if any(
        identity.currency != "USD"
        or identity.exchange not in US_EQUITY_EXCHANGES
        or identity.asset_type not in _SUPPORTED_ASSET_TYPES
        for identity in identities
    ) or len({identity.symbol for identity in identities}) != len(identities):
        raise ValueError("CANDIDATE_BATCH_INVALID")
    return identities


def _result(record: AgentPaperCandidateBatchRecord) -> CandidateBatchResult:
    try:
        canonical = CandidateBatchInput(
            instrument_ids=tuple(record.candidate_instrument_ids)
        ).instrument_ids
    except ValueError:
        raise ValueError("CANDIDATE_BATCH_INVALID") from None
    expected_hash = _canonical_hash(
        account_id=record.account_id,
        universe_snapshot_id=record.universe_snapshot_id,
        instrument_ids=canonical,
    )
    if (
        tuple(record.candidate_instrument_ids) != canonical
        or record.source != _SOURCE
        or record.batch_hash != expected_hash
    ):
        raise ValueError("CANDIDATE_BATCH_INVALID")
    return CandidateBatchResult(
        batch_id=record.id,
        account_id=record.account_id,
        universe_snapshot_id=record.universe_snapshot_id,
        instrument_ids=canonical,
        source=record.source,
        batch_hash=record.batch_hash,
        created_at=record.created_at,
    )


async def load_agent_paper_candidate_batch(
    session: AsyncSession, *, batch_id: UUID
) -> CandidateBatchResult | None:
    """Read one immutable candidate batch and verify its canonical hash."""
    record = await session.get(AgentPaperCandidateBatchRecord, batch_id)
    return None if record is None else _result(record)


async def validate_agent_paper_candidate_batch(
    session: AsyncSession,
    *,
    batch_id: UUID | None,
    account_id: UUID,
    as_of: datetime,
    universe_snapshot_id: UUID | None = None,
) -> tuple[CandidateBatchResult, tuple[CandidateInstrumentIdentity, ...]]:
    """Revalidate the complete explicit scope before a new cycle can collect evidence."""
    if batch_id is None:
        raise ValueError("CANDIDATE_BATCH_REQUIRED")
    as_of = _require_utc(as_of)
    batch = await load_agent_paper_candidate_batch(session, batch_id=batch_id)
    account = await session.get(PaperAccountRecord, account_id)
    underlying = None if account is None else await session.get(Account, account.account_id)
    if (
        batch is None
        or batch.account_id != account_id
        or batch.created_at > as_of
        or (universe_snapshot_id is not None and batch.universe_snapshot_id != universe_snapshot_id)
        or account is None
        or account.capability != "simulation_write_internal"
        or account.currency != "USD"
        or account.created_at > as_of
        or underlying is None
        or underlying.provider != "internal"
        or underlying.status != "ACTIVE"
        or underlying.created_at > as_of
    ):
        raise ValueError("CANDIDATE_BATCH_INVALID")
    identities = await resolve_agent_paper_candidate_identities(
        session,
        universe_snapshot_id=batch.universe_snapshot_id,
        instrument_ids=batch.instrument_ids,
        created_at=as_of,
    )
    return batch, identities


async def inspect_agent_paper_candidate_static_readiness(
    session: AsyncSession,
    *,
    account_id: UUID,
    batch_id: UUID,
    as_of: datetime,
) -> dict[str, object]:
    """Read candidate evidence before the one daily formal decision window."""
    as_of = _require_utc(as_of)
    batch, identities = await validate_agent_paper_candidate_batch(
        session, batch_id=batch_id, account_id=account_id, as_of=as_of
    )
    calendar = await select_agent_paper_service_calendar_authority(session, now=as_of)
    prior_session = await session.scalar(
        select(ExchangeCalendarSessionRecord.session)
        .where(
            ExchangeCalendarSessionRecord.calendar_authority_id == calendar.calendar_authority_id,
            ExchangeCalendarSessionRecord.session < calendar.session,
            ExchangeCalendarSessionRecord.created_at <= as_of,
        )
        .order_by(ExchangeCalendarSessionRecord.session.desc())
        .limit(1)
    )
    if prior_session is None:
        raise ValueError("AGENT_PAPER_CALENDAR_PRIOR_SESSION_MISSING")
    scheduled_slot = datetime.combine(
        calendar.local_date,
        PORTFOLIO_ALLOCATION_V1.decision_local_time,
        tzinfo=ZoneInfo(PORTFOLIO_ALLOCATION_V1.decision_timezone),
    ).astimezone(UTC)
    scheduled_claim = await session.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == f"local-agent-paper-schwab:{account_id}",
            IdempotencyRecord.route == "agent-paper-schwab-trigger-v2",
            IdempotencyRecord.idempotency_key == scheduled_slot.isoformat(),
            IdempotencyRecord.schema_version == "agent_paper_schwab_trigger_v2",
            IdempotencyRecord.created_at <= as_of,
        )
    )
    scheduled_payload = None if scheduled_claim is None else scheduled_claim.response_payload
    scheduled_trigger: dict[str, object] | None = None
    if scheduled_claim is not None:
        outcome = scheduled_payload.get("outcome") if isinstance(scheduled_payload, dict) else None
        scheduled_trigger = {
            "status": outcome if outcome in {"completed", "failed"} else "unknown",
            "error_code": (
                scheduled_payload.get("error_code")
                if isinstance(scheduled_payload, dict)
                and isinstance(scheduled_payload.get("error_code"), str)
                else None
            ),
        }
    current_cycles = tuple(
        (
            await session.scalars(
                select(AgentPaperCycleRecord)
                .where(
                    AgentPaperCycleRecord.account_id == account_id,
                    AgentPaperCycleRecord.decision_session == calendar.local_date,
                    AgentPaperCycleRecord.candidate_batch_id.is_not(None),
                    AgentPaperCycleRecord.created_at <= as_of,
                )
                .order_by(AgentPaperCycleRecord.decision_slot_at, AgentPaperCycleRecord.id)
            )
        ).all()
    )
    current_session_cycles: list[dict[str, object]] = []
    for cycle in current_cycles:
        event = await session.scalar(
            select(AgentPaperCycleEventRecord)
            .where(
                AgentPaperCycleEventRecord.cycle_id == cycle.id,
                AgentPaperCycleEventRecord.occurred_at <= as_of,
            )
            .order_by(AgentPaperCycleEventRecord.sequence.desc())
            .limit(1)
        )
        current_session_cycles.append(
            {
                "cycle_id": str(cycle.id),
                "status": event.to_status if event is not None else "unknown",
                "result_code": event.result_code if event is not None else None,
            }
        )
    selected_universe = await session.scalar(
        select(UniverseSnapshot)
        .where(
            or_(
                (UniverseSnapshot.key == "us_liquid")
                & (UniverseSnapshot.source == "fosun_local_read_only"),
                (UniverseSnapshot.key == "agent_paper_research")
                & (UniverseSnapshot.source == "nexus_local_reviewed_research"),
            ),
            UniverseSnapshot.effective_from <= calendar.session,
            UniverseSnapshot.published_at <= as_of,
            UniverseSnapshot.created_at <= as_of,
        )
        .order_by(UniverseSnapshot.published_at.desc(), UniverseSnapshot.id.desc())
        .limit(1)
    )
    universe_matches = (
        selected_universe is not None
        and is_formal_agent_paper_universe(selected_universe)
        and selected_universe.id == batch.universe_snapshot_id
    )
    candidates: list[dict[str, object]] = []
    for identity in sorted(identities, key=lambda item: item.symbol):
        missing: list[str] = []
        snapshot = await select_authoritative_snapshot(
            session, instrument_id=identity.instrument_id, as_of=as_of
        )
        if (
            snapshot is None
            or snapshot.source != "nexus_research"
            or snapshot.schema_version != "research_filings_v1"
        ):
            missing.append("RESEARCH_SNAPSHOT_MISSING")
        reviewed_sectors = tuple(
            await session.scalars(
                select(SectorExposure)
                .where(
                    SectorExposure.instrument_id == identity.instrument_id,
                    SectorExposure.as_of <= calendar.session,
                    SectorExposure.reviewed.is_(True),
                    SectorExposure.synthetic.is_(False),
                    SectorExposure.created_at <= as_of,
                )
                .order_by(SectorExposure.as_of.desc(), SectorExposure.id)
            )
        )
        latest_sectors = (
            tuple(row for row in reviewed_sectors if row.as_of == reviewed_sectors[0].as_of)
            if reviewed_sectors
            else ()
        )
        sector_ready = (
            bool(latest_sectors)
            and (identity.asset_type != "common_stock" or len(latest_sectors) == 1)
            and sum((row.weight for row in latest_sectors), start=Decimal(0)) == Decimal(1)
            and all(
                row.weight > 0
                and bool(row.sector_code)
                and len(row.content_hash) == 64
                and all(character in "0123456789abcdef" for character in row.content_hash)
                for row in latest_sectors
            )
        )
        if not sector_ready:
            missing.append("REVIEWED_SECTOR_MISSING")
        daily_authority = await session.scalar(
            select(PriceBarSetAuthority.id)
            .join(
                PriceSeriesManifest,
                PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
            )
            .join(
                PriceBarRecord,
                PriceBarRecord.price_series_manifest_id == PriceSeriesManifest.id,
            )
            .where(
                PriceBarSetAuthority.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
                PriceBarSetAuthority.available_at <= as_of,
                PriceBarSetAuthority.created_at <= as_of,
                PriceBarRecord.instrument_id == identity.instrument_id,
                PriceBarRecord.session == prior_session,
                PriceBarRecord.available_at <= as_of,
                PriceBarRecord.created_at <= as_of,
                PriceSeriesManifest.created_at <= as_of,
                PriceSeriesManifest.source == "alpaca",
                PriceSeriesManifest.feed == "sip",
                PriceSeriesManifest.delivery_mode == "delayed",
                PriceSeriesManifest.adjustment == "all",
                PriceSeriesManifest.interval == "1Day",
            )
            .limit(1)
        )
        if daily_authority is None:
            missing.append("PRIOR_DAILY_AUTHORITY_MISSING")
        candidates.append({"symbol": identity.symbol, "missing": missing})
    static_ready = universe_matches and all(not item["missing"] for item in candidates)
    if calendar.session != calendar.local_date:
        normal_cycle_start_status = "outside_normal_window"
    elif current_session_cycles:
        normal_cycle_start_status = "blocked_existing_cycle"
    elif scheduled_trigger is not None:
        normal_cycle_start_status = "blocked_scheduled_claim"
    elif not static_ready:
        normal_cycle_start_status = "blocked_static_inputs"
    elif not (scheduled_slot <= as_of < scheduled_slot + timedelta(minutes=5)):
        normal_cycle_start_status = "outside_normal_window"
    else:
        normal_cycle_start_status = "requires_window_evidence"
    return {
        "status": "ready_static" if static_ready else "blocked",
        "decision_session": calendar.session.isoformat(),
        "prior_session": prior_session.isoformat(),
        "current_session": calendar.local_date.isoformat(),
        "scheduled_trigger": scheduled_trigger,
        "current_session_cycles": current_session_cycles,
        "normal_cycle_start_status": normal_cycle_start_status,
        "universe_status": "matched" if universe_matches else "mismatch",
        "candidates": candidates,
        "window_evidence_status": "not_evaluated",
        "execution_locked": True,
        "external_requests": 0,
    }


async def _lock_candidate_batch_account(session: AsyncSession, *, account_id: UUID) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1818))"),
        {"scope": f"internal-paper:{account_id}"},
    )


async def _load_idempotency_claim(
    session: AsyncSession, *, account_id: UUID, idempotency_key: str
) -> IdempotencyRecord | None:
    claim: IdempotencyRecord | None = await session.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == f"internal-paper:{account_id}",
            IdempotencyRecord.route == _ROUTE,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
    )
    return claim


async def _load_claim_result(
    session: AsyncSession,
    *,
    claim: IdempotencyRecord,
    batch_hash: str,
) -> CandidateBatchResult:
    if claim.request_hash != batch_hash:
        raise ValueError("IDEMPOTENCY_CONFLICT")
    if claim.schema_version != _SCHEMA or not claim.response_payload:
        raise ValueError("CANDIDATE_BATCH_INVALID")
    try:
        replay = CandidateBatchResult.model_validate(claim.response_payload)
    except ValueError:
        raise ValueError("CANDIDATE_BATCH_INVALID") from None
    persisted = await load_agent_paper_candidate_batch(session, batch_id=replay.batch_id)
    if persisted is None or persisted != replay:
        raise ValueError("CANDIDATE_BATCH_INVALID")
    return persisted


async def preflight_agent_paper_candidate_batch(
    session: AsyncSession,
    *,
    account_id: UUID,
    universe_snapshot_id: UUID,
    instrument_ids: tuple[UUID, ...],
    idempotency_key: str,
) -> tuple[CandidateBatchResult, tuple[CandidateInstrumentIdentity, ...]] | None:
    """Read an existing exact replay before a CLI previews current identities."""
    canonical = CandidateBatchInput(instrument_ids=instrument_ids).instrument_ids
    if not 1 <= len(idempotency_key) <= 255:
        raise ValueError("IDEMPOTENCY_CONFLICT")
    batch_hash = _canonical_hash(
        account_id=account_id,
        universe_snapshot_id=universe_snapshot_id,
        instrument_ids=canonical,
    )
    await _lock_candidate_batch_account(session, account_id=account_id)
    claim = await _load_idempotency_claim(
        session, account_id=account_id, idempotency_key=idempotency_key
    )
    if claim is None:
        return None
    batch = await _load_claim_result(session, claim=claim, batch_hash=batch_hash)
    identities = await resolve_agent_paper_candidate_identities(
        session,
        universe_snapshot_id=batch.universe_snapshot_id,
        instrument_ids=batch.instrument_ids,
        created_at=batch.created_at,
    )
    return batch, identities


async def create_agent_paper_candidate_batch(
    session: AsyncSession,
    *,
    account_id: UUID,
    universe_snapshot_id: UUID,
    instrument_ids: tuple[UUID, ...],
    idempotency_key: str,
    created_at: datetime,
) -> CandidateBatchResult:
    """Create or exactly replay one explicit, bounded candidate batch."""
    created_at = _require_utc(created_at)
    canonical = CandidateBatchInput(instrument_ids=instrument_ids).instrument_ids
    if not 1 <= len(idempotency_key) <= 255:
        raise ValueError("IDEMPOTENCY_CONFLICT")
    batch_hash = _canonical_hash(
        account_id=account_id,
        universe_snapshot_id=universe_snapshot_id,
        instrument_ids=canonical,
    )
    await _lock_candidate_batch_account(session, account_id=account_id)
    existing_claim = await _load_idempotency_claim(
        session, account_id=account_id, idempotency_key=idempotency_key
    )
    if existing_claim is not None:
        return await _load_claim_result(session, claim=existing_claim, batch_hash=batch_hash)
    account = await session.scalar(
        select(PaperAccountRecord).where(PaperAccountRecord.id == account_id).with_for_update()
    )
    underlying_account = (
        None
        if account is None
        else await session.scalar(
            select(Account).where(Account.id == account.account_id).with_for_update()
        )
    )
    if (
        account is None
        or account.capability != "simulation_write_internal"
        or account.currency != "USD"
        or account.created_at > created_at
        or underlying_account is None
        or underlying_account.provider != "internal"
        or underlying_account.status != "ACTIVE"
        or underlying_account.created_at > created_at
    ):
        raise ValueError("CANDIDATE_BATCH_INVALID")
    try:
        claim, created = await claim_idempotency(
            session,
            actor_id=f"internal-paper:{account_id}",
            route=_ROUTE,
            idempotency_key=idempotency_key,
            request_hash=batch_hash,
            schema_version=_SCHEMA,
        )
    except IdempotencyClaimError as error:
        raise ValueError("IDEMPOTENCY_CONFLICT") from error
    if not created:
        return await _load_claim_result(session, claim=claim, batch_hash=batch_hash)
    await resolve_agent_paper_candidate_identities(
        session,
        universe_snapshot_id=universe_snapshot_id,
        instrument_ids=canonical,
        created_at=created_at,
    )
    record = await session.scalar(
        select(AgentPaperCandidateBatchRecord).where(
            AgentPaperCandidateBatchRecord.batch_hash == batch_hash
        )
    )
    if record is None:
        record = AgentPaperCandidateBatchRecord(
            id=new_uuid7(),
            account_id=account_id,
            universe_snapshot_id=universe_snapshot_id,
            candidate_instrument_ids=list(canonical),
            source=_SOURCE,
            batch_hash=batch_hash,
            created_at=created_at,
        )
        session.add(record)
        await session.flush()
    result = _result(record)
    if (
        result.account_id != account_id
        or result.universe_snapshot_id != universe_snapshot_id
        or result.instrument_ids != canonical
    ):
        raise ValueError("CANDIDATE_BATCH_INVALID")
    claim.response_payload = result.model_dump(mode="json")
    await session.flush()
    return result
