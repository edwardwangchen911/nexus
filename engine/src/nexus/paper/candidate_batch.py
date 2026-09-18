"""Create and read bounded explicit research scopes for Agent paper cycles."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.persistence.idempotency import IdempotencyClaimError, claim_idempotency
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import AgentPaperCandidateBatchRecord, PaperAccountRecord
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
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


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
            UniverseSnapshot.key == "us_liquid",
            UniverseSnapshot.version == "us_liquid_v1",
            UniverseSnapshot.source == "fosun_local_read_only",
            UniverseSnapshot.effective_from <= created_at.date(),
            UniverseSnapshot.published_at.is_not(None),
            UniverseSnapshot.published_at <= created_at,
            UniverseSnapshot.created_at <= created_at,
        )
    )
    if universe is None:
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
        return await _load_claim_result(
            session, claim=existing_claim, batch_hash=batch_hash
        )
    account = await session.scalar(
        select(PaperAccountRecord)
        .where(PaperAccountRecord.id == account_id)
        .with_for_update()
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
