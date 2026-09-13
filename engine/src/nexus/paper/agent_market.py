"""Strict Agent paper-market window qualification and collection."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.domain.ids import new_uuid7
from nexus.market.ingestion import (
    MarketObjectStore,
    PreparedPricePage,
    persist_prepared_price_page,
    prepare_price_page,
)
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.paper import (
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    AgentPaperMarketWindowRecord,
    PaperOrderRecord,
    StrategyDecisionRecord,
)
from nexus.providers.base import (
    BarsRequest,
    InstrumentId,
    MarketDataProvider,
    PriceBar,
    ProviderPage,
)


class PaperMarketError(ValueError):
    """A market window cannot be admitted for Agent paper use."""


class QualifiedAgentPaperWindow(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    window_start: datetime
    window_end: datetime
    bar_count: Literal[5]
    source: Literal["alpaca"]
    feed: Literal["iex"]
    delivery_mode: Literal["real_time"]


class PaperMarketWindowResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    cycle_id: UUID
    instrument_id: UUID
    purpose: Literal["paper_fill", "acceptance_probe"]
    authority_id: UUID
    window_start: datetime
    window_end: datetime
    bar_count: Literal[5]
    source: Literal["alpaca"]
    feed: Literal["iex"]
    delivery_mode: Literal["real_time"]
    replayed: bool
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


def qualify_agent_paper_window(
    *,
    page: ProviderPage[PriceBar],
    instrument_id: UUID,
    window_start: datetime,
    evaluated_at: datetime,
) -> QualifiedAgentPaperWindow:
    """Admit exactly five consecutive raw IEX minutes and nothing broader."""
    expected = tuple(window_start + timedelta(minutes=index) for index in range(5))
    bars = page.items
    provenance = page.provenance
    valid = (
        window_start.tzinfo is not None
        and window_start.utcoffset() == UTC.utcoffset(window_start)
        and evaluated_at.tzinfo is not None
        and evaluated_at.utcoffset() == UTC.utcoffset(evaluated_at)
        and evaluated_at >= window_start + timedelta(minutes=5)
        and provenance.source == "alpaca"
        and provenance.feed == "iex"
        and provenance.delivery_mode == "real_time"
        and provenance.mode == "read_only"
        and provenance.interval == "1Min"
        and provenance.adjustment == "raw"
        and page.next_cursor is None
        and bool(page.raw_payload)
        and len(bars) == 5
        and tuple(bar.observed_at for bar in bars) == expected
        and all(
            bar.instrument_id == instrument_id
            and bar.observed_at is not None
            and bar.observed_at <= evaluated_at
            and bar.received_at is not None
            and bar.received_at <= evaluated_at
            and bar.available_at <= evaluated_at
            and min(bar.open, bar.high, bar.low, bar.close) > 0
            and bar.volume >= 0
            for bar in bars
        )
    )
    if not valid:
        raise PaperMarketError("PAPER_MARKET_POLICY_REJECTED")
    return QualifiedAgentPaperWindow(
        instrument_id=instrument_id,
        window_start=window_start,
        window_end=window_start + timedelta(minutes=5),
        bar_count=5,
        source="alpaca",
        feed="iex",
        delivery_mode="real_time",
    )


async def collect_agent_paper_window(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    cycle_id: UUID,
    instrument_id: UUID,
    window_start: datetime,
    provider: MarketDataProvider,
    object_store: MarketObjectStore,
    evaluated_at: datetime | None = None,
) -> PaperMarketWindowResult:
    """Fetch, archive, qualify and bind one IEX window with no retry or fallback."""
    evaluation_time = evaluated_at or datetime.now(UTC)
    async with session_factory() as session, session.begin():
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
            {
                "scope": (
                    f"nexus:agent-paper-market:{cycle_id}:{instrument_id}:"
                    f"{window_start.isoformat()}"
                )
            },
        )
        existing = await session.scalar(
            select(AgentPaperMarketWindowRecord).where(
                AgentPaperMarketWindowRecord.cycle_id == cycle_id,
                AgentPaperMarketWindowRecord.instrument_id == instrument_id,
                AgentPaperMarketWindowRecord.window_start_at == window_start,
            )
        )
        if existing is not None:
            return _market_result(existing, replayed=True)
        cycle = await session.get(AgentPaperCycleRecord, cycle_id)
        if (
            cycle is None
            or cycle.execution_price_policy_id != PAPER_EXECUTION_SOURCE_POLICY_ID
            or not cycle.execution_locked
        ):
            raise PaperMarketError("PAPER_MARKET_CYCLE_REJECTED")
        candidates = tuple(cycle.candidate_manifest)
        if not candidates or any(not isinstance(row, dict) for row in candidates):
            raise PaperMarketError("PAPER_MARKET_CYCLE_REJECTED")
        candidate_ids = tuple(UUID(str(row.get("instrument_id"))) for row in candidates)
        if instrument_id not in candidate_ids:
            raise PaperMarketError("PAPER_MARKET_INSTRUMENT_REJECTED")
        order_id = await session.scalar(
            select(PaperOrderRecord.id)
            .join(
                StrategyDecisionRecord,
                StrategyDecisionRecord.id == PaperOrderRecord.strategy_decision_id,
            )
            .where(
                PaperOrderRecord.account_id == cycle.account_id,
                PaperOrderRecord.instrument_id == instrument_id,
                StrategyDecisionRecord.account_id == cycle.account_id,
                StrategyDecisionRecord.decision_session == cycle.decision_session,
                StrategyDecisionRecord.decision_origin == "agent_research",
            )
            .order_by(PaperOrderRecord.id)
            .limit(1)
        )
        purpose: Literal["paper_fill", "acceptance_probe"]
        if order_id is not None:
            purpose = "paper_fill"
        else:
            prior_probe = await session.scalar(
                select(AgentPaperMarketWindowRecord.id).where(
                    AgentPaperMarketWindowRecord.cycle_id == cycle_id,
                    AgentPaperMarketWindowRecord.purpose == "acceptance_probe",
                )
            )
            if instrument_id != candidate_ids[0] or prior_probe is not None:
                raise PaperMarketError("PAPER_MARKET_PROBE_REJECTED")
            purpose = "acceptance_probe"

        request = BarsRequest(
            instrument_id=InstrumentId(instrument_id),
            start=window_start,
            end=window_start + timedelta(minutes=5),
            interval="1Min",
            adjustment="raw",
        )
        page = await provider.fetch_bars(request, cursor=None)
        qualified = qualify_agent_paper_window(
            page=page,
            instrument_id=instrument_id,
            window_start=window_start,
            evaluated_at=evaluation_time,
        )
        prepared = await prepare_price_page(object_store, page)
        raw = await _persist_raw_artifact(session, prepared, page)
        ingested = await persist_prepared_price_page(
            session,
            prepared,
            source_policy_id=PAPER_EXECUTION_SOURCE_POLICY_ID,
        )
        authority = ingested.price_authority
        if authority is None:
            raise PaperMarketError("PAPER_MARKET_AUTHORITY_UNAVAILABLE")
        content_hash = _binding_hash(
            cycle_id=cycle_id,
            instrument_id=instrument_id,
            order_id=order_id,
            purpose=purpose,
            source_authority_id=authority.id,
            raw_artifact_id=raw.id,
            manifest_id=ingested.manifest.id,
            window_start=qualified.window_start,
            window_end=qualified.window_end,
            evaluated_at=evaluation_time,
            source_content_hash=authority.content_hash,
            raw_content_hash=raw.sha256,
        )
        binding = AgentPaperMarketWindowRecord(
            id=new_uuid7(),
            cycle_id=cycle_id,
            instrument_id=instrument_id,
            order_id=order_id,
            purpose=purpose,
            source_authority_id=authority.id,
            raw_artifact_id=raw.id,
            price_series_manifest_id=ingested.manifest.id,
            window_start_at=qualified.window_start,
            window_end_at=qualified.window_end,
            evaluated_at=evaluation_time,
            data_available_at=authority.available_at,
            source="alpaca",
            feed="iex",
            delivery_mode="real_time",
            source_content_hash=authority.content_hash,
            raw_content_hash=raw.sha256,
            content_hash=content_hash,
        )
        session.add(binding)
        await session.flush()
        return _market_result(binding, replayed=False)


async def _persist_raw_artifact(
    session: AsyncSession, prepared: PreparedPricePage, page: ProviderPage[PriceBar]
) -> DataArtifact:
    raw_digest = prepared.raw_digest
    existing = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == raw_digest))
    observed_at = min(bar.observed_at or bar.available_at for bar in page.items)
    received_at = max(bar.received_at or bar.available_at for bar in page.items)
    values = {
        "uri": prepared.raw_uri,
        "sha256": raw_digest,
        "byte_size": len(page.raw_payload),
        "mime_type": "application/json",
        "schema_version": "alpaca_iex_bars_raw_v1",
        "source": "alpaca",
        "observed_at": observed_at,
        "available_at": max(bar.available_at for bar in page.items),
        "received_at": received_at,
    }
    if existing is not None:
        if any(getattr(existing, key) != value for key, value in values.items()):
            raise PaperMarketError("PAPER_MARKET_RAW_REPLAY_CONFLICT")
        return existing
    artifact = DataArtifact(id=new_uuid7(), **values)
    session.add(artifact)
    await session.flush()
    return artifact


def _binding_hash(**values: object) -> str:
    payload = {
        key: (
            value.isoformat()
            if isinstance(value, datetime)
            else str(value)
            if isinstance(value, UUID)
            else value
        )
        for key, value in values.items()
    }
    return hashlib.sha256(
        json.dumps(
            {"schema": "agent_paper_market_window_v1", **payload},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _market_result(row: AgentPaperMarketWindowRecord, *, replayed: bool) -> PaperMarketWindowResult:
    return PaperMarketWindowResult(
        cycle_id=row.cycle_id,
        instrument_id=row.instrument_id,
        purpose=cast(Literal["paper_fill", "acceptance_probe"], row.purpose),
        authority_id=row.id,
        window_start=row.window_start_at,
        window_end=row.window_end_at,
        bar_count=5,
        source="alpaca",
        feed="iex",
        delivery_mode="real_time",
        replayed=replayed,
        content_hash=row.content_hash,
    )


__all__ = [
    "PaperMarketError",
    "PaperMarketWindowResult",
    "QualifiedAgentPaperWindow",
    "collect_agent_paper_window",
    "qualify_agent_paper_window",
]
