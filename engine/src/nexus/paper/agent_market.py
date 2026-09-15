"""Strict Agent paper-market window qualification and collection."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import String, select, text
from sqlalchemy import cast as sql_cast
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.domain.ids import new_uuid7
from nexus.market.ingestion import (
    MarketObjectStore,
    PreparedPricePage,
    persist_prepared_price_page,
    prepare_price_page,
)
from nexus.persistence.models.agents import AgentResearchOutputRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.paper import (
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    AgentPaperMarketWindowRecord,
    PaperOrderRecord,
    StrategyDecisionAgentOutputRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.providers.base import (
    BarsRequest,
    InstrumentId,
    MarketDataProvider,
    PriceBar,
    ProviderPage,
)
from nexus.providers.market.alpaca import MarketProviderPayloadError


class PaperMarketError(ValueError):
    """A market window cannot be admitted for Agent paper use."""


class AgentPaperMarketScope(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    cycle_id: UUID
    instrument_id: UUID
    execution_source_policy_id: UUID
    window_start: datetime
    window_end: datetime


class _QualifiedAgentPaperWindow(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    window_start: datetime
    window_end: datetime
    bar_count: Literal[5]


class QualifiedAlpacaAgentPaperWindow(_QualifiedAgentPaperWindow):
    source: Literal["alpaca"] = "alpaca"
    feed: Literal["iex"] = "iex"
    delivery_mode: Literal["real_time"] = "real_time"


class QualifiedSchwabAgentPaperWindow(_QualifiedAgentPaperWindow):
    source: Literal["schwab"] = "schwab"
    feed: Literal["price_history"] = "price_history"
    delivery_mode: Literal["unknown"] = "unknown"


type QualifiedAgentPaperWindow = QualifiedAlpacaAgentPaperWindow | QualifiedSchwabAgentPaperWindow


class _PaperMarketWindowResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    cycle_id: UUID
    instrument_id: UUID
    purpose: Literal["paper_fill", "acceptance_probe"]
    authority_id: UUID
    window_start: datetime
    window_end: datetime
    bar_count: Literal[5]
    replayed: bool
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class AlpacaPaperMarketWindowResult(_PaperMarketWindowResult):
    source: Literal["alpaca"] = "alpaca"
    feed: Literal["iex"] = "iex"
    delivery_mode: Literal["real_time"] = "real_time"


class SchwabPaperMarketWindowResult(_PaperMarketWindowResult):
    source: Literal["schwab"] = "schwab"
    feed: Literal["price_history"] = "price_history"
    delivery_mode: Literal["unknown"] = "unknown"


type PaperMarketWindowResult = AlpacaPaperMarketWindowResult | SchwabPaperMarketWindowResult


def _utc_now() -> datetime:
    return datetime.now(UTC)


def qualify_agent_paper_window(
    *,
    page: ProviderPage[PriceBar],
    instrument_id: UUID,
    window_start: datetime,
    evaluated_at: datetime,
    expected_source: Literal["alpaca", "schwab"] = "alpaca",
) -> QualifiedAgentPaperWindow:
    """Admit one exact five-minute Alpaca or Schwab execution window."""
    expected = tuple(window_start + timedelta(minutes=index) for index in range(5))
    bars = page.items
    provenance = page.provenance
    valid = (
        window_start.tzinfo is not None
        and window_start.utcoffset() == UTC.utcoffset(window_start)
        and evaluated_at.tzinfo is not None
        and evaluated_at.utcoffset() == UTC.utcoffset(evaluated_at)
        and evaluated_at >= window_start + timedelta(minutes=5)
        and (
            (
                expected_source == "alpaca"
                and provenance.source == "alpaca"
                and provenance.feed == "iex"
                and provenance.delivery_mode == "real_time"
            )
            or (
                expected_source == "schwab"
                and provenance.source == "schwab"
                and provenance.feed == "price_history"
                and provenance.delivery_mode == "unknown"
            )
        )
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
        raise PaperMarketError(
            "PAPER_MARKET_WINDOW_UNAVAILABLE"
            if expected_source == "schwab"
            else "PAPER_MARKET_POLICY_REJECTED"
        )
    if expected_source == "schwab":
        latest_bar_end = expected[-1] + timedelta(minutes=1)
        received_at = max(bar.received_at or bar.available_at for bar in bars)
        if received_at - latest_bar_end > timedelta(seconds=120):
            raise PaperMarketError("PAPER_MARKET_WINDOW_UNAVAILABLE")
        return QualifiedSchwabAgentPaperWindow(
            instrument_id=instrument_id,
            window_start=window_start,
            window_end=window_start + timedelta(minutes=5),
            bar_count=5,
        )
    return QualifiedAlpacaAgentPaperWindow(
        instrument_id=instrument_id,
        window_start=window_start,
        window_end=window_start + timedelta(minutes=5),
        bar_count=5,
    )


def agent_paper_window_bounds(
    output_available_at: datetime, market_close_at: datetime
) -> tuple[datetime, datetime]:
    """Derive the sole five-minute window after an Agent output becomes available."""
    if (
        output_available_at.tzinfo is None
        or output_available_at.utcoffset() != UTC.utcoffset(output_available_at)
        or market_close_at.tzinfo is None
        or market_close_at.utcoffset() != UTC.utcoffset(market_close_at)
    ):
        raise PaperMarketError("PAPER_MARKET_WINDOW_UNAVAILABLE")
    start = output_available_at.replace(second=0, microsecond=0) + timedelta(minutes=1)
    end = start + timedelta(minutes=5)
    if end > market_close_at:
        raise PaperMarketError("PAPER_MARKET_WINDOW_UNAVAILABLE")
    return start, end


async def collect_agent_paper_window(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    cycle_id: UUID,
    instrument_id: UUID | None = None,
    window_start: datetime | None = None,
    provider: MarketDataProvider,
    object_store: MarketObjectStore,
    evaluated_at: datetime | None = None,
) -> PaperMarketWindowResult:
    """Fetch, archive, qualify and bind one derived window with no retry or fallback."""
    async with session_factory() as session, session.begin():
        cycle = await session.get(AgentPaperCycleRecord, cycle_id)
        if cycle is None:
            raise PaperMarketError("PAPER_MARKET_CYCLE_REJECTED")
        if cycle.execution_price_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID:
            scope = await resolve_agent_paper_market_scope(session, cycle_id=cycle_id)
            if instrument_id not in {None, scope.instrument_id} or window_start not in {
                None,
                scope.window_start,
            }:
                raise PaperMarketError("PAPER_MARKET_CYCLE_REJECTED")
            instrument_id = scope.instrument_id
            window_start = scope.window_start
        elif instrument_id is None or window_start is None:
            raise PaperMarketError("PAPER_MARKET_CYCLE_REJECTED")
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
        if (
            cycle.execution_price_policy_id
            not in {PAPER_EXECUTION_SOURCE_POLICY_ID, PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID}
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
                StrategyDecisionRecord.agent_paper_cycle_id == cycle.id,
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
        try:
            page = await provider.fetch_bars(request, cursor=None)
        except MarketProviderPayloadError:
            if cycle.execution_price_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID:
                raise PaperMarketError("PAPER_MARKET_WINDOW_UNAVAILABLE") from None
            raise
        evaluation_time = evaluated_at or _utc_now()
        qualified = qualify_agent_paper_window(
            page=page,
            instrument_id=instrument_id,
            window_start=window_start,
            evaluated_at=evaluation_time,
            expected_source=(
                "schwab"
                if cycle.execution_price_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
                else "alpaca"
            ),
        )
        prepared = await prepare_price_page(object_store, page)
        raw = await _persist_raw_artifact(session, prepared, page)
        ingested = await persist_prepared_price_page(
            session,
            prepared,
            source_policy_id=cycle.execution_price_policy_id,
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
            source=qualified.source,
            feed=qualified.feed,
            delivery_mode=qualified.delivery_mode,
            source_content_hash=authority.content_hash,
            raw_content_hash=raw.sha256,
            content_hash=content_hash,
        )
        session.add(binding)
        await session.flush()
        return _market_result(binding, replayed=False)


async def resolve_agent_paper_market_scope(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    instrument_id: UUID | None = None,
) -> AgentPaperMarketScope:
    """Resolve one persisted candidate and its database-owned execution window."""
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if (
        cycle is None
        or cycle.execution_price_policy_id
        not in {PAPER_EXECUTION_SOURCE_POLICY_ID, PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID}
        or not cycle.candidate_manifest
        or any(not isinstance(candidate, dict) for candidate in cycle.candidate_manifest)
    ):
        raise PaperMarketError("PAPER_MARKET_CYCLE_REJECTED")
    try:
        candidate_ids = tuple(
            UUID(str(candidate.get("instrument_id")))
            for candidate in cycle.candidate_manifest
        )
    except (TypeError, ValueError):
        raise PaperMarketError("PAPER_MARKET_CYCLE_REJECTED") from None
    if cycle.execution_price_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID:
        if instrument_id is not None or len(candidate_ids) != 1:
            raise PaperMarketError("PAPER_MARKET_CYCLE_REJECTED")
        instrument_id = candidate_ids[0]
    elif instrument_id is None:
        if len(candidate_ids) != 1:
            raise PaperMarketError("PAPER_MARKET_CYCLE_REJECTED")
        instrument_id = candidate_ids[0]
    elif instrument_id not in candidate_ids:
        raise PaperMarketError("PAPER_MARKET_INSTRUMENT_REJECTED")
    if cycle.execution_price_policy_id == PAPER_EXECUTION_SOURCE_POLICY_ID:
        calendars = tuple(
            await session.scalars(
                select(ExchangeCalendarSessionRecord)
                .join(
                    StrategyDecisionRecord,
                    (
                        ExchangeCalendarSessionRecord.calendar_authority_id
                        == StrategyDecisionRecord.calendar_authority_id
                    )
                    & (
                        ExchangeCalendarSessionRecord.session
                        == cycle.execution_session
                    ),
                )
                .where(
                    StrategyDecisionRecord.agent_paper_cycle_id == cycle.id,
                    StrategyDecisionRecord.decision_origin == "agent_research",
                    StrategyDecisionRecord.execution_source_policy_id
                    == cycle.execution_price_policy_id,
                )
                .limit(2)
            )
        )
        if len(calendars) != 1 or calendars[0].market_open_at is None:
            raise PaperMarketError("PAPER_MARKET_WINDOW_UNAVAILABLE")
        start = calendars[0].market_open_at
        return AgentPaperMarketScope(
            cycle_id=cycle.id,
            instrument_id=instrument_id,
            execution_source_policy_id=cycle.execution_price_policy_id,
            window_start=start,
            window_end=start + timedelta(minutes=5),
        )
    row = (
        await session.execute(
            select(AgentResearchOutputRecord, Job, ExchangeCalendarSessionRecord)
            .join(
                StrategyDecisionAgentOutputRecord,
                StrategyDecisionAgentOutputRecord.agent_research_output_id
                == AgentResearchOutputRecord.id,
            )
            .join(
                StrategyDecisionRecord,
                StrategyDecisionRecord.id == StrategyDecisionAgentOutputRecord.strategy_decision_id,
            )
            .join(
                Job,
                Job.payload["run_id"].as_string()
                == sql_cast(AgentResearchOutputRecord.run_id, String),
            )
            .join(
                ExchangeCalendarSessionRecord,
                (
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == StrategyDecisionRecord.calendar_authority_id
                )
                & (ExchangeCalendarSessionRecord.session == cycle.execution_session),
            )
            .where(
                StrategyDecisionRecord.agent_paper_cycle_id == cycle.id,
                StrategyDecisionRecord.decision_origin == "agent_research",
                StrategyDecisionRecord.execution_source_policy_id
                == cycle.execution_price_policy_id,
                AgentResearchOutputRecord.status == "completed",
                AgentResearchOutputRecord.role.is_(None),
                Job.job_type == "agent.research",
                Job.status == "succeeded",
                Job.finished_at.is_not(None),
            )
            .limit(2)
        )
    ).all()
    if len(row) != 1:
        raise PaperMarketError("PAPER_MARKET_WINDOW_UNAVAILABLE")
    output, job, calendar = row[0]
    if job.finished_at is None:
        raise PaperMarketError("PAPER_MARKET_WINDOW_UNAVAILABLE")
    output_available_at = max(output.created_at, job.finished_at)
    start, end = agent_paper_window_bounds(output_available_at, calendar.market_close_at)
    return AgentPaperMarketScope(
        cycle_id=cycle.id,
        instrument_id=instrument_id,
        execution_source_policy_id=cycle.execution_price_policy_id,
        window_start=start,
        window_end=end,
    )


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
        "schema_version": (
            "schwab_price_history_raw_v1"
            if page.provenance.source == "schwab"
            else "alpaca_iex_bars_raw_v1"
        ),
        "source": page.provenance.source,
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
    result_type = (
        SchwabPaperMarketWindowResult if row.source == "schwab" else AlpacaPaperMarketWindowResult
    )
    return result_type(
        cycle_id=row.cycle_id,
        instrument_id=row.instrument_id,
        purpose=cast(Literal["paper_fill", "acceptance_probe"], row.purpose),
        authority_id=row.id,
        window_start=row.window_start_at,
        window_end=row.window_end_at,
        bar_count=5,
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
