"""One durable attempt for all frozen economic orders, with I/O outside transactions."""

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.market.ingestion import (
    MarketObjectStore,
    PreparedPricePage,
    persist_prepared_price_page,
    prepare_price_page,
)
from nexus.paper.agent_market import (
    AgentPaperMarketScope,
    QualifiedAgentPaperWindow,
    _binding_hash,
    _persist_raw_artifact,
    qualify_agent_paper_window,
)
from nexus.paper.economic_cycle import read_economic_cycle
from nexus.paper.economic_isolation import require_running_isolation
from nexus.paper.economic_window import _read_window
from nexus.persistence.models.economic import EconomicCommonWindowRecord, EconomicWindowOrderRecord
from nexus.persistence.models.paper import (
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    AgentPaperMarketWindowRecord,
    PaperOrderRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import AuditEvent
from nexus.providers.base import BarsRequest, InstrumentId, MarketDataProvider

_CLAIM = "economic.window.collection_claimed"


@dataclass(frozen=True)
class EconomicCollectionResult:
    status: str
    window_id: UUID
    replayed: bool


async def _scope(
    session: AsyncSession, pair_slot_id: UUID, window_seq: int, *, lock: bool = True
) -> tuple[
    EconomicCommonWindowRecord, dict[UUID, AgentPaperMarketScope], dict[UUID, AgentPaperMarketScope]
]:
    if window_seq < 1:
        raise ValueError("ECONOMIC_WINDOW_SEQUENCE_UNSUPPORTED")
    query = select(EconomicCommonWindowRecord).where(
        EconomicCommonWindowRecord.pair_slot_id == pair_slot_id,
        EconomicCommonWindowRecord.window_seq == window_seq,
    )
    if lock:
        query = query.with_for_update()
    window = await session.scalar(query)
    if window is None:
        raise ValueError("ECONOMIC_WINDOW_REQUIRED")
    await _read_window(session, window)
    if window.status != "frozen":
        raise ValueError("ECONOMIC_WINDOW_MISSED")
    links = tuple(
        await session.scalars(
            select(EconomicWindowOrderRecord)
            .where(EconomicWindowOrderRecord.window_id == window.id)
            .order_by(EconomicWindowOrderRecord.order_id)
        )
    )
    scopes: dict[UUID, AgentPaperMarketScope] = {}
    order_scopes: dict[UUID, AgentPaperMarketScope] = {}
    for link in links:
        order = await session.get(PaperOrderRecord, link.order_id)
        if order is None:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        decision = await session.get(StrategyDecisionRecord, order.strategy_decision_id)
        if decision is None or decision.agent_paper_cycle_id is None:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        cycle = await session.get(AgentPaperCycleRecord, decision.agent_paper_cycle_id)
        if cycle is None:
            raise ValueError("ECONOMIC_WINDOW_INVALID")
        scope = AgentPaperMarketScope(
            cycle_id=cycle.id,
            instrument_id=link.instrument_id,
            execution_source_policy_id=cycle.execution_price_policy_id,
            window_start=window.window_start,
            window_end=window.window_end,
        )
        previous = scopes.get(scope.instrument_id)
        if (
            previous is not None
            and previous.execution_source_policy_id != scope.execution_source_policy_id
        ):
            raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
        scopes[scope.instrument_id] = scope
        order_scopes[link.order_id] = scope
    if len({s.execution_source_policy_id for s in scopes.values()}) != 1:
        raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
    return window, scopes, order_scopes


async def _existing(
    session: AsyncSession,
    window: EconomicCommonWindowRecord,
    order_scopes: dict[UUID, AgentPaperMarketScope],
) -> bool:
    rows = tuple(
        await session.scalars(
            select(AgentPaperMarketWindowRecord).where(
                AgentPaperMarketWindowRecord.order_id.in_(order_scopes),
                AgentPaperMarketWindowRecord.window_start_at == window.window_start,
            )
        )
    )
    if not rows:
        return False
    if {r.order_id for r in rows} != set(order_scopes) or len(rows) != len(order_scopes):
        raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
    shared: dict[UUID, tuple[UUID, UUID, UUID, str, str]] = {}
    for row in rows:
        if row.order_id is None:
            raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
        scope = order_scopes[row.order_id]
        wanted_hash = _binding_hash(
            cycle_id=scope.cycle_id,
            instrument_id=scope.instrument_id,
            order_id=row.order_id,
            purpose="paper_fill",
            source_authority_id=row.source_authority_id,
            raw_artifact_id=row.raw_artifact_id,
            manifest_id=row.price_series_manifest_id,
            window_start=window.window_start,
            window_end=window.window_end,
            evaluated_at=row.evaluated_at,
            source_content_hash=row.source_content_hash,
            raw_content_hash=row.raw_content_hash,
        )
        if (
            row.cycle_id != scope.cycle_id
            or row.instrument_id != scope.instrument_id
            or row.purpose != "paper_fill"
            or row.window_end_at != window.window_end
            or row.content_hash != wanted_hash
            or row.data_available_at > row.evaluated_at
        ):
            raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
        value = (
            row.raw_artifact_id,
            row.price_series_manifest_id,
            row.source_authority_id,
            row.raw_content_hash,
            row.source_content_hash,
        )
        if row.instrument_id in shared and shared[row.instrument_id] != value:
            raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
        if any(
            iid != row.instrument_id and any(previous[i] == value[i] for i in range(3))
            for iid, previous in shared.items()
        ):
            raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
        shared[row.instrument_id] = value
    return True


async def _require_new_collection(
    session: AsyncSession, scopes: dict[UUID, AgentPaperMarketScope]
) -> None:
    if not scopes:
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    owner = await read_economic_cycle(session, cycle_id=next(iter(scopes.values())).cycle_id)
    if owner is None:
        raise ValueError("ECONOMIC_WINDOW_INVALID")
    await require_running_isolation(session, authority=owner[1])


async def collect_economic_window(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    pair_slot_id: UUID,
    window_seq: int,
    provider: MarketDataProvider,
    object_store: MarketObjectStore,
) -> EconomicCollectionResult:
    """Claim once, prepare every instrument, then atomically bind all trading orders.

    An incomplete committed claim is unknown, never a retry permit. This operation
    does not consume orders, release reservations, or write fills/ledger entries.
    """
    async with session_factory.begin() as session:
        window, scopes, order_scopes = await _scope(session, pair_slot_id, window_seq)
        if await _existing(session, window, order_scopes):
            return EconomicCollectionResult("archives_ready", window.id, True)
        claim = await session.scalar(
            select(AuditEvent.id).where(
                AuditEvent.action == _CLAIM,
                AuditEvent.payload["window_id"].astext == str(window.id),
            )
        )
        if claim is not None:
            raise ValueError("ECONOMIC_WINDOW_COLLECTION_UNKNOWN")
        await _require_new_collection(session, scopes)
        now = await session.scalar(select(func.clock_timestamp()))
        if not isinstance(now, datetime) or now < window.window_end:
            raise ValueError("ECONOMIC_WINDOW_COLLECTION_TOO_EARLY")
        window_id, frozen_hash = window.id, window.content_hash
        await append_audit_event(
            session,
            actor_id="economic-window",
            action=_CLAIM,
            schema_version="economic_window_collection_v1",
            occurred_at=now,
            payload={
                "window_id": str(window_id),
                "window_hash": frozen_hash,
                "instruments": sorted(str(i) for i in scopes),
            },
        )

    # The committed claim and every resolver/authority transaction have ended.
    prepared_pages: dict[UUID, tuple[PreparedPricePage, QualifiedAgentPaperWindow, datetime]] = {}
    try:
        for instrument_id in sorted(scopes, key=str):
            scope = scopes[instrument_id]
            page = await provider.fetch_bars(
                BarsRequest(
                    instrument_id=InstrumentId(instrument_id),
                    start=scope.window_start,
                    end=scope.window_end,
                    interval="1Min",
                    adjustment="raw",
                ),
                cursor=None,
            )
            async with session_factory.begin() as clock_session:
                evaluated_at = await clock_session.scalar(select(func.clock_timestamp()))
            if not isinstance(evaluated_at, datetime):
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            source: Literal["alpaca", "schwab"] = (
                "schwab"
                if scope.execution_source_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
                else "alpaca"
            )
            qualified = qualify_agent_paper_window(
                page=page,
                instrument_id=instrument_id,
                window_start=scope.window_start,
                evaluated_at=evaluated_at,
                expected_source=source,
            )
            if any(
                previous.raw_digest == hashlib.sha256(page.raw_payload).hexdigest()
                for previous, _, _ in prepared_pages.values()
            ):
                raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
            prepared = await prepare_price_page(object_store, page)
            # Exact raw bytes must be readable before either side gets an authority.
            raw_key = (
                "raw/market/"
                + max(b.received_at or b.available_at for b in page.items).strftime("%Y/%m/%d")
                + "/"
                + (page.source_request_id or prepared.raw_digest[:16])
                + "/"
                + prepared.raw_digest
                + ".json"
            )
            raw = await object_store.read_raw(raw_key, uri=prepared.raw_uri)
            if raw != page.raw_payload or hashlib.sha256(raw).hexdigest() != prepared.raw_digest:
                raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
            prepared_pages[instrument_id] = (prepared, qualified, evaluated_at)

        async with session_factory.begin() as session:
            current, current_scopes, current_orders = await _scope(
                session, pair_slot_id, window_seq
            )
            if (
                current.id != window_id
                or current.content_hash != frozen_hash
                or current_orders != order_scopes
            ):
                raise ValueError("ECONOMIC_WINDOW_INVALID")
            if await _existing(session, current, current_orders):
                raise ValueError("ECONOMIC_WINDOW_ARCHIVE_CONFLICT")
            await _require_new_collection(session, current_scopes)
            for instrument_id, (prepared, qualified, evaluated_at) in prepared_pages.items():
                raw_artifact = await _persist_raw_artifact(session, prepared, prepared.page)
                ingested = await persist_prepared_price_page(
                    session,
                    prepared,
                    source_policy_id=current_scopes[instrument_id].execution_source_policy_id,
                )
                authority = ingested.price_authority
                if authority is None:
                    raise ValueError("PAPER_MARKET_AUTHORITY_UNAVAILABLE")
                for order_id, scope in current_orders.items():
                    if scope.instrument_id != instrument_id:
                        continue
                    values = dict(
                        cycle_id=scope.cycle_id,
                        instrument_id=instrument_id,
                        order_id=order_id,
                        purpose="paper_fill",
                        source_authority_id=authority.id,
                        raw_artifact_id=raw_artifact.id,
                        manifest_id=ingested.manifest.id,
                        window_start=qualified.window_start,
                        window_end=qualified.window_end,
                        evaluated_at=evaluated_at,
                        source_content_hash=authority.content_hash,
                        raw_content_hash=raw_artifact.sha256,
                    )
                    session.add(
                        AgentPaperMarketWindowRecord(
                            id=new_uuid7(),
                            cycle_id=scope.cycle_id,
                            instrument_id=instrument_id,
                            order_id=order_id,
                            purpose="paper_fill",
                            source_authority_id=authority.id,
                            raw_artifact_id=raw_artifact.id,
                            price_series_manifest_id=ingested.manifest.id,
                            window_start_at=qualified.window_start,
                            window_end_at=qualified.window_end,
                            evaluated_at=evaluated_at,
                            data_available_at=authority.available_at,
                            source=qualified.source,
                            feed=qualified.feed,
                            delivery_mode=qualified.delivery_mode,
                            source_content_hash=authority.content_hash,
                            raw_content_hash=raw_artifact.sha256,
                            content_hash=_binding_hash(**values),
                        )
                    )
            await session.flush()
            if not await _existing(session, current, current_orders):
                raise ValueError("ECONOMIC_WINDOW_ARCHIVES_REQUIRED")
    except Exception:
        # Retain the known attempt even if transport/archive/commit outcome is unknown.
        # No provider detail (including credentials/raw response) crosses this boundary.
        raise ValueError("ECONOMIC_WINDOW_COLLECTION_FAILED") from None
    return EconomicCollectionResult("archives_ready", window_id, False)
