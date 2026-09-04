"""Prepare an exact backtest bundle from already complete persisted source collections."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.backtest.authority_loader import (
    _load_bundle_dataset,
    _load_calendar,
    _load_collection,
    _load_universe_snapshots,
    load_frozen_point_in_time_dataset,
)
from nexus.backtest.cost_policy import load_backtest_cost_policy_v1
from nexus.backtest.costs import CostModel
from nexus.backtest.engine import (
    BacktestProtocol,
    _audited_data_hash,
    _authority_bundle_hash,
    _hash,
)
from nexus.backtest.persistence import (
    BacktestPersistenceAuthority,
    _source_selection_reports,
    input_data_snapshot_payload,
    select_source_authority,
    validate_bundle_inputs,
)
from nexus.domain.ids import new_uuid7
from nexus.persistence.idempotency import claim_idempotency
from nexus.persistence.models.backtest import (
    BacktestAuthorityBundleRecord,
    BacktestAuthorityCollectionRecord,
    BacktestInstrumentAuthorityMemberRecord,
    BacktestPriceAuthorityMemberRecord,
    ExchangeCalendarAuthorityRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import InstrumentAlias
from nexus.quality.source_policy import load_source_policy_v1


class BundleRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    strategy_version: Literal["baseline_v1"]
    calendar_authority_id: UUID
    universe_collection_id: UUID
    sector_collection_id: UUID
    corporate_action_collection_id: UUID
    spy_instrument_alias_id: UUID
    equal_weight_universe_snapshot_id: UUID
    source_policy_id: UUID
    cost_policy_id: UUID
    start: date
    end: date
    initial_nav: Decimal
    idempotency_key: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_.:-]+$")

    @field_validator("initial_nav", mode="before")
    @classmethod
    def _decimal(cls, value: object) -> object:
        if isinstance(value, float | bool):
            raise ValueError("initial_nav requires a decimal string")
        return value

    @model_validator(mode="after")
    def _valid(self) -> "BundleRequest":
        if self.start > self.end or not self.initial_nav.is_finite() or self.initial_nav <= 0:
            raise ValueError("invalid backtest range or initial_nav")
        return self


@dataclass(frozen=True)
class PreparedBundle:
    authority_bundle_id: UUID
    data_snapshot_id: UUID
    completed_at: datetime
    replayed: bool


async def prepare_bundle(session: AsyncSession, request: BundleRequest) -> PreparedBundle:
    """Atomically prepare or verify an exact replay; never create source completeness."""
    async with session.begin_nested():
        claim, owner = await claim_idempotency(
            session,
            actor_id="local-backtest",
            route="backtest.prepare",
            idempotency_key=request.idempotency_key,
            request_hash=_hash(request.model_dump(mode="json")),
            schema_version="backtest_bundle_v1",
        )
        # Same ordering as the existing result writer and publication triggers.
        await session.execute(
            text(
                "SELECT pg_advisory_xact_lock(hashtextextended('backtest-authority-global', 1602))"
            )
        )
        await session.execute(
            select(ExchangeCalendarAuthorityRecord.id)
            .where(ExchangeCalendarAuthorityRecord.id == request.calendar_authority_id)
            .with_for_update()
        )
        await session.execute(
            select(BacktestAuthorityCollectionRecord.id)
            .where(
                BacktestAuthorityCollectionRecord.id.in_(
                    (
                        request.universe_collection_id,
                        request.sector_collection_id,
                        request.corporate_action_collection_id,
                    )
                )
            )
            .order_by(BacktestAuthorityCollectionRecord.id)
            .with_for_update()
        )
        now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        cost = await load_backtest_cost_policy_v1(session)
        policy = await load_source_policy_v1(session, instrument_tick_size=None)
        if request.cost_policy_id != cost.id or request.source_policy_id != policy.record_id:
            raise ValueError("BACKTEST_PREPARE_POLICY_MISMATCH")
        protocol = BacktestProtocol(
            start=request.start,
            end=request.end,
            initial_nav=request.initial_nav,
            cost_model=CostModel(
                **{
                    name: Decimal(str(cost.parameters[name]))
                    for name in ("commission_bps", "spread_bps", "slippage_bps")
                }
            ),
        )
        if not owner:
            bundle = await session.get(
                BacktestAuthorityBundleRecord,
                UUID(str(claim.response_payload["authority_bundle_id"])),
                with_for_update=True,
                populate_existing=True,
            )
            if bundle is None:
                raise ValueError("BACKTEST_PREPARE_REPLAY_INVALID")
        else:
            bundle = BacktestAuthorityBundleRecord(
                id=new_uuid7(),
                data_snapshot_id=new_uuid7(),
                source_policy_id=policy.record_id,
                cost_policy_id=cost.id,
                calendar_authority_id=request.calendar_authority_id,
                universe_collection_id=request.universe_collection_id,
                sector_collection_id=request.sector_collection_id,
                corporate_action_collection_id=request.corporate_action_collection_id,
                spy_instrument_alias_id=request.spy_instrument_alias_id,
                range_start=request.start,
                range_end=request.end,
                completeness_status="complete",
                completed_at=now,
                created_at=now,
            )
            calendar = await _load_calendar(session, bundle.calendar_authority_id, now)
            collection = await _load_collection(session, bundle, "universe", now)
            universes = await _load_universe_snapshots(session, collection, now)
            if request.equal_weight_universe_snapshot_id not in {u.authority_id for u in universes}:
                raise ValueError("BACKTEST_PREPARE_BENCHMARK_MISMATCH")
            spy = await session.get(InstrumentAlias, request.spy_instrument_alias_id)
            if spy is None:
                raise ValueError("BACKTEST_PREPARE_BENCHMARK_MISSING")
            instruments = tuple(
                sorted(
                    {member for universe in universes for member in universe.members}
                    | {spy.instrument_id},
                    key=lambda value: value.int,
                )
            )
            adjustments: tuple[Literal["raw", "all"], ...] = ("raw", "all")
            reports = [
                await select_source_authority(
                    session,
                    instrument_id=instrument,
                    adjustment=adjustment,
                    calendar=calendar,
                    source_policy_id=policy.record_id,
                    completed_at=now,
                    authority_frozen_at=now,
                )
                for instrument in instruments
                for adjustment in adjustments
            ]
            bundle.source_selection_reports = [report.model_dump(mode="json") for report in reports]
            price_ids = {
                value
                for report in reports
                for value in report.candidate_price_bar_set_authority_ids
            }
            data = await _load_bundle_dataset(
                session,
                bundle,
                now,
                instrument_ids=instruments,
                price_member_ids=price_ids,
                benchmark_universe_id=request.equal_weight_universe_snapshot_id,
            )
            authority_hash = _authority_bundle_hash(data)
            payload = input_data_snapshot_payload(
                data, authority_hash=authority_hash, data_hash=_audited_data_hash(data, protocol)
            )
            bundle.content_hash = authority_hash
            bundle.source_selection_reports = _source_selection_reports(data)
            snapshot = DataSnapshot(
                id=bundle.data_snapshot_id,
                source="nexus_backtest_bundle",
                provider_object_id=str(bundle.id),
                revision=_hash(payload),
                observed_at=now,
                available_at=now,
                received_at=now,
                created_at=now,
                schema_version="backtest_authority_v1",
                payload=payload,
            )
            session.add(snapshot)
            await session.flush()
            session.add(bundle)
            await session.flush()
            session.add_all(
                BacktestPriceAuthorityMemberRecord(
                    bundle_id=bundle.id, price_bar_set_authority_id=value, created_at=now
                )
                for value in sorted(price_ids, key=lambda value: value.int)
            )
            session.add_all(
                BacktestInstrumentAuthorityMemberRecord(
                    bundle_id=bundle.id, instrument_id=value, created_at=now
                )
                for value in instruments
            )
            await session.flush()
        data = await load_frozen_point_in_time_dataset(session, bundle.id, protocol, now)
        await validate_bundle_inputs(
            session,
            data,
            protocol,
            BacktestPersistenceAuthority(
                authority_bundle_id=bundle.id,
                data_snapshot_id=bundle.data_snapshot_id,
                source_policy_id=bundle.source_policy_id,
                cost_policy_id=bundle.cost_policy_id,
                completed_at=bundle.completed_at,
            ),
            bundle.completed_at,
        )
        if owner:
            claim.response_payload = {"authority_bundle_id": str(bundle.id)}
            await session.flush()
        return PreparedBundle(bundle.id, bundle.data_snapshot_id, bundle.completed_at, not owner)
