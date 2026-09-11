"""PostgreSQL authority for frozen scenario spread evidence."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.market.intraday_ingestion import canonical_quote_observation
from nexus.ops.alpaca_market_stream import ARCHIVE_SCHEMA, SCHEMA
from nexus.optimizer.compare import TypedMetric
from nexus.optimizer.scenario_contracts import (
    PortfolioScenarioEvaluationV2,
    ScenarioCostBreakdownV1,
    ScenarioMetricSetV1,
    ScenarioPositionV1,
    canonical_scenario_hash,
)
from nexus.optimizer.scenario_spread_cost import (
    ScenarioSpreadEvidenceReplayInvalid,
    apply_scenario_spread_cost,
    finalize_scenario_spread_manifest,
    replay_scenario_spread_evidence,
    select_scenario_spread_evidence,
)
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.market import Instrument, InstrumentAlias, LatestQuote
from nexus.persistence.session import session_factory
from nexus.providers.broker.base import BrokerPosition, CashBalance, PortfolioSnapshot
from nexus.settings import EngineSettings
from sqlalchemy import text

_VALUATION_TIME = datetime(2026, 9, 10, 15, 0, tzinfo=UTC)
_SNAPSHOT_ID = UUID("019d1000-0000-7000-8000-000000000001")
_PORTFOLIO_ID = UUID("019d1000-0000-7000-8000-000000000002")
_REPORT_ID = UUID("019d1000-0000-7000-8000-000000000003")
_FIRST_ID = UUID("019d1000-0000-7000-8000-000000000004")
_SECOND_ID = UUID("019d1000-0000-7000-8000-000000000005")


@dataclass
class Store:
    objects: dict[str, bytes] = field(default_factory=dict)

    async def read_raw(self, key: str, *, uri: str) -> bytes:
        if uri != f"memory://{key}":
            raise ValueError("object URI mismatch")
        return self.objects[key]


def _timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _archive_payload(
    *, symbol: str, event_at: datetime, bid: str, ask: str
) -> tuple[bytes, datetime]:
    started_at = event_at - timedelta(seconds=1)
    completed_at = event_at + timedelta(seconds=1)
    frames = [
        json.dumps([{"T": "success", "msg": "connected"}], separators=(",", ":")).encode(),
        json.dumps([{"T": "success", "msg": "authenticated"}], separators=(",", ":")).encode(),
        json.dumps(
            [
                {
                    "T": "subscription",
                    "trades": [symbol],
                    "quotes": [symbol],
                    "bars": [],
                    "updatedBars": [],
                    "dailyBars": [],
                    "statuses": [],
                    "lulds": [],
                    "corrections": [symbol],
                    "cancelErrors": [symbol],
                }
            ],
            separators=(",", ":"),
        ).encode(),
        json.dumps(
            [
                {
                    "T": "q",
                    "S": symbol,
                    "bx": "V",
                    "bp": bid,
                    "bs": 1,
                    "ax": "V",
                    "ap": ask,
                    "as": 1,
                    "c": ["R"],
                    "t": _timestamp(event_at),
                    "z": "C",
                }
            ],
            separators=(",", ":"),
        ).encode(),
    ]
    received = (started_at, started_at, event_at, completed_at)
    receipt = {
        "schema": SCHEMA,
        "source": "alpaca",
        "transport": "websocket",
        "origin": "wss://stream.data.alpaca.markets/v2/iex",
        "feed": "iex",
        "delivery_mode": "real_time",
        "symbol": symbol,
        "started_at": _timestamp(started_at),
        "ended_at": _timestamp(completed_at),
        "requested_duration_seconds": 60.0,
        "requested_max_frames": 4,
        "elapsed_monotonic_seconds": 1.0,
        "authenticated": True,
        "subscription_confirmed": True,
        "status": "bounded_complete",
        "stop_reason": "FRAME_LIMIT",
        "trade_count": 0,
        "quote_count": 1,
        "frame_count": 4,
        "frames": [
            {
                "file": f"frame-{index:05}.json",
                "received_at": _timestamp(received[index]),
                "raw_sha256": sha256(frame).hexdigest(),
                "raw_byte_length": len(frame),
            }
            for index, frame in enumerate(frames)
        ],
    }
    receipt_raw = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    payload = json.dumps(
        {
            "schema": ARCHIVE_SCHEMA,
            "receipt_base64": base64.b64encode(receipt_raw).decode(),
            "frames_base64": [base64.b64encode(frame).decode() for frame in frames],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return payload, completed_at


def _snapshot() -> PortfolioSnapshot:
    return PortfolioSnapshot(
        id=_SNAPSHOT_ID,
        portfolio_id=_PORTFOLIO_ID,
        status="READY",
        cash=(
            CashBalance(
                currency="USD",
                cash_balance="100",
                available_cash="100",
                provider_as_of=_VALUATION_TIME,
            ),
        ),
        positions=(
            BrokerPosition(
                instrument_id=_FIRST_ID,
                currency="USD",
                quantity="15",
                available_quantity="15",
                frozen_quantity="0",
                avg_cost="35",
                diluted_cost="35",
                current_price="40",
                provider_as_of=_VALUATION_TIME,
            ),
            BrokerPosition(
                instrument_id=_SECOND_ID,
                currency="USD",
                quantity="10",
                available_quantity="10",
                frozen_quantity="0",
                avg_cost="25",
                diluted_cost="25",
                current_price="30",
                provider_as_of=_VALUATION_TIME,
            ),
        ),
        open_orders=(),
        as_of=_VALUATION_TIME,
    )


def _metrics(*, cash: str, turnover: str, cost: TypedMetric) -> ScenarioMetricSetV1:
    return ScenarioMetricSetV1(
        security_top1=TypedMetric.known("0.60"),
        security_top3=TypedMetric.known("0.90"),
        security_hhi=TypedMetric.known("0.45"),
        cash_weight=TypedMetric.known(cash),
        one_way_turnover=TypedMetric.known(turnover),
        estimated_cost=cost,
    )


def _base_result(*, reduce_second: bool = False) -> PortfolioScenarioEvaluationV2:
    second_target = Decimal("0.25") if reduce_second else Decimal("0.30")
    first_target = Decimal("0.55") if reduce_second else Decimal("0.50")
    cash_target = Decimal("1") - first_target - second_target
    costs = ScenarioCostBreakdownV1(
        currency="USD",
        valuation_time=_VALUATION_TIME,
        basis="portfolio_value_ratio",
        commission=TypedMetric.unknown("BROKER_COMMISSION_SCHEDULE_UNAVAILABLE"),
        spread=TypedMetric.unknown("BID_ASK_EVIDENCE_UNAVAILABLE"),
        slippage=TypedMetric.unknown("SLIPPAGE_MODEL_UNAVAILABLE"),
        regulatory_fees=TypedMetric.unknown("REGULATORY_FEE_SCHEDULE_UNAVAILABLE"),
        tax_impact=TypedMetric.unknown("TAX_DATA_UNAVAILABLE"),
        total_estimated_cost_ex_tax=TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE"),
    )
    draft = PortfolioScenarioEvaluationV2(
        schema_version="portfolio_scenario_evaluation_v2",
        request_hash="a" * 64,
        result_hash="0" * 64,
        portfolio_snapshot_id=_SNAPSHOT_ID,
        diagnostic_report_id=_REPORT_ID,
        diagnostic_input_hash="b" * 64,
        valuation_time=_VALUATION_TIME,
        valuation_scope="currency:USD",
        objective="reduce_security_concentration",
        analysis_status="partial",
        objective_status="improved_not_met",
        baseline=_metrics(cash="0.10", turnover="0", cost=TypedMetric.known("0")),
        candidate=_metrics(
            cash=str(cash_target),
            turnover=str(cash_target - Decimal("0.10")),
            cost=TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE"),
        ),
        positions=(
            ScenarioPositionV1(
                instrument_id=_FIRST_ID,
                current_weight="0.60",
                target_weight=first_target,
                action="reduce",
                reason_code="CONCENTRATION_REDUCTION",
            ),
            ScenarioPositionV1(
                instrument_id=_SECOND_ID,
                current_weight="0.30",
                target_weight=second_target,
                action="reduce" if reduce_second else "maintain",
                reason_code=("CONCENTRATION_REDUCTION" if reduce_second else "BASELINE_WEIGHT"),
            ),
        ),
        cash_current_weight="0.10",
        cash_target_weight=cash_target,
        hard_constraint_reasons=("TURNOVER_BUDGET_EXHAUSTED",),
        unknown_metrics=("estimated_cost",),
        cost_breakdown=costs,
        execution_locked=True,
    )
    return draft.model_copy(update={"result_hash": canonical_scenario_hash(draft)})


async def _seed_identities(database_url: str) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session, session.begin():
        for instrument_id, symbol in ((_FIRST_ID, "AAPL"), (_SECOND_ID, "MSFT")):
            session.add(
                Instrument(
                    id=instrument_id,
                    asset_type="equity",
                    exchange="XNAS",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="us_equity",
                )
            )
            session.add(
                InstrumentAlias(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                )
            )


async def _seed_quote(
    database_url: str,
    store: Store,
    *,
    instrument_id: UUID = _FIRST_ID,
    symbol: str = "AAPL",
    event_at: datetime,
    bid: str = "39.90",
    ask: str = "40.10",
    sequence: int = 1,
) -> UUID:
    artifact, quote = _quote_records(
        store,
        instrument_id=instrument_id,
        symbol=symbol,
        event_at=event_at,
        bid=bid,
        ask=ask,
        sequence=sequence,
    )
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session, session.begin():
        session.add_all((artifact, quote))
    return quote.id


def _quote_records(
    store: Store,
    *,
    instrument_id: UUID = _FIRST_ID,
    symbol: str = "AAPL",
    event_at: datetime,
    bid: str = "39.90",
    ask: str = "40.10",
    sequence: int = 1,
) -> tuple[DataArtifact, LatestQuote]:
    payload, completed_at = _archive_payload(symbol=symbol, event_at=event_at, bid=bid, ask=ask)
    digest = sha256(payload).hexdigest()
    key = f"raw/market/alpaca/stream/{completed_at:%Y/%m/%d}/{digest}.json"
    store.objects[key] = payload
    artifact_id = UUID(f"019d1000-0000-7000-8100-{sequence:012d}")
    quote_id = UUID(f"019d1000-0000-7000-8200-{sequence:012d}")
    content_hash = sha256(
        canonical_quote_observation(
            instrument_id=instrument_id,
            source="alpaca",
            feed="iex",
            delivery_mode="real_time",
            event_kind="quote",
            bid=Decimal(bid),
            ask=Decimal(ask),
            last=None,
            observed_at=event_at,
            available_at=completed_at,
            received_at=completed_at,
            revision=digest,
            quality_flags=(),
        )
    ).hexdigest()
    return (
        DataArtifact(
            id=artifact_id,
            uri=f"memory://{key}",
            sha256=digest,
            byte_size=len(payload),
            mime_type="application/json",
            schema_version=ARCHIVE_SCHEMA,
            source="alpaca",
            observed_at=event_at,
            available_at=completed_at,
            received_at=completed_at,
        ),
        LatestQuote(
            id=quote_id,
            instrument_id=instrument_id,
            source="alpaca",
            feed="iex",
            delivery_mode="real_time",
            bid=Decimal(bid),
            ask=Decimal(ask),
            last=None,
            observed_at=event_at,
            available_at=completed_at,
            received_at=completed_at,
            revision=digest,
            quality_flags=[],
            event_kind="quote",
            content_hash=content_hash,
            evidence_status="verified",
            data_artifact_id=artifact_id,
        ),
    )


@pytest.mark.asyncio
async def test_selects_45_second_quote_and_exactly_replays_frozen_manifest(
    migrated_database_url: str,
) -> None:
    await _seed_identities(migrated_database_url)
    store = Store()
    quote_id = await _seed_quote(
        migrated_database_url,
        store,
        event_at=_VALUATION_TIME - timedelta(seconds=45),
    )
    snapshot = _snapshot()
    base_result = _base_result()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session:
        evidence = await select_scenario_spread_evidence(session, store, snapshot, base_result)
    assert evidence.input.policy_status == "available"
    assert tuple(item.latest_quote_id for item in evidence.input.selected_quotes) == (quote_id,)
    assert evidence.quotes[0].bid == Decimal("39.90")
    result = apply_scenario_spread_cost(
        snapshot=snapshot, base_result=base_result, evidence=evidence
    )
    assert result.cost_breakdown.spread == TypedMetric.known("0.00025")

    manifest = finalize_scenario_spread_manifest(
        evidence.input, expected_result_hash=result.result_hash
    )
    async with factory() as session:
        replayed = await replay_scenario_spread_evidence(
            session, store, snapshot, base_result, manifest
        )
    assert replayed == evidence


@pytest.mark.asyncio
async def test_quote_older_than_45_seconds_is_not_selected(
    migrated_database_url: str,
) -> None:
    await _seed_identities(migrated_database_url)
    store = Store()
    await _seed_quote(
        migrated_database_url,
        store,
        event_at=_VALUATION_TIME - timedelta(seconds=46),
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session:
        evidence = await select_scenario_spread_evidence(
            session, store, _snapshot(), _base_result()
        )

    assert evidence.input.policy_status == "available"
    assert evidence.input.selected_quotes == ()
    assert evidence.quotes == ()


@pytest.mark.asyncio
async def test_missing_one_reduced_quote_keeps_spread_unknown(
    migrated_database_url: str,
) -> None:
    await _seed_identities(migrated_database_url)
    store = Store()
    await _seed_quote(
        migrated_database_url,
        store,
        event_at=_VALUATION_TIME - timedelta(seconds=10),
    )
    snapshot = _snapshot()
    base_result = _base_result(reduce_second=True)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session:
        evidence = await select_scenario_spread_evidence(session, store, snapshot, base_result)
    result = apply_scenario_spread_cost(
        snapshot=snapshot, base_result=base_result, evidence=evidence
    )

    assert tuple(item.instrument_id for item in evidence.input.selected_quotes) == (_FIRST_ID,)
    assert result.cost_breakdown.spread == TypedMetric.unknown("BID_ASK_EVIDENCE_UNAVAILABLE")


def _at_time(
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
    valuation_time: datetime,
) -> tuple[PortfolioSnapshot, PortfolioScenarioEvaluationV2]:
    shifted_snapshot = snapshot.model_copy(update={"as_of": valuation_time})
    shifted = base_result.model_copy(
        update={
            "valuation_time": valuation_time,
            "cost_breakdown": base_result.cost_breakdown.model_copy(
                update={"valuation_time": valuation_time}
            ),
        }
    )
    return shifted_snapshot, shifted.model_copy(
        update={"result_hash": canonical_scenario_hash(shifted)}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "valuation_time",
    (
        datetime(2026, 9, 10, 12, 0, tzinfo=UTC),
        datetime(2025, 12, 31, 15, 0, tzinfo=UTC),
    ),
)
async def test_outside_regular_session_or_pre_policy_snapshot_is_unavailable(
    migrated_database_url: str,
    valuation_time: datetime,
) -> None:
    snapshot, base_result = _at_time(_snapshot(), _base_result(), valuation_time)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session:
        evidence = await select_scenario_spread_evidence(session, Store(), snapshot, base_result)

    assert evidence.input.policy_status == "unavailable"
    assert evidence.input.source_policy_id is None
    assert evidence.input.calendar_authority is None
    assert evidence.input.selected_quotes == ()
    assert evidence.available_at is None


@pytest.mark.asyncio
async def test_newer_quote_changes_new_selection_but_not_exact_replay(
    migrated_database_url: str,
) -> None:
    await _seed_identities(migrated_database_url)
    store = Store()
    first_id = await _seed_quote(
        migrated_database_url,
        store,
        event_at=_VALUATION_TIME - timedelta(seconds=30),
        sequence=1,
    )
    snapshot = _snapshot()
    base_result = _base_result()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        first = await select_scenario_spread_evidence(session, store, snapshot, base_result)
    first_result = apply_scenario_spread_cost(
        snapshot=snapshot, base_result=base_result, evidence=first
    )
    manifest = finalize_scenario_spread_manifest(
        first.input, expected_result_hash=first_result.result_hash
    )

    second_id = await _seed_quote(
        migrated_database_url,
        store,
        event_at=_VALUATION_TIME - timedelta(seconds=5),
        bid="39.80",
        ask="40.20",
        sequence=2,
    )
    async with factory() as session:
        second = await select_scenario_spread_evidence(session, store, snapshot, base_result)
        replayed = await replay_scenario_spread_evidence(
            session, store, snapshot, base_result, manifest
        )

    assert first.input.selected_quotes[0].latest_quote_id == first_id
    assert second.input.selected_quotes[0].latest_quote_id == second_id
    assert replayed == first


@pytest.mark.asyncio
async def test_equal_time_quotes_use_descending_quote_id_as_stable_tiebreaker(
    migrated_database_url: str,
) -> None:
    await _seed_identities(migrated_database_url)
    store = Store()
    event_at = _VALUATION_TIME - timedelta(seconds=10)
    await _seed_quote(
        migrated_database_url,
        store,
        event_at=event_at,
        bid="39.90",
        ask="40.10",
        sequence=1,
    )
    expected_id = await _seed_quote(
        migrated_database_url,
        store,
        event_at=event_at,
        bid="39.80",
        ask="40.20",
        sequence=2,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session:
        evidence = await select_scenario_spread_evidence(
            session, store, _snapshot(), _base_result()
        )

    assert evidence.input.selected_quotes[0].latest_quote_id == expected_id
    assert evidence.quotes[0].bid == Decimal("39.80")


@pytest.mark.asyncio
async def test_replay_rejects_changed_bid_with_unchanged_stored_hash(
    migrated_database_url: str,
) -> None:
    await _seed_identities(migrated_database_url)
    store = Store()
    quote_id = await _seed_quote(
        migrated_database_url,
        store,
        event_at=_VALUATION_TIME - timedelta(seconds=10),
    )
    snapshot = _snapshot()
    base_result = _base_result()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        evidence = await select_scenario_spread_evidence(session, store, snapshot, base_result)
    result = apply_scenario_spread_cost(
        snapshot=snapshot, base_result=base_result, evidence=evidence
    )
    manifest = finalize_scenario_spread_manifest(
        evidence.input, expected_result_hash=result.result_hash
    )
    async with factory() as session, session.begin():
        await session.execute(text("SET LOCAL session_replication_role = 'replica'"))
        await session.execute(
            text("UPDATE latest_quotes SET bid = 39.80 WHERE id = :quote_id"),
            {"quote_id": quote_id},
        )

    async with factory() as session:
        with pytest.raises(ScenarioSpreadEvidenceReplayInvalid):
            await replay_scenario_spread_evidence(session, store, snapshot, base_result, manifest)


@pytest.mark.asyncio
async def test_replay_rejects_tampered_artifact_bytes(
    migrated_database_url: str,
) -> None:
    await _seed_identities(migrated_database_url)
    store = Store()
    await _seed_quote(
        migrated_database_url,
        store,
        event_at=_VALUATION_TIME - timedelta(seconds=10),
    )
    snapshot = _snapshot()
    base_result = _base_result()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        evidence = await select_scenario_spread_evidence(session, store, snapshot, base_result)
    result = apply_scenario_spread_cost(
        snapshot=snapshot, base_result=base_result, evidence=evidence
    )
    manifest = finalize_scenario_spread_manifest(
        evidence.input, expected_result_hash=result.result_hash
    )
    key = next(iter(store.objects))
    store.objects[key] += b"tampered"

    async with factory() as session:
        with pytest.raises(ScenarioSpreadEvidenceReplayInvalid):
            await replay_scenario_spread_evidence(session, store, snapshot, base_result, manifest)


@pytest.mark.asyncio
async def test_selection_cannot_see_quote_committed_after_statement_snapshot(
    migrated_database_url: str,
) -> None:
    await _seed_identities(migrated_database_url)
    store = Store()
    artifact, quote = _quote_records(
        store,
        event_at=_VALUATION_TIME - timedelta(seconds=10),
    )
    snapshot = _snapshot()
    base_result = _base_result()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as writer:
        async with writer.begin():
            writer.add_all((artifact, quote))
            await writer.flush()
            async with factory() as reader:
                invisible = await select_scenario_spread_evidence(
                    reader, store, snapshot, base_result
                )
            assert invisible.input.selected_quotes == ()

    async with factory() as reader:
        visible = await select_scenario_spread_evidence(reader, store, snapshot, base_result)
    assert tuple(item.latest_quote_id for item in visible.input.selected_quotes) == (quote.id,)
