import asyncio
import inspect
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import permutations
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from nexus.persistence.models.data import ProviderCapability, SourcePolicy
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.persistence.session import session_factory
from nexus.providers.base import PriceBar, PriceSeries
from nexus.quality.source_policy import (
    INTRADAY_SOURCE_POLICY_V2_ID,
    QUOTE_SOURCE_POLICY_V2_ID,
    SOURCE_POLICY_V2_ID,
    load_fosun_market_admission,
    load_intraday_source_policy_v2,
    load_quote_source_policy_v2,
    load_source_policy_v2,
    select_price_series,
)
from nexus.settings import EngineSettings
from sqlalchemy import select

INSTRUMENT_ID = UUID("29e18e7a-78fb-5fc1-a65a-6641fbc1cc64")
DECISION_TIME = datetime(2026, 8, 21, 22, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy_id", "loader"),
    [
        (
            SOURCE_POLICY_V2_ID,
            lambda session, decision_time: load_source_policy_v2(
                session,
                instrument_tick_size=None,
                decision_time=decision_time,
            ),
        ),
        (QUOTE_SOURCE_POLICY_V2_ID, load_quote_source_policy_v2),
        (INTRADAY_SOURCE_POLICY_V2_ID, load_intraday_source_policy_v2),
    ],
)
async def test_v2_policy_is_not_visible_before_its_publication_time(
    migrated_database_url: str,
    policy_id: UUID,
    loader: object,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        record = await session.scalar(select(SourcePolicy).where(SourcePolicy.id == policy_id))
        assert record is not None
        with pytest.raises(ValueError, match="missing or invalid"):
            await loader(  # type: ignore[operator]
                session,
                decision_time=record.created_at - timedelta(microseconds=1),
            )


def test_public_selector_is_async_and_owns_all_authority_loading() -> None:
    signature = inspect.signature(select_price_series)

    assert inspect.iscoroutinefunction(select_price_series)
    assert tuple(signature.parameters) == (
        "session",
        "candidates",
        "instrument_tick_size",
        "decision_time",
    )
    assert "policy" not in signature.parameters
    assert "fosun_admission" not in signature.parameters


def test_missing_database_policy_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(Config(str(engine_root / "alembic.ini")), "0004_market_master")

    async def select_without_policy():
        factory = session_factory(EngineSettings(database_url=temporary_database_url))
        async with factory() as session:
            return await select_price_series(
                session=session,
                candidates=(_series("alpaca", "100"),),
                instrument_tick_size=Decimal("0.01"),
                decision_time=DECISION_TIME,
            )

    result = asyncio.run(select_without_policy())
    assert result.selected is None
    assert result.quality == "FAIL"
    assert result.reasons == ("SOURCE_POLICY_UNAVAILABLE",)


@pytest.mark.asyncio
async def test_historical_policy_rejects_a_bypassed_delivery_mode_contradiction(
    migrated_database_url: str,
) -> None:
    """Even a model-copy bypass cannot label delayed SIP history as real-time."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    contradictory = _series("alpaca", "100").model_copy(
        update={"delivery_mode": "real_time"}
    )
    async with factory() as session:
        result = await _select(session, (contradictory,))

    assert result.selected is None
    assert result.reasons == ("NO_ELIGIBLE_SOURCE",)


@pytest.mark.asyncio
async def test_public_selector_rejects_caller_authority_parameters(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        with pytest.raises(TypeError):
            await select_price_series(  # type: ignore[call-arg]
                session=session,
                candidates=(_series("alpaca", "100"),),
                instrument_tick_size=Decimal("0.01"),
                decision_time=DECISION_TIME,
                policy=object(),
            )
        with pytest.raises(TypeError):
            await select_price_series(  # type: ignore[call-arg]
                session=session,
                candidates=(_series("alpaca", "100"),),
                instrument_tick_size=Decimal("0.01"),
                decision_time=DECISION_TIME,
                fosun_admission=object(),
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expires_at", "invalidated_at"),
    [
        ({}, DECISION_TIME + timedelta(hours=1), None),
        (
            {"operation": "fosun_sync_portfolio", "market_scope": "US"},
            DECISION_TIME + timedelta(hours=1),
            None,
        ),
        (
            {"operation": "fosun_get_bars", "market_scope": "HK"},
            DECISION_TIME + timedelta(hours=1),
            None,
        ),
        (
            {"operation": "fosun_get_bars", "market_scope": "US"},
            DECISION_TIME,
            None,
        ),
        (
            {"operation": "fosun_get_bars", "market_scope": "US"},
            DECISION_TIME + timedelta(hours=1),
            DECISION_TIME - timedelta(hours=1),
        ),
    ],
)
async def test_public_selector_never_falls_back_when_fosun_authority_is_invalid(
    migrated_database_url: str,
    payload: dict[str, str],
    expires_at: datetime,
    invalidated_at: datetime | None,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            ProviderCapability(
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="a" * 64,
                expires_at=expires_at,
                schema_version="1",
                payload=payload,
                created_at=DECISION_TIME - timedelta(hours=2),
            )
        )
        session.add(
            IntegrationStatus(
                provider="fosun",
                status="read_only",
                admitted_contract_hash="a" * 64,
                mode_changed_at=DECISION_TIME - timedelta(hours=1),
                capability_invalidated_at=invalidated_at,
            )
        )
        await session.commit()
        candidates = (
            _series("fosun", "100", contract_hash="a" * 64),
            _series("alpaca", "100"),
        )

        result = await select_price_series(
            session,
            candidates,
            Decimal("0.01"),
            DECISION_TIME,
        )

    assert result.selected is None
    assert result.quality == "FAIL"


@pytest.mark.asyncio
async def test_public_selector_returns_unavailable_without_fosun_capability(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        candidates = (
            _series("fosun", "100", contract_hash="a" * 64),
            _series("alpaca", "100"),
        )
        result = await select_price_series(
            session, candidates, Decimal("0.01"), DECISION_TIME
        )

    assert result.selected is None
    assert result.quality == "FAIL"


@pytest.mark.asyncio
async def test_future_created_capability_cannot_authorize_past_decision(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            ProviderCapability(
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="a" * 64,
                expires_at=DECISION_TIME + timedelta(hours=2),
                schema_version="1",
                payload={
                    "operation": "fosun_get_bars",
                    "market_scope": "US",
                    "delivery_mode": "unknown",
                },
                created_at=DECISION_TIME + timedelta(minutes=1),
            )
        )
        session.add(
            IntegrationStatus(
                provider="fosun",
                status="read_only",
                admitted_contract_hash="a" * 64,
                mode_changed_at=DECISION_TIME - timedelta(hours=1),
            )
        )
        await session.commit()
        candidates = (
            _series("fosun", "100", contract_hash="a" * 64),
            _series("alpaca", "100"),
        )

        result = await _select(session, candidates)

    assert result.selected is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_created_at", "mode_changed_at"),
    [
        (
            DECISION_TIME + timedelta(minutes=1),
            DECISION_TIME - timedelta(hours=1),
        ),
        (
            DECISION_TIME - timedelta(hours=2),
            DECISION_TIME + timedelta(minutes=1),
        ),
    ],
)
async def test_future_status_cannot_authorize_past_market_decision(
    migrated_database_url: str,
    status_created_at: datetime,
    mode_changed_at: datetime,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            ProviderCapability(
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="a" * 64,
                expires_at=DECISION_TIME + timedelta(hours=2),
                schema_version="1",
                payload={
                    "operation": "fosun_get_bars",
                    "market_scope": "US",
                    "delivery_mode": "unknown",
                },
                created_at=DECISION_TIME - timedelta(hours=2),
            )
        )
        session.add(
            IntegrationStatus(
                provider="fosun",
                status="read_only",
                admitted_contract_hash="a" * 64,
                created_at=status_created_at,
                mode_changed_at=mode_changed_at,
            )
        )
        await session.commit()

        admission = await load_fosun_market_admission(
            session,
            decision_time=DECISION_TIME,
            operation="fosun_get_bars",
            market_scope="US",
        )

    assert admission is None


def _bar(
    close: str,
    *,
    available_at: datetime = DECISION_TIME,
    observed_at: datetime | None = None,
    revision: str = "1",
) -> PriceBar:
    price = Decimal(close)
    return PriceBar(
        instrument_id=INSTRUMENT_ID,
        session=date(2026, 8, 21),
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("1000"),
        available_at=available_at,
        observed_at=observed_at,
        received_at=available_at,
        revision=revision,
    )


def _series(
    source: str,
    close: str,
    *,
    interval: str = "1Day",
    quality: str = "PASS",
    bars: tuple[PriceBar, ...] | None = None,
    adjustment: str | None = None,
    delivery_mode: str | None = None,
    contract_hash: str | None = None,
) -> PriceSeries:
    mode = (
        "read_only"
        if source == "fosun"
        else "fixture"
        if source == "fixture"
        else "delayed"
    )
    resolved_delivery = (
        delivery_mode
        if delivery_mode is not None
        else "real_time"
        if source == "fosun"
        else mode
    )
    return PriceSeries(
        source=source,
        feed="l1" if source == "fosun" else "sip",
        adjustment=("raw" if source == "fosun" else "all")
        if adjustment is None
        else adjustment,
        interval=interval,
        mode=mode,
        delivery_mode=resolved_delivery,
        quality=quality,
        contract_hash=contract_hash if source == "fosun" else None,
        bars=bars or (_bar(close),),
    )


async def _select(
    session,
    candidates: tuple[PriceSeries, ...],
    tick_size: Decimal | None = Decimal("0.01"),
):
    return await select_price_series(session, candidates, tick_size, DECISION_TIME)


@pytest.mark.asyncio
async def test_current_selection_uses_only_bound_fosun_and_never_falls_back(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            ProviderCapability(
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="a" * 64,
                expires_at=DECISION_TIME + timedelta(hours=1),
                schema_version="1",
                payload={
                    "operation": "fosun_get_bars",
                    "market_scope": "US",
                    "delivery_mode": "real_time",
                },
                created_at=DECISION_TIME - timedelta(hours=2),
            )
        )
        session.add(
            IntegrationStatus(
                provider="fosun",
                status="read_only",
                admitted_contract_hash="a" * 64,
                mode_changed_at=DECISION_TIME - timedelta(hours=1),
            )
        )
        await session.commit()
        fosun = _series("fosun", "100.00", contract_hash="a" * 64)
        alpaca = _series("alpaca", "100.01")
        preferred = await _select(session, (alpaca, fosun))
        assert (preferred.quality, preferred.selected) == ("PASS", fosun)

        contract_mismatch = await _select(
            session,
            (
                _series("fosun", "100.00", contract_hash="b" * 64),
                alpaca,
            ),
        )
        assert contract_mismatch.selected is None
        assert (await _select(session, (alpaca,))).selected is None
        assert (
            await _select(
                session,
                (fosun.model_copy(update={"adjustment": "all"}), alpaca),
            )
        ).selected is None

        old = _bar("99", available_at=DECISION_TIME - timedelta(minutes=2), revision="1")
        latest = _bar(
            "100", available_at=DECISION_TIME - timedelta(minutes=1), revision="2"
        )
        future = _bar(
            "500", available_at=DECISION_TIME + timedelta(seconds=1), revision="3"
        )
        revision_results = {
            (
                await _select(
                    session,
                    (
                        _series(
                            "fosun",
                            "0",
                            bars=order,
                            contract_hash="a" * 64,
                        ),
                    ),
                )
            ).model_dump_json()
            for order in permutations((old, latest, future))
        }
        assert len(revision_results) == 1
        selected_revision = await _select(
            session,
            (
                _series(
                    "fosun",
                    "0",
                    bars=(future, old, latest),
                    contract_hash="a" * 64,
                ),
            ),
        )
        assert selected_revision.selected is not None
        assert selected_revision.selected.bars == (latest,)
        assert selected_revision.quality == "PARTIAL"
        assert selected_revision.reasons == ("FUTURE_DATA_EXCLUDED",)

        future_only = _series(
            "fosun",
            "100",
            contract_hash="a" * 64,
            bars=(
                _bar(
                    "100",
                    available_at=DECISION_TIME + timedelta(microseconds=1),
                ),
            ),
        )
        assert (await _select(session, (future_only,))).reasons == (
            "NO_POINT_IN_TIME_DATA",
        )
        assert (await _select(session, (_series("other", "100"),))).reasons == (
            "NO_ELIGIBLE_SOURCE",
        )
