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
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.persistence.session import session_factory
from nexus.providers.base import PriceBar, PriceSeries
from nexus.quality.source_policy import select_price_series
from nexus.settings import EngineSettings

INSTRUMENT_ID = UUID("29e18e7a-78fb-5fc1-a65a-6641fbc1cc64")
DECISION_TIME = datetime(2026, 8, 21, 22, tzinfo=UTC)


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
async def test_public_selector_falls_back_when_task5_authority_is_invalid(
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

    assert result.selected == candidates[1]


@pytest.mark.asyncio
async def test_public_selector_falls_back_without_task5_capability(
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

    assert result.selected == candidates[1]


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
    adjustment: str = "all",
    contract_hash: str | None = None,
) -> PriceSeries:
    return PriceSeries(
        source=source,
        feed="authorized" if source == "fosun" else "sip",
        adjustment=adjustment,
        interval=interval,
        mode="read_only" if source == "fosun" else "delayed",
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
async def test_selection_contract_uses_only_database_loaded_policy_and_admission(
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
                payload={"operation": "fosun_get_bars", "market_scope": "US"},
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
        assert contract_mismatch.selected == alpaca

        conflict = await _select(session, (fosun, _series("alpaca", "100.21")))
        assert (conflict.selected, conflict.quality, conflict.reasons) == (
            None,
            "FAIL",
            ("SOURCE_CONFLICT",),
        )
        unknown_tick = await _select(
            session, (fosun, _series("alpaca", "100.00")), None
        )
        adjustment_mismatch = await _select(
            session,
            (fosun, _series("alpaca", "100.00", adjustment="raw")),
            None,
        )
        assert (unknown_tick.quality, unknown_tick.reasons) == (
            "PARTIAL",
            ("TICK_SIZE_UNKNOWN",),
        )
        assert adjustment_mismatch.reasons == ("SOURCE_CONFLICT",)

        candidates = (
            _series("alpaca", "100", quality="PARTIAL"),
            _series("alpaca", "101", quality="PASS"),
            _series("fixture", "99", quality="PASS"),
        )
        candidate_results = {
            (await _select(session, order)).model_dump_json()
            for order in permutations(candidates)
        }
        assert len(candidate_results) == 1
        assert (await _select(session, candidates)).selected == candidates[1]

        old = _bar("99", available_at=DECISION_TIME - timedelta(minutes=2), revision="1")
        latest = _bar(
            "100", available_at=DECISION_TIME - timedelta(minutes=1), revision="2"
        )
        future = _bar(
            "500", available_at=DECISION_TIME + timedelta(seconds=1), revision="3"
        )
        revision_results = {
            (
                await _select(session, (_series("alpaca", "0", bars=order),))
            ).model_dump_json()
            for order in permutations((old, latest, future))
        }
        assert len(revision_results) == 1
        selected_revision = await _select(
            session, (_series("alpaca", "0", bars=(future, old, latest)),)
        )
        assert selected_revision.selected is not None
        assert selected_revision.selected.bars == (latest,)

        instant = DECISION_TIME - timedelta(minutes=1)
        intraday_fosun = _series(
            "fosun",
            "0",
            interval="1Min",
            contract_hash="a" * 64,
            bars=(
                _bar("80", observed_at=instant, available_at=instant, revision="1"),
                _bar("100", observed_at=instant, revision="2"),
            ),
        )
        mismatched_instant = _series(
            "alpaca",
            "0",
            interval="1Min",
            bars=(_bar("200", observed_at=instant - timedelta(minutes=1)),),
        )
        matching_instant = _series(
            "alpaca",
            "0",
            interval="1Min",
            bars=(_bar("101", observed_at=instant),),
        )
        assert (await _select(session, (intraday_fosun, mismatched_instant))).reasons == ()
        assert (
            await _select(session, (intraday_fosun, matching_instant))
        ).reasons == ("INTRADAY_DIVERGENCE_ALERT",)

        future_only = _series(
            "alpaca",
            "100",
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
