import json
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from nexus.agents.portfolio_synthesis_contracts import SYNTHESIS_PROMPT_VERSION
from nexus.ops import agent_paper_loop as loop_module
from nexus.ops import agent_paper_once as candidate_cli
from nexus.ops import agent_paper_universe_local as universe_cli
from nexus.ops.agent_paper_once import AgentPaperPrepareCandidatesRequest
from nexus.paper import agent_cycle
from nexus.paper import candidate_batch as batch_module
from nexus.paper.agent_cycle_authority import load_quasi_realtime_cycle_authority
from nexus.paper.candidate_batch import (
    create_agent_paper_candidate_batch,
    load_agent_paper_candidate_batch,
)
from nexus.paper.universe_publisher import publish_reviewed_research_universe
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    SectorExposure,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import (
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCandidateBatchRecord,
    AgentPaperCycleRecord,
    PaperAccountRecord,
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.portfolio import Account, Portfolio
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.persistence.session import session_factory
from nexus.risk.query_authority_producer import inspect_query_authority_scope
from nexus.settings import EngineSettings
from sqlalchemy import func, literal, select, text

NOW = datetime(2026, 9, 18, 14, tzinfo=UTC)
ACCOUNT_ID = UUID("10000000-0000-0000-0000-000000000001")
BROKER_ACCOUNT_ID = UUID("10000000-0000-0000-0000-000000000002")
PORTFOLIO_ID = UUID("10000000-0000-0000-0000-000000000003")
UNIVERSE_ID = UUID("10000000-0000-0000-0000-000000000004")
AAPL_ID = UUID("20000000-0000-0000-0000-000000000001")
SPY_ID = UUID("20000000-0000-0000-0000-000000000002")
OUTSIDE_ID = UUID("20000000-0000-0000-0000-000000000003")
MSFT_ID = UUID("20000000-0000-0000-0000-000000000004")
NVDA_ID = UUID("20000000-0000-0000-0000-000000000005")
GOOG_ID = UUID("20000000-0000-0000-0000-000000000006")
REVIEWED_UNIVERSE_ID = UUID("10000000-0000-0000-0000-000000000007")


@pytest.mark.asyncio
async def test_reviewed_universe_publisher_requires_evidence_and_replays_immutable_scope(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_scope(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    created = NOW.replace(day=17)
    ids = (AAPL_ID, NVDA_ID)
    monkeypatch.setattr(universe_cli, "func", SimpleNamespace(clock_timestamp=lambda: literal(NOW)))
    blocked = await universe_cli.run(
        EngineSettings(database_url=migrated_database_url),
        instrument_ids=ids, effective_from=NOW.date(), publish=False,
    )
    assert blocked["status"] == "blocked"
    assert blocked["candidates"] == [
        {"symbol": "AAPL", "missing": ["RESEARCH_SNAPSHOT_MISSING", "REVIEWED_SECTOR_MISSING"]},
        {"symbol": "NVDA", "missing": ["RESEARCH_SNAPSHOT_MISSING", "REVIEWED_SECTOR_MISSING"]},
    ]
    with pytest.raises(ValueError, match="REVIEWED_UNIVERSE_EVIDENCE_MISSING"):
        async with factory() as session, session.begin():
            await publish_reviewed_research_universe(
                session, instrument_ids=ids, effective_from=NOW.date(), as_of=NOW,
            )
    async with factory() as session, session.begin():
        for index, instrument_id in enumerate(ids, start=1):
            evidence_hash = str(index + 3) * 64
            evidence = ResearchEvidenceRef(
                source="sec", provider_object_id=f"reviewed:{instrument_id}",
                revision="v1", available_at=created,
                location=f"memory://sha256/{evidence_hash}",
                content_hash=evidence_hash, byte_length=10, created_at=created,
            )
            snapshot = DataSnapshot(
                source="nexus_research", provider_object_id=str(instrument_id),
                revision="reviewed-v1", observed_at=created, available_at=created,
                received_at=created, schema_version="research_filings_v1",
                payload={"scope": "persisted_research_observations"}, created_at=created,
            )
            session.add_all((evidence, snapshot))
            await session.flush()
            session.add(Filing(
                instrument_id=instrument_id, evidence_ref_id=evidence.id,
                source=evidence.source, provider_object_id=evidence.provider_object_id,
                revision=evidence.revision, accession_number=f"000000000{index}-26-000001",
                form="10-Q", filed_at=created, accepted_at=created, created_at=created,
            ))
            await session.flush()
            session.add(AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=snapshot.id, instrument_id=instrument_id,
                evidence_ref_id=evidence.id, authority_kind="research", selected_at=None,
                source=evidence.source, provider_object_id=evidence.provider_object_id,
                revision=evidence.revision, available_at=created,
                location=evidence.location, content_hash=evidence.content_hash,
                created_at=created,
            ))
            session.add(SectorExposure(
                instrument_id=instrument_id, sector_code="information_technology",
                weight=Decimal(1), as_of=created.date(),
                source_url="https://example.test/reviewed-sector",
                content_hash=evidence_hash,
                synthetic=index == 2, reviewed=True, created_at=created,
            ))
    with pytest.raises(ValueError, match="REVIEWED_UNIVERSE_EVIDENCE_MISSING"):
        async with factory() as session, session.begin():
            await publish_reviewed_research_universe(
                session, instrument_ids=ids, effective_from=NOW.date(), as_of=NOW,
            )
    async with factory() as session, session.begin():
        session.add(SectorExposure(
            instrument_id=NVDA_ID, sector_code="information_technology",
            weight=Decimal(1), as_of=created.date(),
            source_url="https://example.test/reviewed-sector",
            content_hash="f" * 64,
            synthetic=False, reviewed=True, created_at=created,
        ))
    async with factory() as session, session.begin():
        first = await publish_reviewed_research_universe(
            session, instrument_ids=ids, effective_from=NOW.date(), as_of=NOW,
        )
        replay = await publish_reviewed_research_universe(
            session, instrument_ids=tuple(reversed(ids)), effective_from=NOW.date(), as_of=NOW,
        )
        members = tuple(await session.scalars(select(UniverseMember).where(
            UniverseMember.universe_snapshot_id == first.id,
        )))
        published = await session.scalar(select(func.count()).select_from(UniverseSnapshot))
    assert first.id == replay.id
    assert first.published_at == NOW
    assert first.key == "agent_paper_research"
    assert first.version == f"reviewed_v1_{first.content_hash[:16]}"
    assert {member.instrument_id for member in members} == set(ids)
    assert all(
        "research=" in member.inclusion_reason and "sector=" in member.inclusion_reason
        for member in members
    )
    assert published == 2
    future_session = date(2026, 9, 19)
    checked = await universe_cli.run(
        EngineSettings(database_url=migrated_database_url),
        instrument_ids=ids, effective_from=future_session, publish=False,
    )
    async with factory() as session:
        count_after_check = await session.scalar(select(func.count()).select_from(UniverseSnapshot))
    published_result = await universe_cli.run(
        EngineSettings(database_url=migrated_database_url),
        instrument_ids=ids, effective_from=future_session, publish=True,
    )
    assert checked["status"] == "publishable"
    assert count_after_check == 2
    assert published_result["status"] == "published"
    assert published_result["universe_snapshot_id"] != str(first.id)

    async with factory() as session, session.begin():
        session.add(SectorExposure(
            instrument_id=AAPL_ID, sector_code="information_technology",
            weight=Decimal(1), as_of=created.date(),
            source_url="https://example.test/duplicate-reviewed-sector",
            content_hash="e" * 64,
            synthetic=False, reviewed=True, created_at=NOW,
        ))
    duplicate = await universe_cli.run(
        EngineSettings(database_url=migrated_database_url),
        instrument_ids=ids, effective_from=future_session, publish=False,
    )
    assert duplicate["status"] == "blocked"
    assert duplicate["candidates"] == [
        {"symbol": "AAPL", "missing": ["REVIEWED_SECTOR_INVALID"]},
        {"symbol": "NVDA", "missing": []},
    ]
    with pytest.raises(ValueError, match="REVIEWED_UNIVERSE_EVIDENCE_MISSING"):
        async with factory() as session, session.begin():
            await publish_reviewed_research_universe(
                session, instrument_ids=ids, effective_from=future_session, as_of=NOW,
            )


@pytest.mark.asyncio
async def test_reviewed_research_universe_accepts_multi_stock_without_rewriting_legacy(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_scope(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    created = NOW.replace(day=17)
    async with factory() as session, session.begin():
        calendar_id = uuid4()
        session.add(ExchangeCalendarAuthorityRecord(
            id=calendar_id,
            exchange="XNYS",
            version="reviewed-universe-fixture",
            source="fixture",
            range_start=date(2026, 9, 17),
            range_end=date(2026, 9, 30),
            query_parameters={},
            completeness_status="complete",
            content_hash="c" * 64,
            published_at=created,
            created_at=created,
        ))
        session.add_all(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=calendar_id,
                session=date(2026, 9, day),
                market_open_at=datetime(2026, 9, day, 13, 30, tzinfo=UTC),
                market_close_at=datetime(2026, 9, day, 20, tzinfo=UTC),
                decision_at=datetime(2026, 9, day, 21, tzinfo=UTC),
                created_at=created,
            )
            for day in (17, 18, 21)
        )
        session.add(UniverseSnapshot(
            id=REVIEWED_UNIVERSE_ID,
            key="agent_paper_research",
            version=f"reviewed_v1_{'b' * 16}",
            effective_from=date(2026, 9, 18),
            source="nexus_local_reviewed_research",
            content_hash="b" * 64,
            published_at=None,
            created_at=created,
        ))
        await session.flush()
        session.add_all(
            UniverseMember(
                universe_snapshot_id=REVIEWED_UNIVERSE_ID,
                instrument_id=instrument_id,
                inclusion_reason="reviewed research scope",
                created_at=created,
            )
            for instrument_id in (AAPL_ID, NVDA_ID)
        )
        await session.flush()
        await session.execute(
            text("UPDATE universe_snapshots SET published_at=:at WHERE id=:id"),
            {"at": created, "id": REVIEWED_UNIVERSE_ID},
        )
        batch = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=REVIEWED_UNIVERSE_ID,
            instrument_ids=(AAPL_ID, NVDA_ID),
            idempotency_key="reviewed-multi-stock-scope",
            created_at=NOW,
        )
    async with factory() as session, session.begin():
        validated, identities = await batch_module.validate_agent_paper_candidate_batch(
            session, batch_id=batch.batch_id, account_id=ACCOUNT_ID, as_of=NOW,
        )
        readiness = await batch_module.inspect_agent_paper_candidate_static_readiness(
            session, account_id=ACCOUNT_ID, batch_id=batch.batch_id, as_of=NOW,
        )
        authority = await load_quasi_realtime_cycle_authority(
            session,
            account_id=ACCOUNT_ID,
            now=NOW,
            expected_calendar_authority_id=calendar_id,
            expected_universe_snapshot_id=REVIEWED_UNIVERSE_ID,
        )
        risk_scope = await inspect_query_authority_scope(
            session,
            decision_session=date(2026, 9, 18),
            symbol="NVDA",
            as_of=NOW,
            universe_snapshot_id=REVIEWED_UNIVERSE_ID,
            source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
        )
        legacy = await session.get(UniverseSnapshot, UNIVERSE_ID)
    assert validated == batch
    assert {identity.symbol for identity in identities} == {"AAPL", "NVDA"}
    assert {identity.universe_snapshot_id for identity in identities} == {REVIEWED_UNIVERSE_ID}
    assert readiness["universe_status"] == "matched"
    assert readiness["status"] == "blocked"
    assert {candidate["symbol"] for candidate in readiness["candidates"]} == {"AAPL", "NVDA"}
    assert authority.universe_snapshot_id == REVIEWED_UNIVERSE_ID
    assert risk_scope.instrument_id == NVDA_ID
    assert legacy is not None and legacy.version == "us_liquid_v1"
    monkeypatch.setattr(loop_module, "func", SimpleNamespace(clock_timestamp=lambda: literal(NOW)))
    host_candidates = await loop_module._load_loop_candidates(
        EngineSettings(database_url=migrated_database_url),
        account_id=ACCOUNT_ID,
        batch_id=batch.batch_id,
        expected_calendar_authority_id=calendar_id,
    )
    assert {candidate.symbol for candidate in host_candidates} == {"AAPL", "NVDA"}
    assert {candidate.universe_snapshot_id for candidate in host_candidates} == {
        REVIEWED_UNIVERSE_ID
    }


@pytest.mark.asyncio
async def test_five_candidate_upper_bound_persists_and_replays(
    migrated_database_url: str,
) -> None:
    await _seed_scope(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    ids = (AAPL_ID, SPY_ID, MSFT_ID, NVDA_ID, GOOG_ID)
    async with factory() as session, session.begin():
        created = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=ids,
            idempotency_key="five-candidate-boundary",
            created_at=NOW,
        )
        replay = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=ids,
            idempotency_key="five-candidate-boundary",
            created_at=NOW,
        )
    assert created == replay
    assert created.instrument_ids == ids


@pytest.mark.asyncio
async def test_trigger_freezes_batch_identity_and_never_replays_other_candidates(
    migrated_database_url,
):
    from nexus.paper.agent_cycle import (
        finish_schwab_agent_paper_trigger,
        reserve_schwab_agent_paper_trigger,
    )
    from nexus.paper.agent_cycle_models import AgentPaperCycleResult

    await _seed_scope(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        first = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(AAPL_ID,),
            idempotency_key="first",
            created_at=NOW,
        )
        second = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(SPY_ID,),
            idempotency_key="second",
            created_at=NOW,
        )
        reservation = await reserve_schwab_agent_paper_trigger(
            session,
            account_id=ACCOUNT_ID,
            candidate_batch_id=first.batch_id,
            client_idempotency_key="one-slot",
            now=NOW,
        )
        assert reservation.candidate_batch_hash == first.batch_hash
        assert reservation.candidate_batch_id == first.batch_id
        result = AgentPaperCycleResult(
            cycle_id=uuid4(),
            status="unavailable",
            result_code="RISK_INPUT_REQUIRED",
            replayed=False,
        )
        await finish_schwab_agent_paper_trigger(session, reservation=reservation, result=result)
    async with factory() as session, session.begin():
        replay = await reserve_schwab_agent_paper_trigger(
            session,
            account_id=ACCOUNT_ID,
            candidate_batch_id=first.batch_id,
            client_idempotency_key="one-slot",
            now=NOW,
        )
        assert replay.replay == result.model_copy(update={"replayed": True})
    with pytest.raises(ValueError, match="AGENT_PAPER_IDEMPOTENCY_CONFLICT"):
        async with factory() as session, session.begin():
            await reserve_schwab_agent_paper_trigger(
                session,
                account_id=ACCOUNT_ID,
                candidate_batch_id=second.batch_id,
                client_idempotency_key="one-slot",
                now=NOW,
            )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["missing", "unknown", "wrong_account", "wrong_universe", "future"]
)
async def test_trigger_candidate_scope_rejects_before_any_provider_or_job(
    migrated_database_url, case
):
    from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch

    await _seed_scope(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        batch = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(AAPL_ID, SPY_ID),
            idempotency_key="batch",
            created_at=NOW,
        )
    async with factory() as session:
        with pytest.raises(
            ValueError,
            match="CANDIDATE_BATCH_REQUIRED" if case == "missing" else "CANDIDATE_BATCH_INVALID",
        ):
            await validate_agent_paper_candidate_batch(
                session,
                batch_id=None
                if case == "missing"
                else uuid4()
                if case == "unknown"
                else batch.batch_id,
                account_id=uuid4() if case == "wrong_account" else ACCOUNT_ID,
                universe_snapshot_id=uuid4() if case == "wrong_universe" else UNIVERSE_ID,
                as_of=NOW.replace(hour=13) if case == "future" else NOW,
            )
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 0


async def _seed_scope(
    database_url: str,
    *,
    account_provider: str = "internal",
    account_status: str = "ACTIVE",
    aapl_currency: str = "USD",
    aapl_asset_type: str = "common_stock",
    aapl_alias: str = "current",
    aapl_valid_to: date | None = None,
    include_aapl: bool = True,
) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    created = NOW.replace(day=17)
    async with factory() as session, session.begin():
        session.add_all(
            [
                Account(
                    id=BROKER_ACCOUNT_ID,
                    provider=account_provider,
                    status=account_status,
                    created_at=created,
                ),
                UniverseSnapshot(
                    id=UNIVERSE_ID,
                    key="us_liquid",
                    version="us_liquid_v1",
                    effective_from=date(2026, 9, 1),
                    source="fosun_local_read_only",
                    content_hash="a" * 64,
                    published_at=None,
                    created_at=created,
                ),
                Instrument(
                    id=AAPL_ID,
                    asset_type=aapl_asset_type,
                    exchange="XNAS",
                    currency=aapl_currency,
                    tick_size=Decimal("0.01"),
                    risk_bucket="large_cap",
                    created_at=created,
                ),
                Instrument(
                    id=SPY_ID,
                    asset_type="ETF",
                    exchange="ARCX",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="broad_market",
                    created_at=created,
                ),
                Instrument(
                    id=OUTSIDE_ID,
                    asset_type="common_stock",
                    exchange="XNYS",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="large_cap",
                    created_at=created,
                ),
                *(
                    Instrument(
                        id=instrument_id,
                        asset_type="common_stock",
                        exchange="XNAS",
                        currency="USD",
                        tick_size=Decimal("0.01"),
                        risk_bucket="large_cap",
                        created_at=created,
                    )
                    for instrument_id in (MSFT_ID, NVDA_ID, GOOG_ID)
                ),
            ]
        )
        await session.flush()
        session.add(
            Portfolio(
                id=PORTFOLIO_ID,
                account_id=BROKER_ACCOUNT_ID,
                name="Agent Paper",
                created_at=created,
            )
        )
        await session.flush()
        session.add(
            PaperAccountRecord(
                id=ACCOUNT_ID,
                account_id=BROKER_ACCOUNT_ID,
                portfolio_id=PORTFOLIO_ID,
                code="agent-paper",
                currency="USD",
                account_model="cash",
                position_model="long_only",
                initial_capital=Decimal("100000"),
                capability="simulation_write_internal",
                created_at=created,
            )
        )
        await session.flush()
        session.add_all(
            [
                UniverseMember(
                    universe_snapshot_id=UNIVERSE_ID,
                    instrument_id=SPY_ID,
                    inclusion_reason="approved fixture",
                    created_at=created,
                ),
                InstrumentAlias(
                    instrument_id=SPY_ID,
                    symbol="SPY",
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                    created_at=created,
                ),
                InstrumentAlias(
                    instrument_id=OUTSIDE_ID,
                    symbol="OUT",
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                    created_at=created,
                ),
                *(
                    UniverseMember(
                        universe_snapshot_id=UNIVERSE_ID,
                        instrument_id=instrument_id,
                        inclusion_reason="approved fixture",
                        created_at=created,
                    )
                    for instrument_id in (MSFT_ID, NVDA_ID, GOOG_ID)
                ),
                *(
                    InstrumentAlias(
                        instrument_id=instrument_id,
                        symbol=symbol,
                        alias_type="ticker",
                        valid_from=date(2020, 1, 1),
                        valid_to=None,
                        created_at=created,
                    )
                    for instrument_id, symbol in (
                        (MSFT_ID, "MSFT"),
                        (NVDA_ID, "NVDA"),
                        (GOOG_ID, "GOOG"),
                    )
                ),
            ]
        )
        if include_aapl:
            session.add(
                UniverseMember(
                    universe_snapshot_id=UNIVERSE_ID,
                    instrument_id=AAPL_ID,
                    inclusion_reason="approved fixture",
                    created_at=created,
                )
            )
        if aapl_alias != "missing":
            session.add(
                InstrumentAlias(
                    instrument_id=AAPL_ID,
                    symbol="AAPL",
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=(date(2026, 9, 17) if aapl_alias == "expired" else aapl_valid_to),
                    created_at=created,
                )
            )
        await session.flush()
        await session.execute(
            text("UPDATE universe_snapshots SET published_at=:published WHERE id=:id"),
            {"published": created, "id": UNIVERSE_ID},
        )


@pytest.mark.asyncio
async def test_static_readiness_reports_each_missing_candidate_input_without_writing(
    migrated_database_url: str,
) -> None:
    await _seed_scope(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    calendar_id = uuid4()
    created = NOW.replace(day=17)
    async with factory() as session, session.begin():
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=calendar_id,
                exchange="XNYS",
                version="static-readiness-fixture",
                source="fixture",
                range_start=date(2026, 9, 17),
                range_end=date(2026, 9, 30),
                query_parameters={},
                completeness_status="complete",
                content_hash="b" * 64,
                published_at=created,
                created_at=created,
            )
        )
        for day in (17, 18, 21, 22):
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=calendar_id,
                    session=date(2026, 9, day),
                    market_open_at=datetime(2026, 9, day, 13, 30, tzinfo=UTC),
                    market_close_at=datetime(2026, 9, day, 20, tzinfo=UTC),
                    decision_at=datetime(2026, 9, day, 21, tzinfo=UTC),
                    created_at=created,
                )
            )
        session.add(SectorExposure(
            instrument_id=AAPL_ID,
            sector_code="information_technology",
            weight=Decimal(1),
            as_of=date(2026, 9, 17),
            source_url="https://example.test/fixture-sector",
            content_hash="d" * 64,
            synthetic=True,
            reviewed=True,
            created_at=created,
        ))
        batch = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(AAPL_ID, NVDA_ID),
            idempotency_key="static-readiness",
            created_at=NOW,
        )
    async with factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        result = await batch_module.inspect_agent_paper_candidate_static_readiness(
            session,
            account_id=ACCOUNT_ID,
            batch_id=batch.batch_id,
            as_of=NOW,
        )
        assert result["status"] == "blocked"
        assert result["decision_session"] == "2026-09-18"
        assert result["prior_session"] == "2026-09-17"
        assert result["candidates"] == [
            {
                "symbol": "AAPL",
                "missing": [
                    "RESEARCH_SNAPSHOT_MISSING",
                    "REVIEWED_SECTOR_MISSING",
                    "PRIOR_DAILY_AUTHORITY_MISSING",
                ],
            },
            {
                "symbol": "NVDA",
                "missing": [
                    "RESEARCH_SNAPSHOT_MISSING",
                    "REVIEWED_SECTOR_MISSING",
                    "PRIOR_DAILY_AUTHORITY_MISSING",
                ],
            },
        ]
        assert result["execution_locked"] is True
        assert result["external_requests"] == 0
        assert result["synthesis_prompt_status"] == "missing_or_invalid"
        assert await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 0

    async with factory() as session, session.begin():
        content = "frozen synthesis prompt"
        session.add(
            AgentPromptVersionRecord(
                role="portfolio_decision",
                version=SYNTHESIS_PROMPT_VERSION,
                content=content,
                content_hash=sha256(content.encode()).hexdigest(),
                created_at=created,
            )
        )
    async with factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        with_prompt = await batch_module.inspect_agent_paper_candidate_static_readiness(
            session, account_id=ACCOUNT_ID, batch_id=batch.batch_id, as_of=NOW
        )
        assert with_prompt["synthesis_prompt_status"] == "ready"
        assert with_prompt["status"] == "blocked"  # Candidate evidence is still missing.

    async with factory() as session, session.begin():
        session.add_all(
            (
                SectorExposure(
                    instrument_id=AAPL_ID,
                    sector_code="information_technology",
                    weight=Decimal(1),
                    as_of=date(2026, 9, 17),
                    source_url="https://example.test/older-valid-sector",
                    content_hash="f" * 64,
                    synthetic=False,
                    reviewed=True,
                    created_at=created,
                ),
                SectorExposure(
                    instrument_id=AAPL_ID,
                    sector_code="information_technology",
                    weight=Decimal("0.5"),
                    as_of=date(2026, 9, 18),
                    source_url="https://example.test/latest-incomplete-sector",
                    content_hash="e" * 64,
                    synthetic=False,
                    reviewed=True,
                    created_at=NOW,
                ),
            )
        )
    async with factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        incomplete = await batch_module.inspect_agent_paper_candidate_static_readiness(
            session, account_id=ACCOUNT_ID, batch_id=batch.batch_id, as_of=NOW
        )
        assert "REVIEWED_SECTOR_MISSING" in incomplete["candidates"][0]["missing"]

    async with factory() as session, session.begin():
        session.add(
            IdempotencyRecord(
                id=uuid4(),
                actor_id=f"local-agent-paper-schwab:{ACCOUNT_ID}",
                route="agent-paper-schwab-trigger-v2",
                idempotency_key="2026-09-18T14:00:00+00:00",
                request_hash="a" * 64,
                schema_version="agent_paper_schwab_trigger_v2",
                response_payload={
                    "outcome": "failed",
                    "error_code": "SCHWAB_L1_INCOMPLETE",
                },
                created_at=NOW,
            )
        )
    async with factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        claimed = await batch_module.inspect_agent_paper_candidate_static_readiness(
            session, account_id=ACCOUNT_ID, batch_id=batch.batch_id, as_of=NOW
        )
        assert claimed["scheduled_trigger"] == {
            "status": "failed",
            "error_code": "SCHWAB_L1_INCOMPLETE",
        }
        assert claimed["normal_cycle_start_status"] == "blocked_scheduled_claim"

    cycle_id = uuid4()
    async with factory() as session, session.begin():
        cycle = AgentPaperCycleRecord(
                id=cycle_id,
                account_id=ACCOUNT_ID,
                portfolio_id=PORTFOLIO_ID,
                decision_session=date(2026, 9, 18),
                decision_time=NOW,
                decision_slot_at=NOW,
                execution_session=date(2026, 9, 18),
                universe_snapshot_id=UNIVERSE_ID,
                quality_report_id=None,
                candidate_batch_id=batch.batch_id,
                decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                candidate_manifest=[],
                candidate_hash="a" * 64,
                sizing_policy_version="portfolio_allocation_v1",
                sizing_policy_hash="d7b719f47edbf3cb7ebb40dbe555a51f717ca581162a72d05c23391b8b083dd7",
                execution_locked=True,
                created_at=NOW,
        )
        session.add(cycle)
        await session.flush()
        await agent_cycle._append_event(
            session, cycle, None, "prepared", "CYCLE_PREPARED", {}
        )
    async with factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        occupied = await batch_module.inspect_agent_paper_candidate_static_readiness(
            session, account_id=ACCOUNT_ID, batch_id=batch.batch_id, as_of=NOW
        )
        assert occupied["current_session_cycles"] == [
            {
                "cycle_id": str(cycle_id),
                "status": "prepared",
                "result_code": "CYCLE_PREPARED",
            }
        ]
        assert occupied["normal_cycle_start_status"] == "blocked_existing_cycle"

    async with factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        after_close = await batch_module.inspect_agent_paper_candidate_static_readiness(
            session,
            account_id=ACCOUNT_ID,
            batch_id=batch.batch_id,
            as_of=NOW.replace(hour=20, minute=1),
        )
        assert after_close["current_session"] == "2026-09-18"
        assert after_close["decision_session"] == "2026-09-21"
        assert after_close["normal_cycle_start_status"] == "outside_normal_window"


@pytest.mark.asyncio
async def test_candidate_batch_persists_canonical_scope_and_replays_exact_result(
    migrated_database_url: str,
) -> None:
    await _seed_scope(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session, session.begin():
        created = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(SPY_ID, AAPL_ID),
            idempotency_key="candidate-scope-1",
            created_at=NOW,
        )
    async with factory() as session, session.begin():
        replay = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(AAPL_ID, SPY_ID),
            idempotency_key="candidate-scope-1",
            created_at=NOW,
        )
        loaded = await load_agent_paper_candidate_batch(session, batch_id=created.batch_id)
        counts = {
            "batch": await session.scalar(
                select(func.count()).select_from(AgentPaperCandidateBatchRecord)
            ),
            "claim": await session.scalar(select(func.count()).select_from(IdempotencyRecord)),
            "job": await session.scalar(select(func.count()).select_from(Job)),
            "model_run": await session.scalar(
                select(func.count()).select_from(AgentResearchRunRecord)
            ),
            "synthesis_run": await session.scalar(
                select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
            ),
            "cycle": await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)),
            "decision": await session.scalar(
                select(func.count()).select_from(StrategyDecisionRecord)
            ),
            "target": await session.scalar(
                select(func.count()).select_from(PaperTargetVersionRecord)
            ),
            "plan": await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord)),
            "order": await session.scalar(select(func.count()).select_from(PaperOrderRecord)),
        }

    assert replay == created
    assert loaded == created
    assert created.instrument_ids == (AAPL_ID, SPY_ID)
    assert created.source == "explicit_research_scope"
    assert created.created_at == NOW
    assert counts == {
        "batch": 1,
        "claim": 1,
        "job": 0,
        "model_run": 0,
        "synthesis_run": 0,
        "cycle": 0,
        "decision": 0,
        "target": 0,
        "plan": 0,
        "order": 0,
    }


@pytest.mark.asyncio
async def test_same_candidate_content_with_a_new_key_reuses_the_immutable_batch(
    migrated_database_url: str,
) -> None:
    await _seed_scope(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        first = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(AAPL_ID,),
            idempotency_key="first-key",
            created_at=NOW,
        )
    async with factory() as session, session.begin():
        second = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(AAPL_ID,),
            idempotency_key="second-key",
            created_at=NOW,
        )
        batch_count = await session.scalar(
            select(func.count()).select_from(AgentPaperCandidateBatchRecord)
        )
        claim_count = await session.scalar(select(func.count()).select_from(IdempotencyRecord))

    assert second == first
    assert batch_count == 1
    assert claim_count == 2


@pytest.mark.asyncio
async def test_candidate_batch_same_key_rejects_changed_scope(
    migrated_database_url: str,
) -> None:
    await _seed_scope(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(AAPL_ID,),
            idempotency_key="same-key",
            created_at=NOW,
        )
    with pytest.raises(ValueError, match="IDEMPOTENCY_CONFLICT"):
        async with factory() as session, session.begin():
            await create_agent_paper_candidate_batch(
                session,
                account_id=ACCOUNT_ID,
                universe_snapshot_id=UNIVERSE_ID,
                instrument_ids=(SPY_ID,),
                idempotency_key="same-key",
                created_at=NOW,
            )


@pytest.mark.asyncio
async def test_cli_replay_uses_persisted_creation_identity_after_alias_expiry(
    migrated_database_url: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_scope(migrated_database_url, aapl_valid_to=NOW.date())
    settings = EngineSettings(database_url=migrated_database_url)
    request = AgentPaperPrepareCandidatesRequest(
        action="prepare-candidates",
        account_id=ACCOUNT_ID,
        universe_snapshot_id=UNIVERSE_ID,
        instrument_ids=(AAPL_ID,),
        idempotency_key="cli-replay-identity",
    )

    monkeypatch.setattr(
        candidate_cli,
        "func",
        SimpleNamespace(clock_timestamp=lambda: literal(NOW)),
    )
    first = await candidate_cli.prepare_candidates_once(settings, request)
    assert json.loads(capsys.readouterr().err) == {
        "candidate_identities": [
            {
                "asset_type": "common_stock",
                "currency": "USD",
                "exchange": "XNAS",
                "instrument_id": str(AAPL_ID),
                "symbol": "AAPL",
            }
        ]
    }
    monkeypatch.setattr(
        candidate_cli,
        "func",
        SimpleNamespace(clock_timestamp=lambda: literal(NOW.replace(day=19))),
    )
    replay = await candidate_cli.prepare_candidates_once(settings, request)
    replay_display = json.loads(capsys.readouterr().err)
    factory = session_factory(settings)
    async with factory() as session:
        counts = {
            "batch": await session.scalar(
                select(func.count()).select_from(AgentPaperCandidateBatchRecord)
            ),
            "claim": await session.scalar(select(func.count()).select_from(IdempotencyRecord)),
            "job": await session.scalar(select(func.count()).select_from(Job)),
            "model_run": await session.scalar(
                select(func.count()).select_from(AgentResearchRunRecord)
            ),
            "target": await session.scalar(
                select(func.count()).select_from(PaperTargetVersionRecord)
            ),
            "plan": await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord)),
            "order": await session.scalar(select(func.count()).select_from(PaperOrderRecord)),
        }

    assert replay.batch == first.batch
    assert replay.identities == first.identities
    assert replay_display == {
        "candidate_identities": [
            {
                "asset_type": "common_stock",
                "currency": "USD",
                "exchange": "XNAS",
                "instrument_id": str(AAPL_ID),
                "symbol": "AAPL",
            }
        ]
    }
    assert counts == {
        "batch": 1,
        "claim": 1,
        "job": 0,
        "model_run": 0,
        "target": 0,
        "plan": 0,
        "order": 0,
    }


@pytest.mark.asyncio
async def test_cli_conflict_preflight_emits_no_changed_identity_preview(
    migrated_database_url: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    await _seed_scope(migrated_database_url)
    settings = EngineSettings(database_url=migrated_database_url)
    await candidate_cli.prepare_candidates_once(
        settings,
        AgentPaperPrepareCandidatesRequest(
            action="prepare-candidates",
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(AAPL_ID,),
            idempotency_key="cli-conflict",
        ),
    )
    capsys.readouterr()

    with pytest.raises(ValueError, match="IDEMPOTENCY_CONFLICT"):
        await candidate_cli.prepare_candidates_once(
            settings,
            AgentPaperPrepareCandidatesRequest(
                action="prepare-candidates",
                account_id=ACCOUNT_ID,
                universe_snapshot_id=UNIVERSE_ID,
                instrument_ids=(SPY_ID,),
                idempotency_key="cli-conflict",
            ),
        )

    assert capsys.readouterr().err == ""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "status"),
    [
        ("internal", "DISABLED"),
        ("paper", "ACTIVE"),
        ("internal_paper_evaluation", "ACTIVE"),
    ],
    ids=["disabled", "wrong-provider", "evaluation-account"],
)
async def test_new_candidate_batch_requires_active_normal_internal_account(
    migrated_database_url: str,
    provider: str,
    status: str,
) -> None:
    await _seed_scope(
        migrated_database_url,
        account_provider=provider,
        account_status=status,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    with pytest.raises(ValueError, match="CANDIDATE_BATCH_INVALID"):
        async with factory() as session, session.begin():
            await create_agent_paper_candidate_batch(
                session,
                account_id=ACCOUNT_ID,
                universe_snapshot_id=UNIVERSE_ID,
                instrument_ids=(AAPL_ID,),
                idempotency_key="invalid-account-authority",
                created_at=NOW,
            )


@pytest.mark.asyncio
async def test_persisted_candidate_batch_replays_after_underlying_account_is_disabled(
    migrated_database_url: str,
) -> None:
    await _seed_scope(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        created = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(AAPL_ID,),
            idempotency_key="replay-disabled-account",
            created_at=NOW,
        )
    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE accounts DISABLE TRIGGER trg_accounts_immutable"))
        await session.execute(
            text("UPDATE accounts SET status='DISABLED' WHERE id=:id"),
            {"id": BROKER_ACCOUNT_ID},
        )
        await session.execute(text("ALTER TABLE accounts ENABLE TRIGGER trg_accounts_immutable"))
    async with factory() as session, session.begin():
        replay = await create_agent_paper_candidate_batch(
            session,
            account_id=ACCOUNT_ID,
            universe_snapshot_id=UNIVERSE_ID,
            instrument_ids=(AAPL_ID,),
            idempotency_key="replay-disabled-account",
            created_at=NOW,
        )

    assert replay == created


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("seed", "instrument_id", "account_id"),
    [
        ({"aapl_currency": "EUR"}, AAPL_ID, ACCOUNT_ID),
        ({"aapl_asset_type": "crypto"}, AAPL_ID, ACCOUNT_ID),
        ({"aapl_alias": "missing"}, AAPL_ID, ACCOUNT_ID),
        ({"aapl_alias": "expired"}, AAPL_ID, ACCOUNT_ID),
        ({"include_aapl": False}, AAPL_ID, ACCOUNT_ID),
        ({}, OUTSIDE_ID, ACCOUNT_ID),
        ({}, AAPL_ID, UUID("30000000-0000-0000-0000-000000000001")),
    ],
    ids=[
        "non-usd",
        "unsupported-asset",
        "missing-alias",
        "expired-alias",
        "missing-membership",
        "outside-universe",
        "non-internal-account",
    ],
)
async def test_candidate_batch_rejects_non_authoritative_scope_without_side_effects(
    migrated_database_url: str,
    seed: dict[str, object],
    instrument_id: UUID,
    account_id: UUID,
) -> None:
    await _seed_scope(migrated_database_url, **seed)  # type: ignore[arg-type]
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    with pytest.raises(ValueError, match="CANDIDATE_BATCH_INVALID"):
        async with factory() as session, session.begin():
            await create_agent_paper_candidate_batch(
                session,
                account_id=account_id,
                universe_snapshot_id=UNIVERSE_ID,
                instrument_ids=(instrument_id,),
                idempotency_key=f"invalid-{uuid4()}",
                created_at=NOW,
            )

    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(AgentPaperCandidateBatchRecord))
            == 0
        )
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
