import json
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from nexus.ops import agent_paper_once as candidate_cli
from nexus.ops.agent_paper_once import AgentPaperPrepareCandidatesRequest
from nexus.paper.candidate_batch import (
    create_agent_paper_candidate_batch,
    load_agent_paper_candidate_batch,
)
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisRunRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import (
    AgentPaperCandidateBatchRecord,
    AgentPaperCycleRecord,
    PaperAccountRecord,
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.portfolio import Account, Portfolio
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.persistence.session import session_factory
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
                    valid_to=(
                        date(2026, 9, 17) if aapl_alias == "expired" else aapl_valid_to
                    ),
                    created_at=created,
                )
            )
        await session.flush()
        await session.execute(
            text("UPDATE universe_snapshots SET published_at=:published WHERE id=:id"),
            {"published": created, "id": UNIVERSE_ID},
        )


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
            "cycle": await session.scalar(
                select(func.count()).select_from(AgentPaperCycleRecord)
            ),
            "decision": await session.scalar(
                select(func.count()).select_from(StrategyDecisionRecord)
            ),
            "target": await session.scalar(
                select(func.count()).select_from(PaperTargetVersionRecord)
            ),
            "plan": await session.scalar(
                select(func.count()).select_from(PaperOrderBatchRecord)
            ),
            "order": await session.scalar(
                select(func.count()).select_from(PaperOrderRecord)
            ),
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
            "plan": await session.scalar(
                select(func.count()).select_from(PaperOrderBatchRecord)
            ),
            "order": await session.scalar(
                select(func.count()).select_from(PaperOrderRecord)
            ),
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
        await session.execute(
            text("ALTER TABLE accounts DISABLE TRIGGER trg_accounts_immutable")
        )
        await session.execute(
            text("UPDATE accounts SET status='DISABLED' WHERE id=:id"),
            {"id": BROKER_ACCOUNT_ID},
        )
        await session.execute(
            text("ALTER TABLE accounts ENABLE TRIGGER trg_accounts_immutable")
        )
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
            await session.scalar(
                select(func.count()).select_from(AgentPaperCandidateBatchRecord)
            )
            == 0
        )
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
