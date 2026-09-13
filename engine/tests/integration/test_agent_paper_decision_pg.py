from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import (
    DATA_SNAPSHOT_ID,
    INSTRUMENTS,
    PAPER_ACCOUNT_ID,
    seed_paper_batch,
)
from nexus.agents.actionable_research import ActionableResearchDecision
from nexus.market.ingestion import ingest_price_page
from nexus.paper.agent_decision import (
    AGENT_PAPER_SIZING_POLICY_HASH,
    build_agent_paper_decision,
)
from nexus.paper.agent_decision_store import (
    PersistedAgentPaperDecision,
    load_paper_decision,
    persist_agent_paper_decision,
)
from nexus.paper.projection import load_paper_projection
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    RiskQueryAuthorityRecord,
)
from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
from nexus.risk.authority import (
    RiskAuthorityInput,
    canonical_query_authority_hash,
    prepare_risk_market_snapshot,
)
from nexus.strategy.forward import load_forward_strategy_decision
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


async def _seed_output_rows(session: AsyncSession) -> dict[UUID, ActionableResearchDecision]:
    await session.execute(
        text(
            "INSERT INTO agent_prompt_versions (id,role,version,content_hash,content) VALUES "
            "('62000000-0000-0000-0000-000000000001','portfolio','agent-decision-v1',"
            "repeat('a',64),'test')"
        )
    )
    await session.execute(
        text(
            "INSERT INTO agent_model_configs "
            "(id,model_id,version,config_hash,max_input_tokens,max_tool_calls,max_output_tokens,"
            "input_usd_per_million,output_usd_per_million,pricing_available_at,pricing_expires_at) "
            "VALUES ('62000000-0000-0000-0000-000000000002','fixture','agent-decision-v1',"
            "repeat('b',64),1,0,1,0,0,'2026-01-01T00:00:00Z','2027-01-01T00:00:00Z')"
        )
    )
    decisions: dict[UUID, ActionableResearchDecision] = {}
    for index, (instrument_id, _, _, _) in enumerate(INSTRUMENTS, start=1):
        run_id = UUID(int=0x62000000000000000000000000000100 + index)
        output_id = UUID(int=0x62000000000000000000000000000200 + index)
        await session.execute(
            text(
                "INSERT INTO agent_research_runs "
                "(id,instrument_id,data_snapshot_id,prompt_version_id,model_config_id,"
                "decision_time,actor_id,idempotency_key,status,retry_count,schema_hash,"
                "model_hash,prompt_hash,prompt_manifest,evidence_hash,evidence_manifest,"
                "tool_hash,input_hash,reserved_cost_usd,settled_cost_usd,pricing_version,"
                "input_tokens,output_tokens) VALUES "
                "(:run_id,:instrument_id,:snapshot_id,"
                "'62000000-0000-0000-0000-000000000001',"
                "'62000000-0000-0000-0000-000000000002','2026-08-26T21:00:00Z',"
                "'agent-decision-test',:idempotency_key,'completed',0,repeat('a',64),"
                "repeat('b',64),repeat('c',64),'{}'::jsonb,repeat('d',64),"
                "jsonb_build_array(jsonb_build_object('fixture',true)),"
                "repeat('e',64),repeat('f',64),0,0,'codex-sdk-chatgpt-v4-actionable',0,0)"
            ),
            {
                "run_id": run_id,
                "instrument_id": instrument_id,
                "snapshot_id": DATA_SNAPSHOT_ID,
                "idempotency_key": f"agent-decision-{index}",
            },
        )
        await session.execute(
            text(
                "INSERT INTO agent_research_outputs "
                "(id,run_id,sequence,status,role,prompt_hash,content_hash,payload,intent) "
                "VALUES (:output_id,:run_id,1,'completed',NULL,NULL,repeat('a',64),"
                "'{}'::jsonb,'{}'::jsonb)"
            ),
            {"output_id": output_id, "run_id": run_id},
        )
        decisions[run_id] = ActionableResearchDecision(
            instrument_id=instrument_id,
            output_id=output_id,
            intent_hash=f"{index:064x}",
            stance="buy_candidate",
            sizing_action="increase",
            evidence_ref_ids=(UUID(int=0x62000000000000000000000000000300 + index),),
        )
    return decisions


class _ObjectStore:
    async def archive_raw(self, key: str, payload: bytes) -> str:
        return f"memory://{key}"

    async def put_normalized(self, key: str, payload: bytes) -> str:
        return f"memory://{key}"


async def _seed_adjusted_authority(session: AsyncSession, decision) -> None:
    available_at = decision.decision_time - timedelta(minutes=30)
    store = _ObjectStore()
    for index, (instrument_id, close, _, _) in enumerate(INSTRUMENTS, start=1):
        for adjustment in ("raw", "all"):
            page = ProviderPage(
                items=(
                    PriceBar(
                        instrument_id=instrument_id,
                        session=decision.decision_session,
                        open=close,
                        high=close,
                        low=close,
                        close=close,
                        volume=Decimal("1000000"),
                        observed_at=available_at,
                        available_at=available_at,
                        received_at=available_at,
                        revision=f"agent-{adjustment}-{index}",
                        quality_flags=(),
                    ),
                ),
                next_cursor=None,
                raw_payload=(f'{{"agent_price":{index},"adjustment":"{adjustment}"}}'.encode()),
                source_request_id=None,
                as_of=available_at,
                provenance=MarketDataProvenance(
                    source="alpaca",
                    feed="sip",
                    adjustment=adjustment,
                    interval="1Day",
                    mode="delayed",
                    delivery_mode="delayed",
                ),
            )
            result = await ingest_price_page(
                session,
                store,
                page,
                commit=False,
                source_policy_id=(PAPER_DECISION_SOURCE_POLICY_ID if adjustment == "all" else None),
            )
            assert result.price_authority is not None
    required = tuple(item[0] for item in INSTRUMENTS)
    for index, kind in enumerate(("corporate_action", "suspension", "delisting"), start=1):
        content_hash = canonical_query_authority_hash(
            source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
            universe_snapshot_id=decision.universe_snapshot_id,
            decision_session=decision.decision_session,
            kind=kind,
            required_instrument_ids=required,
            blocked_instrument_ids=(),
            complete=True,
            source_watermark_hash=f"{index + 6}" * 64,
            available_at=available_at,
        )
        session.add(
            RiskQueryAuthorityRecord(
                id=UUID(int=0x62000000000000000000000000000400 + index),
                source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
                universe_snapshot_id=decision.universe_snapshot_id,
                decision_session=decision.decision_session,
                kind=kind,
                required_instrument_ids=list(required),
                blocked_instrument_ids=[],
                complete=True,
                source_watermark_hash=f"{index + 6}" * 64,
                available_at=available_at,
                content_hash=content_hash,
            )
        )
    await session.flush()


@pytest.mark.asyncio
async def test_agent_decision_round_trip_uses_existing_risk_and_order_batch_path(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=seed.decision_time.date(),
            )
            assert registered is not None
            projection = await load_paper_projection(
                session,
                account_id=seed.account_id,
                as_of=seed.decision_time,
            )
            await _seed_adjusted_authority(session, registered.decision)
            paper_market = await prepare_risk_market_snapshot(
                session,
                authority=RiskAuthorityInput(
                    decision_session=registered.decision.decision_session,
                    execution_session=registered.decision.execution_session,
                    decision_time=registered.decision.decision_time,
                    source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
                    calendar_authority_id=registered.decision.calendar_authority_id,
                    universe_snapshot_id=registered.decision.universe_snapshot_id,
                    quality_report_id=None,
                ),
                required_instrument_ids=tuple(item[0] for item in INSTRUMENTS),
            )
            await session.execute(
                text("ALTER TABLE strategy_decision_targets DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE strategy_decisions DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text("DELETE FROM strategy_decision_targets WHERE strategy_decision_id=:id"),
                    {"id": seed.decision_id},
                )
                await session.execute(
                    text("DELETE FROM strategy_decisions WHERE id=:id"),
                    {"id": seed.decision_id},
                )
            finally:
                await session.execute(text("ALTER TABLE strategy_decisions ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE strategy_decision_targets ENABLE TRIGGER USER")
                )
            cycle = AgentPaperCycleRecord(
                id=UUID("62000000-0000-0000-0000-000000000010"),
                account_id=PAPER_ACCOUNT_ID,
                portfolio_id=seed.portfolio_id,
                decision_session=registered.decision.decision_session,
                decision_time=registered.decision.decision_time,
                execution_session=registered.decision.execution_session,
                universe_snapshot_id=registered.decision.universe_snapshot_id,
                quality_report_id=None,
                decision_price_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
                execution_price_policy_id=PAPER_EXECUTION_SOURCE_POLICY_ID,
                candidate_manifest=[],
                candidate_hash="a" * 64,
                sizing_policy_version="agent_paper_sizing_v1",
                sizing_policy_hash=AGENT_PAPER_SIZING_POLICY_HASH,
                execution_locked=True,
            )
            session.add(cycle)
            admitted_by_run = await _seed_output_rows(session)

            async def fake_load_actionable(
                _session: AsyncSession, *, run_id: UUID, decision_time: object
            ) -> ActionableResearchDecision:
                return admitted_by_run[run_id]

            monkeypatch.setattr(
                "nexus.paper.agent_decision_store.load_actionable_research_output",
                fake_load_actionable,
            )
            decision = build_agent_paper_decision(
                cycle=cycle,
                actions=tuple(admitted_by_run.values()),
                projection=projection,
                market=paper_market,
            )
            persisted = await persist_agent_paper_decision(session, decision)
            loaded = await load_paper_decision(
                session,
                account_id=seed.account_id,
                decision_id=persisted.decision_id,
            )
            assert isinstance(loaded, PersistedAgentPaperDecision)
            assert loaded.decision == decision

            result = await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=persisted.decision_id,
                idempotency_key="agent-paper-decision-v1",
                request_time=seed.request_time + timedelta(seconds=1),
            )
            assert result.decision_hash == decision.decision_hash
            assert len(result.evaluations) == 3
            assert {item.outcome for item in result.evaluations} == {"approved"}
            assert all(item.approved_quantity > Decimal(0) for item in result.evaluations)
    finally:
        await engine.dispose()
