import asyncio
import json
import subprocess
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import httpx
import nexus.ops.agent_paper_loop as agent_paper_loop
import nexus.ops.agent_paper_once as agent_paper_once
import nexus.paper.agent_cycle_authority as agent_cycle_authority
import nexus.paper.agent_version_evaluation as agent_version_evaluation
import pytest
from alembic import command
from alembic.config import Config
from helpers.paper_api import app, headers
from helpers.paper_batch import CALENDAR_ID, DECISION_TIME, INSTRUMENTS, seed_paper_batch
from helpers.paper_performance import seed_paper_performance, seed_second_paper_close
from nexus.agents.actionable_research import ActionableResearchDecision
from nexus.agents.codex_sdk import MODEL_ID
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSION, aggregate_input_limit
from nexus.agents.committee import load_prompt_assets
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.execution_authority import ROLE_ORDER, load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.paper_context import (
    FrozenAgentPaperContextStatus,
    revalidate_frozen_agent_paper_context,
)
from nexus.agents.persistence import admit_research_run, select_authoritative_snapshot
from nexus.broker_simulation.intents import load_agent_paper_intent_source
from nexus.domain.evidence import EvidenceRef
from nexus.jobs.daily_research import admit_agent_paper_research
from nexus.market.ingestion import ingest_price_page
from nexus.market.intraday_ingestion import canonical_quote_observation
from nexus.ops.agent_paper_loop import inspect_agent_paper_loop
from nexus.paper.agent_cycle import (
    advance_agent_paper_cycle,
    reserve_schwab_agent_paper_trigger,
    resume_schwab_agent_paper_cycle,
    start_schwab_agent_paper_cycle,
)
from nexus.paper.agent_cycle_authority import (
    load_agent_paper_service_calendar_authority,
    load_quasi_realtime_cycle_authority,
    prepare_candidates,
)
from nexus.paper.agent_cycle_models import (
    LegacyCandidateManifestEntry,
    candidate_manifest_hash,
    floor_decision_slot,
)
from nexus.paper.agent_market import (
    PaperMarketError,
    collect_agent_paper_window,
    resolve_agent_paper_market_scope,
)
from nexus.paper.agent_version_evaluation import register_agent_version_evaluation
from nexus.paper.agent_version_models import (
    AgentVersionEvaluationRequest,
    FrozenAgentVersionSelection,
)
from nexus.paper.agent_version_runner import (
    advance_agent_version_slot,
    resume_agent_version_slot,
)
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.paper.performance import publish_paper_performance_snapshot
from nexus.paper.read_service import PaperReadService
from nexus.persistence.models.agent_versions import AgentVersionCyclePairRecord
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, LatestQuote
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    AgentPaperMarketWindowRecord,
    PaperExecutionWindowAuthorityRecord,
    PaperExecutionWindowMemberRecord,
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderRecord,
    PaperPositionAdjustmentEventRecord,
    PaperReservationEventRecord,
    RiskNoChangeEvaluationRecord,
    RiskQueryAuthorityRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.persistence.models.runtime import IdempotencyRecord, Job, WorkerHeartbeat
from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
from nexus.risk.authority import (
    canonical_query_authority_hash,
    prepare_schwab_realtime_risk_snapshot,
)
from nexus.settings import EngineSettings
from nexus.strategy.intents import (
    ResearchClaim,
    ResearchCondition,
    ResearchIntent,
    RoleVote,
    SynthesisAssessment,
    SynthesisClaimReview,
)
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, literal, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ENGINE_ROOT = Path(__file__).parents[2]


async def _wait_for_advisory_wait(
    sessions: async_sessionmaker[AsyncSession], backend_pid: int
) -> None:
    async with asyncio.timeout(2):
        while True:
            async with sessions() as observer:
                wait_event = await observer.scalar(
                    text("SELECT wait_event FROM pg_stat_activity WHERE pid=:backend_pid"),
                    {"backend_pid": backend_pid},
                )
            if wait_event == "advisory":
                return
            await asyncio.sleep(0)


def test_service_status_reads_one_heartbeat_without_producers_or_business_writes(
    migrated_database_url,
    monkeypatch,
    tmp_path,
    capsys,
):
    from sqlalchemy import event

    worker_id = f"agent-paper-loop-{uuid4()}"

    async def seed() -> datetime:
        engine = create_async_engine(migrated_database_url)
        try:
            async with async_sessionmaker(engine).begin() as session:
                seen_at = await session.scalar(select(func.clock_timestamp()))
                assert isinstance(seen_at, datetime)
                session.add(
                    WorkerHeartbeat(
                        queue="agent_paper",
                        worker_id=worker_id,
                        process_version=agent_paper_loop.AGENT_PAPER_LOOP_PROCESS_VERSION,
                        migration_head=agent_paper_loop.CURRENT_MIGRATION_HEAD,
                        started_at=seen_at - timedelta(minutes=5),
                        seen_at=seen_at,
                    )
                )
                return seen_at
        finally:
            await engine.dispose()

    seen_at = asyncio.run(seed())
    (tmp_path / "infra").mkdir()
    (tmp_path / "infra/compose.yaml").touch()
    (tmp_path / ".gitignore").write_text("infra/.env.service\n")
    env_file = tmp_path / "infra/.env.service"
    env_file.write_text(f"DATABASE_URL={migrated_database_url}\n")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True, capture_output=True)
    statements = []

    def observed_engine(*args, **kwargs):
        engine = create_async_engine(*args, **kwargs)

        @event.listens_for(engine.sync_engine, "before_cursor_execute")
        def observe(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        return engine

    def unexpected(*args, **kwargs):
        pytest.fail("service status must not run producers or heartbeat writes")

    for name in (
        "run_local_agent_paper_loop",
        "heartbeat_agent_paper_loop",
        "trigger_once",
        "collect_window_once",
        "run_agent_worker_once",
        "sdk_status",
    ):
        monkeypatch.setattr(agent_paper_loop, name, unexpected)
    monkeypatch.setattr(agent_paper_loop, "create_async_engine", observed_engine)
    assert (
        agent_paper_loop.main(
            [
                "--repo-root",
                str(tmp_path),
                "--env-file",
                str(env_file),
                "--service-status",
            ]
        )
        == 1
    )  # Heartbeat exists, while configuration preflight independently remains blocked.
    output = capsys.readouterr().out
    result = json.loads(output)
    assert result["heartbeat"] == {
        "status": "fresh",
        "seen_at": seen_at.isoformat(),
        "started_at": (seen_at - timedelta(minutes=5)).isoformat(),
        "process_version": agent_paper_loop.AGENT_PAPER_LOOP_PROCESS_VERSION,
        "migration_head": agent_paper_loop.CURRENT_MIGRATION_HEAD,
    }
    assert result["preflight"]["status"] == "blocked"
    assert result["preflight"]["external_requests"] == 0
    assert result["execution_locked"] is True
    assert worker_id not in output
    selects = [sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]
    assert len(selects) == 1 and "worker_heartbeats" in selects[0]
    assert not any(
        sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for sql in statements
    )


@pytest.mark.parametrize(
    ("worker_id", "process_version", "seen_offset", "expected"),
    [
        (
            f"agent-paper-loop-{uuid4()}",
            agent_paper_loop.AGENT_PAPER_LOOP_PROCESS_VERSION,
            timedelta(seconds=-31),
            "stale",
        ),
        (
            f"agent-paper-loop-{uuid4()}",
            "old-agent-paper-loop",
            timedelta(0),
            "incompatible",
        ),
        (
            "malformed-worker",
            agent_paper_loop.AGENT_PAPER_LOOP_PROCESS_VERSION,
            timedelta(0),
            "unavailable",
        ),
        (
            f"agent-paper-loop-{uuid4()}",
            agent_paper_loop.AGENT_PAPER_LOOP_PROCESS_VERSION,
            timedelta(seconds=1),
            "unavailable",
        ),
    ],
)
def test_service_heartbeat_rejects_stale_incompatible_or_malformed_liveness(
    migrated_database_url,
    worker_id,
    process_version,
    seen_offset,
    expected,
):
    async def scenario() -> dict[str, object]:
        engine = create_async_engine(migrated_database_url)
        try:
            async with async_sessionmaker(engine).begin() as session:
                now = await session.scalar(select(func.clock_timestamp()))
                assert isinstance(now, datetime)
                session.add(
                    WorkerHeartbeat(
                        queue="agent_paper",
                        worker_id=worker_id,
                        process_version=process_version,
                        migration_head=agent_paper_loop.CURRENT_MIGRATION_HEAD,
                        started_at=now - timedelta(minutes=5),
                        seen_at=now + seen_offset,
                    )
                )
            return await agent_paper_loop.read_agent_paper_service_heartbeat(migrated_database_url)
        finally:
            await engine.dispose()

    assert asyncio.run(scenario()) == {"status": expected}


class _ObjectStore:
    async def archive_raw(self, key: str, payload: bytes) -> str:
        return f"memory://{key}"

    async def put_normalized(self, key: str, payload: bytes) -> str:
        return f"memory://{key}"


class _IexProvider:
    calls = 0

    async def fetch_bars(self, request, cursor=None):  # type: ignore[no-untyped-def]
        self.calls += 1
        assert cursor is None
        evaluated_at = request.end + timedelta(seconds=2)
        return ProviderPage(
            items=tuple(
                PriceBar(
                    instrument_id=request.instrument_id,
                    session=request.start.date(),
                    open=Decimal("100"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100.5"),
                    volume=Decimal("20"),
                    observed_at=request.start + timedelta(minutes=index),
                    available_at=evaluated_at - timedelta(seconds=1),
                    received_at=evaluated_at - timedelta(seconds=1),
                )
                for index in range(5)
            ),
            next_cursor=None,
            raw_payload=(f'{{"bars":"{request.start.isoformat()}"}}'.encode()),
            source_request_id="agent-cycle-iex-window",
            as_of=evaluated_at - timedelta(seconds=1),
            provenance=MarketDataProvenance(
                source="alpaca",
                feed="iex",
                adjustment="raw",
                interval="1Min",
                mode="read_only",
                delivery_mode="real_time",
            ),
        )


class _SchwabPriceHistoryProvider(_IexProvider):
    async def fetch_bars(self, request, cursor=None):  # type: ignore[no-untyped-def]
        page = await super().fetch_bars(request, cursor)
        return page.model_copy(
            update={
                "items": tuple(
                    bar.model_copy(update={"volume": Decimal("2000")}) for bar in page.items
                ),
                "raw_payload": b'{"candles":"agent-cycle-schwab-window"}',
                "source_request_id": None,
                "provenance": page.provenance.model_copy(
                    update={
                        "source": "schwab",
                        "feed": "price_history",
                        "delivery_mode": "unknown",
                    }
                ),
            }
        )


async def _seed_adjusted_authority(
    session: AsyncSession,
    decision: object,
    *,
    source_policy_id: UUID = PAPER_DECISION_SOURCE_POLICY_ID,
    available_at: datetime | None = None,
) -> None:
    available_at = available_at or decision.decision_time - timedelta(minutes=30)
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
    prior_page = ProviderPage(
        items=(
            PriceBar(
                instrument_id=INSTRUMENTS[0][0],
                session=decision.decision_session - timedelta(days=1),
                open=INSTRUMENTS[0][1],
                high=INSTRUMENTS[0][1],
                low=INSTRUMENTS[0][1],
                close=INSTRUMENTS[0][1],
                volume=Decimal("1000000"),
                observed_at=available_at - timedelta(days=1),
                available_at=available_at,
                received_at=available_at,
                revision="agent-prior-all-aapl",
                quality_flags=(),
            ),
        ),
        next_cursor=None,
        raw_payload=b'{"agent_price":"prior","adjustment":"all"}',
        source_request_id=None,
        as_of=available_at,
        provenance=MarketDataProvenance(
            source="alpaca",
            feed="sip",
            adjustment="all",
            interval="1Day",
            mode="delayed",
            delivery_mode="delayed",
        ),
    )
    prior_result = await ingest_price_page(
        session,
        store,
        prior_page,
        commit=False,
        source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
    )
    assert prior_result.price_authority is not None
    required = (INSTRUMENTS[0][0],)
    for index, kind in enumerate(("corporate_action", "suspension", "delisting"), start=1):
        content_hash = canonical_query_authority_hash(
            source_policy_id=source_policy_id,
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
                id=UUID(int=0x63000000000000000000000000000500 + index),
                source_policy_id=source_policy_id,
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


async def _seed_cycle_research_authority(
    session: AsyncSession,
    *,
    registered_decision: object,
    source_policy_id: UUID = PAPER_DECISION_SOURCE_POLICY_ID,
    quote_received_at: datetime = DECISION_TIME,
    price_available_at: datetime | None = None,
    base_payload: dict[str, object] | None = None,
) -> None:
    authority_time = price_available_at or DECISION_TIME
    await session.execute(text("ALTER TABLE instrument_aliases DISABLE TRIGGER USER"))
    try:
        await session.execute(
            text("UPDATE instrument_aliases SET symbol='AAPL' WHERE instrument_id=:instrument_id"),
            {"instrument_id": INSTRUMENTS[0][0]},
        )
    finally:
        await session.execute(text("ALTER TABLE instrument_aliases ENABLE TRIGGER USER"))
    await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
    try:
        await session.execute(
            text(
                "UPDATE universe_snapshots SET key='us_liquid',version='us_liquid_v1',"
                "source='fosun_local_read_only'"
            )
        )
    finally:
        await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
    # Agent admission validates its exact roots directly. A legacy QualityReport
    # must not be an otherwise unreachable prerequisite for this path.
    await session.execute(text("ALTER TABLE quality_reports DISABLE TRIGGER USER"))
    try:
        await session.execute(
            text(
                "UPDATE quality_reports SET payload=jsonb_set("
                "payload,'{decision_session}','\"2026-01-01\"'::jsonb)"
            )
        )
    finally:
        await session.execute(text("ALTER TABLE quality_reports ENABLE TRIGGER USER"))
    prompts = load_prompt_assets()
    calendar_authority_id = await session.scalar(
        select(ExchangeCalendarSessionRecord.calendar_authority_id).limit(1)
    )
    assert calendar_authority_id is not None
    session.add(
        ExchangeCalendarSessionRecord(
            calendar_authority_id=calendar_authority_id,
            session=authority_time.date() - timedelta(days=1),
            decision_at=DECISION_TIME - timedelta(days=1),
        )
    )
    session.add_all(
        AgentPromptVersionRecord(
            id=UUID(int=0x63000000000000000000000000000100 + index),
            role=role,
            version=f"agent-paper-{role}-v1",
            content_hash=prompts[role].content_hash,
            content=prompts[role].content,
        )
        for index, role in enumerate(ROLE_ORDER)
    )
    session.add(
        AgentModelConfigRecord(
            id=UUID("63000000-0000-0000-0000-000000000001"),
            model_id="fixture-agent-paper",
            version="codex-sdk-chatgpt-v4-actionable",
            config_hash="6" * 64,
            max_input_tokens=100,
            max_tool_calls=1,
            max_output_tokens=100,
            input_usd_per_million=Decimal("1"),
            output_usd_per_million=Decimal("1"),
            pricing_available_at=authority_time - timedelta(days=1),
            pricing_expires_at=authority_time + timedelta(days=1),
        )
    )
    await session.execute(text("ALTER TABLE instrument_aliases DISABLE TRIGGER USER"))
    try:
        for index, (instrument_id, _, _, _) in enumerate(INSTRUMENTS, start=1):
            await session.execute(
                text(
                    "UPDATE instrument_aliases SET symbol=:symbol "
                    "WHERE instrument_id=:instrument_id AND alias_type='ticker'"
                ),
                {
                    "instrument_id": instrument_id,
                    "symbol": "AAPL" if index == 1 else f"T{index}",
                },
            )
    finally:
        await session.execute(text("ALTER TABLE instrument_aliases ENABLE TRIGGER USER"))
    for index, (instrument_id, _, _, _) in enumerate(INSTRUMENTS, start=1):
        evidence = ResearchEvidenceRef(
            id=UUID(int=0x63000000000000000000000000000200 + index),
            source="sec",
            provider_object_id=f"agent-paper:{instrument_id}",
            revision="v1",
            available_at=authority_time - timedelta(hours=2),
            location=f"memory://sha256/{f'{index + 6}' * 64}",
            content_hash=f"{index + 6}" * 64,
            byte_length=10,
        )
        snapshot = DataSnapshot(
            id=UUID(int=0x63000000000000000000000000000300 + index),
            source="nexus_research",
            provider_object_id=str(instrument_id),
            revision="agent-paper-v1",
            observed_at=authority_time - timedelta(hours=2),
            available_at=authority_time - timedelta(hours=2),
            received_at=authority_time - timedelta(hours=2),
            schema_version="research_filings_v1",
            payload={"scope": "persisted_research_observations", **(base_payload or {})},
        )
        session.add_all(
            (
                evidence,
                snapshot,
            )
        )
        await session.flush()
        session.add(
            Filing(
                instrument_id=instrument_id,
                evidence_ref_id=evidence.id,
                source="sec",
                provider_object_id=evidence.provider_object_id,
                revision=evidence.revision,
                accession_number=f"000000000{index}-26-000001",
                form="10-Q",
                filed_at=authority_time - timedelta(days=1),
                accepted_at=authority_time - timedelta(hours=2),
                created_at=authority_time - timedelta(hours=2),
            )
        )
        await session.flush()
        session.add(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=snapshot.id,
                instrument_id=instrument_id,
                evidence_ref_id=evidence.id,
                authority_kind="research",
                selected_at=None,
                source=evidence.source,
                provider_object_id=evidence.provider_object_id,
                revision=evidence.revision,
                available_at=evidence.available_at,
                location=evidence.location,
                content_hash=evidence.content_hash,
            )
        )
    await session.flush()

    await _seed_adjusted_authority(
        session,
        registered_decision,
        source_policy_id=source_policy_id,
        available_at=price_available_at,
    )
    artifact = DataArtifact(
        id=UUID("63000000-0000-0000-0000-000000000090"),
        uri=f"memory://raw/market/schwab/stream/{'e' * 64}.json",
        sha256="e" * 64,
        byte_size=100,
        mime_type="application/json",
        schema_version="schwab_level_one_stream_v1",
        source="schwab",
        observed_at=quote_received_at - timedelta(seconds=10),
        available_at=quote_received_at,
        received_at=quote_received_at,
        created_at=quote_received_at,
    )
    quote_hash = canonical_quote_observation(
        instrument_id=INSTRUMENTS[0][0],
        source="schwab",
        feed="l1",
        delivery_mode="real_time",
        event_kind="quote",
        bid=Decimal("99.90"),
        ask=Decimal("100.10"),
        last=None,
        observed_at=quote_received_at - timedelta(seconds=10),
        available_at=quote_received_at,
        received_at=quote_received_at,
        revision="e" * 64,
    )
    session.add_all(
        (
            artifact,
            LatestQuote(
                id=UUID("63000000-0000-0000-0000-000000000091"),
                instrument_id=INSTRUMENTS[0][0],
                source="schwab",
                feed="l1",
                delivery_mode="real_time",
                bid=Decimal("99.90"),
                ask=Decimal("100.10"),
                last=None,
                observed_at=quote_received_at - timedelta(seconds=10),
                available_at=quote_received_at,
                received_at=quote_received_at,
                revision="e" * 64,
                quality_flags=[],
                event_kind="quote",
                content_hash=sha256(quote_hash).hexdigest(),
                evidence_status="verified",
                data_artifact_id=artifact.id,
                created_at=quote_received_at,
            ),
        )
    )
    await session.flush()


def _safe_no_trade_intent(
    *,
    run: AgentResearchRunRecord,
    evidence: ResearchEvidenceRef,
    valid_until: datetime,
) -> ResearchIntent:
    reference = EvidenceRef(
        source=evidence.source,
        object_id=evidence.provider_object_id,
        available_at=evidence.available_at,
        location=evidence.location,
        content_hash=evidence.content_hash,
    )
    claim = ResearchClaim(text="现有证据不足以支持增加风险暴露。", evidence_refs=(reference,))
    condition = ResearchCondition(
        signal="committee.evidence_valid",
        operator="is_true",
        threshold=True,
        unit="boolean",
        evidence_refs=(reference,),
    )
    return ResearchIntent(
        synthesis_assessment=SynthesisAssessment(
            stance="watch",
            claims=(claim,),
            reviews=(
                SynthesisClaimReview(
                    claim_id="bear:0",
                    status="unresolved",
                    rationale="缺少足够证据，保持观察且不交易。",
                    evidence_refs=(reference,),
                    resolved=False,
                    missing_evidence=("additional market evidence",),
                    prevented_conclusion="Only watch and no trade are permitted.",
                ),
                SynthesisClaimReview(
                    claim_id="evidence_audit:0",
                    status="accepted",
                    rationale="已核对现有冻结证据。",
                    evidence_refs=(reference,),
                    resolved=True,
                    missing_evidence=(),
                    prevented_conclusion=None,
                ),
            ),
            review_conclusion="limited",
            uncertainties=("方向证据仍不完整。",),
        ),
        schema_version="research_intent_v1",
        decision_id=UUID("63000000-0000-0000-0000-000000000701"),
        instrument_id=run.instrument_id,
        display_symbol="AAPL",
        decision_time=run.decision_time,
        data_snapshot_id=run.data_snapshot_id,
        horizon="medium_term",
        stance="watch",
        exposure_action="none",
        theses=(claim,),
        counter_theses=(
            ResearchClaim(text="新增证据可能改变观察结论。", evidence_refs=(reference,)),
        ),
        uncertainties=("方向证据仍不完整。",),
        invalidation_conditions=(condition,),
        entry_conditions=(condition,),
        exit_conditions=(condition,),
        valid_until=valid_until,
        quality_status="partial",
        evidence_audit="partial",
        role_votes=tuple(RoleVote(role=role, vote="abstain") for role in ROLE_ORDER),
        disagreement="证据不足，因此不改变风险暴露。",
        strategy_version="baseline_v1",
        model_version="fixture-agent-paper",
        prompt_version="agent-paper-v1",
        data_version="research-filings-v1",
        evidence_refs=(reference,),
    )


@pytest.mark.asyncio
async def test_agent_paper_research_keeps_frozen_champion_when_newer_version_exists(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(session, registered_decision=registered.decision)

        async with sessions.begin() as session:
            version_source = await admit_agent_paper_research(
                session,
                cycle_id=uuid4(),
                instrument_id=INSTRUMENTS[0][0],
                research_snapshot_id=UUID(int=0x63000000000000000000000000000301),
                research_as_of=DECISION_TIME,
                daily_limit_usd=Decimal("1"),
                now=DECISION_TIME,
            )

        evaluation_id = uuid4()
        async with sessions.begin() as session:
            started = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-frozen-champion",
                version_evaluation_id=evaluation_id,
                version_lane="incumbent",
                version_source_run_id=version_source.run_id,
            )
            first_run = await session.get(AgentResearchRunRecord, started.research_run_ids[0])
            assert first_run is not None
            assert first_run.actor_id == "local-agent-version-incumbent"
            assert first_run.prompt_hash == version_source.run.prompt_hash
            assert first_run.model_hash == version_source.run.model_hash
            first_manifest = dict(first_run.prompt_manifest)
            first_model_config_id = first_run.model_config_id
            first_model_hash = first_run.model_hash
            newer_content = "newer portfolio prompt must not replace the paper champion"
            session.add_all(
                (
                    AgentPromptVersionRecord(
                        id=UUID("f3000000-0000-0000-0000-000000000001"),
                        role="portfolio",
                        version="agent-paper-portfolio-v2",
                        content_hash=sha256(newer_content.encode()).hexdigest(),
                        content=newer_content,
                    ),
                    AgentModelConfigRecord(
                        id=UUID("f3000000-0000-0000-0000-000000000002"),
                        model_id="newer-fixture-agent-paper",
                        version="codex-sdk-chatgpt-v4-actionable",
                        config_hash="f" * 64,
                        max_input_tokens=100,
                        max_tool_calls=1,
                        max_output_tokens=100,
                        input_usd_per_million=Decimal("1"),
                        output_usd_per_million=Decimal("1"),
                        pricing_available_at=first_run.decision_time - timedelta(days=1),
                        pricing_expires_at=first_run.decision_time + timedelta(days=1),
                    ),
                )
            )
            await session.flush()
            second = await admit_agent_paper_research(
                session,
                cycle_id=uuid4(),
                instrument_id=first_run.instrument_id,
                research_snapshot_id=first_run.data_snapshot_id,
                research_as_of=first_run.decision_time,
                daily_limit_usd=Decimal("1"),
                now=first_run.decision_time,
            )

        assert second.run.prompt_manifest == first_manifest
        assert second.run.model_config_id == first_model_config_id
        assert second.replayed is False
        assert second.run_id != first_run.id

        async def selected_version(
            session: AsyncSession, *, now: datetime
        ) -> FrozenAgentVersionSelection:
            del session, now
            return FrozenAgentVersionSelection(
                source_run_id=first_run.id,
                prompt_hash=first_run.prompt_hash,
                model_hash=first_run.model_hash,
            )

        with monkeypatch.context() as selected_patch:
            selected_patch.setattr(
                agent_version_evaluation,
                "load_selected_internal_agent_version",
                selected_version,
            )
            async with sessions.begin() as session:
                selected_admission = await admit_agent_paper_research(
                    session,
                    cycle_id=uuid4(),
                    instrument_id=first_run.instrument_id,
                    research_snapshot_id=first_run.data_snapshot_id,
                    research_as_of=first_run.decision_time,
                    daily_limit_usd=Decimal("1"),
                    now=first_run.decision_time,
                )

        assert selected_admission.run.prompt_hash == first_run.prompt_hash
        assert selected_admission.run.model_hash == first_run.model_hash
        assert selected_admission.run.model_config_id == first_run.model_config_id

        pinned_cycle_id = uuid4()
        pinned_key = f"agent-version:test:incumbent:{first_run.decision_time.isoformat()}"
        async with sessions.begin() as session:
            pinned = await admit_agent_paper_research(
                session,
                cycle_id=pinned_cycle_id,
                instrument_id=first_run.instrument_id,
                research_snapshot_id=first_run.data_snapshot_id,
                research_as_of=first_run.decision_time,
                daily_limit_usd=Decimal("1"),
                now=first_run.decision_time,
                actor_id="local-agent-version-incumbent",
                admission_idempotency_key=pinned_key,
                version_source_run_id=first_run.id,
            )
            pinned_job = await session.scalar(
                select(Job).where(Job.payload["run_id"].as_string() == str(pinned.run_id))
            )
        assert pinned.run.actor_id == "local-agent-version-incumbent"
        assert pinned.run.prompt_hash == first_run.prompt_hash
        assert pinned.run.model_hash == first_run.model_hash
        assert pinned_job is not None
        assert pinned_job.job_type == "agent.paper.research"

        async with sessions.begin() as session:
            pinned_replay = await admit_agent_paper_research(
                session,
                cycle_id=pinned_cycle_id,
                instrument_id=first_run.instrument_id,
                research_snapshot_id=first_run.data_snapshot_id,
                research_as_of=first_run.decision_time,
                daily_limit_usd=Decimal("1"),
                now=first_run.decision_time,
                actor_id="local-agent-version-incumbent",
                admission_idempotency_key=pinned_key,
                version_source_run_id=first_run.id,
            )
        assert pinned_replay.run_id == pinned.run_id
        assert pinned_replay.replayed is True

        async with sessions.begin() as session:
            versioned_replay = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-frozen-champion",
                version_evaluation_id=evaluation_id,
                version_lane="incumbent",
                version_source_run_id=version_source.run_id,
            )
        assert versioned_replay.cycle_id == started.cycle_id
        assert versioned_replay.research_run_ids == started.research_run_ids
        assert versioned_replay.replayed is True

        backend_pid = asyncio.get_running_loop().create_future()

        async def admit_while_locked():
            async with sessions.begin() as session:
                backend_pid.set_result(int(await session.scalar(text("SELECT pg_backend_pid()"))))
                return await admit_agent_paper_research(
                    session,
                    cycle_id=uuid4(),
                    instrument_id=first_run.instrument_id,
                    research_snapshot_id=first_run.data_snapshot_id,
                    research_as_of=first_run.decision_time,
                    daily_limit_usd=Decimal("1"),
                    now=first_run.decision_time,
                )

        async with sessions.begin() as lock_session:
            await lock_session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1415))"),
                {"scope": "agent-version:local-agent-paper-cycle"},
            )
            locked_admission = asyncio.create_task(admit_while_locked())
            await _wait_for_advisory_wait(sessions, await backend_pid)
        third = await locked_admission
        assert third.run.prompt_manifest == first_manifest
        assert third.run.model_config_id == first_model_config_id

        async with sessions.begin() as session:
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text("UPDATE agent_research_runs SET model_hash=:model_hash WHERE id=:run_id"),
                    {"model_hash": "e" * 64, "run_id": third.run_id},
                )
            finally:
                await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
        with pytest.raises(ValueError, match="point-in-time data authority is invalid"):
            async with sessions.begin() as session:
                await admit_agent_paper_research(
                    session,
                    cycle_id=uuid4(),
                    instrument_id=first_run.instrument_id,
                    research_snapshot_id=first_run.data_snapshot_id,
                    research_as_of=first_run.decision_time,
                    daily_limit_usd=Decimal("1"),
                    now=first_run.decision_time,
                )
        async with sessions.begin() as session:
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE agent_research_runs SET model_hash=:model_hash,"
                        "prompt_version_id=:prompt_version_id WHERE id=:run_id"
                    ),
                    {
                        "model_hash": first_model_hash,
                        "prompt_version_id": UUID("f3000000-0000-0000-0000-000000000001"),
                        "run_id": third.run_id,
                    },
                )
            finally:
                await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
        with pytest.raises(ValueError, match="point-in-time data authority is invalid"):
            async with sessions.begin() as session:
                await admit_agent_paper_research(
                    session,
                    cycle_id=uuid4(),
                    instrument_id=first_run.instrument_id,
                    research_snapshot_id=first_run.data_snapshot_id,
                    research_as_of=first_run.decision_time,
                    daily_limit_usd=Decimal("1"),
                    now=first_run.decision_time,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_safe_no_trade_output_completes_without_order_and_reads_back_via_api(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(session, registered_decision=registered.decision)

        async with sessions.begin() as session:
            started = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-safe-no-trade",
            )
        assert started.status == "researching", started.result_code
        assert len(started.research_run_ids) == 1

        output_at = datetime.now(UTC) + timedelta(seconds=5)
        async with sessions.begin() as session:
            run_id = started.research_run_ids[0]
            run = await session.get(AgentResearchRunRecord, run_id)
            assert run is not None
            manifest = run.evidence_manifest
            assert len(manifest) == 1
            evidence = await session.get(ResearchEvidenceRef, UUID(manifest[0]["id"]))
            assert evidence is not None
            intent = _safe_no_trade_intent(
                run=run,
                evidence=evidence,
                valid_until=output_at + timedelta(days=1),
            )
            intent_document = intent.model_dump(mode="json")
            payload = {"quality_status": "partial"}
            output_id = UUID("63000000-0000-0000-0000-000000000702")
            content_hash = sha256(
                json.dumps(
                    {
                        "status": "completed",
                        "role": None,
                        "payload": payload,
                        "intent": intent_document,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ).encode()
            ).hexdigest()
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text("UPDATE agent_research_runs SET status='completed' WHERE id=:run_id"),
                    {"run_id": run_id},
                )
                await session.execute(
                    text(
                        "UPDATE jobs SET status='succeeded',started_at=:at,finished_at=:at,"
                        "attempt_count=1,result_code='AGENT_RESEARCH_COMPLETED',version=version+1 "
                        "WHERE payload->>'run_id'=:run_id"
                    ),
                    {"at": output_at, "run_id": str(run_id)},
                )
            finally:
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
                await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
            session.add(
                AgentResearchOutputRecord(
                    id=output_id,
                    run_id=run_id,
                    sequence=1,
                    status="completed",
                    role=None,
                    prompt_hash=None,
                    content_hash=content_hash,
                    payload=payload,
                    intent=intent_document,
                )
            )
            await session.flush()
            session.add(
                AgentResearchOutputEvidenceRecord(
                    output_id=output_id,
                    evidence_ref_id=evidence.id,
                    source=evidence.source,
                    provider_object_id=evidence.provider_object_id,
                    revision=evidence.revision,
                    available_at=evidence.available_at,
                    location=evidence.location,
                    content_hash=evidence.content_hash,
                )
            )

        async with sessions.begin() as session:
            completed = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-safe-no-trade",
            )
            order_count = await session.scalar(
                select(func.count())
                .select_from(PaperOrderRecord)
                .where(PaperOrderRecord.strategy_decision_id == completed.strategy_decision_id)
            )
            event_codes = tuple(
                await session.scalars(
                    select(AgentPaperCycleEventRecord.result_code)
                    .where(AgentPaperCycleEventRecord.cycle_id == completed.cycle_id)
                    .order_by(AgentPaperCycleEventRecord.sequence)
                )
            )

        assert completed.status == "completed"
        assert completed.result_code == "NO_ORDER_REQUIRED"
        assert completed.strategy_decision_id is not None
        assert completed.batch_id is not None
        assert completed.order_ids == ()
        assert order_count == 0
        assert event_codes[-3:] == (
            "AGENT_DECISION_RECORDED",
            "PAPER_BATCH_EVALUATED",
            "NO_ORDER_REQUIRED",
        )

        async with sessions.begin() as session:
            no_change = await session.scalar(
                select(RiskNoChangeEvaluationRecord).where(
                    RiskNoChangeEvaluationRecord.strategy_decision_id
                    == completed.strategy_decision_id
                )
            )
            assert no_change is not None
            await session.execute(
                text("ALTER TABLE risk_no_change_evaluations DISABLE TRIGGER USER")
            )
            try:
                await session.execute(
                    text(
                        "UPDATE risk_no_change_evaluations SET created_at=:created_at "
                        "WHERE id=:evaluation_id"
                    ),
                    {
                        "created_at": output_at + timedelta(seconds=2),
                        "evaluation_id": no_change.id,
                    },
                )
            finally:
                await session.execute(
                    text("ALTER TABLE risk_no_change_evaluations ENABLE TRIGGER USER")
                )

        path = "/internal/v1/paper/cycles/latest"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(
                app=app(migrated_database_url, now=output_at + timedelta(seconds=1))
            ),
            base_url="http://testserver",
        ) as client:
            historical_response = await client.get(path, headers=headers("GET", path))

        assert historical_response.status_code == 200
        assert historical_response.json()["data"]["decision"]["risk_outcomes"] == []

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(
                app=app(migrated_database_url, now=output_at + timedelta(seconds=3))
            ),
            base_url="http://testserver",
        ) as client:
            response = await client.get(path, headers=headers("GET", path))

        assert response.status_code == 200
        data = response.json()["data"]
        assert data["status"] == "completed"
        assert data["result_code"] == "NO_ORDER_REQUIRED"
        assert data["execution_locked"] is True
        assert data["order_ids"] == []
        assert data["decision"]["risk_outcomes"] == ["no_change"]
        assert len(data["candidates"]) == 1
        candidate = data["candidates"][0]
        intent_hash = candidate.pop("intent_hash")
        assert len(intent_hash) == 64
        assert set(intent_hash) <= set("0123456789abcdef")
        assert candidate == {
            "instrument_id": str(INSTRUMENTS[0][0]),
            "symbol": "AAPL",
            "selection_reasons": ["PAPER_BATCH_TEST"],
            "research_status": "completed",
            "stance": "watch",
            "sizing_action": "none",
            "output_id": str(output_id),
            "rejection_code": None,
        }

        async with sessions.begin() as session:
            for _ in range(49):
                instrument_id = uuid4()
                session.add(
                    Instrument(
                        id=instrument_id,
                        asset_type="equity",
                        exchange="XNAS",
                        currency="USD",
                        tick_size=Decimal("0.01"),
                        risk_bucket="large_cap",
                    )
                )
                session.add(
                    RiskNoChangeEvaluationRecord(
                        batch_id=no_change.batch_id,
                        account_id=no_change.account_id,
                        strategy_decision_id=no_change.strategy_decision_id,
                        instrument_id=instrument_id,
                        current_quantity=no_change.current_quantity,
                        target_quantity=no_change.target_quantity,
                        target_weight=no_change.target_weight,
                        strategy_priority=no_change.strategy_priority,
                        reference_close=no_change.reference_close,
                        price_authority_id=no_change.price_authority_id,
                        authority_ids=no_change.authority_ids,
                        reason_code=no_change.reason_code,
                        input_hash=no_change.input_hash,
                        evidence_hash=no_change.evidence_hash,
                    )
                )

        boundary_time = output_at + timedelta(minutes=1)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=boundary_time)),
            base_url="http://testserver",
        ) as client:
            bounded_response = await client.get(path, headers=headers("GET", path))

        assert bounded_response.status_code == 200
        assert len(bounded_response.json()["data"]["decision"]["risk_outcomes"]) == 50

        async with sessions.begin() as session:
            instrument_id = uuid4()
            session.add(
                Instrument(
                    id=instrument_id,
                    asset_type="equity",
                    exchange="XNAS",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="large_cap",
                )
            )
            session.add(
                RiskNoChangeEvaluationRecord(
                    batch_id=no_change.batch_id,
                    account_id=no_change.account_id,
                    strategy_decision_id=no_change.strategy_decision_id,
                    instrument_id=instrument_id,
                    current_quantity=no_change.current_quantity,
                    target_quantity=no_change.target_quantity,
                    target_weight=no_change.target_weight,
                    strategy_priority=no_change.strategy_priority,
                    reference_close=no_change.reference_close,
                    price_authority_id=no_change.price_authority_id,
                    authority_ids=no_change.authority_ids,
                    reason_code=no_change.reason_code,
                    input_hash=no_change.input_hash,
                    evidence_hash=no_change.evidence_hash,
                )
            )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=boundary_time)),
            base_url="http://testserver",
        ) as client:
            unbounded_response = await client.get(path, headers=headers("GET", path))

        assert unbounded_response.status_code == 409
        assert unbounded_response.json()["error"]["code"] == "DATA_PARTIAL"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_agent_version_pair_resumes_original_outputs_without_duplicate_work(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    evaluation_calendar_id = UUID("63000000-0000-0000-0000-000000000810")
    compatible_model_id = UUID("63000000-0000-0000-0000-000000000811")
    evaluation_actor = "test-agent-version-pair"
    evaluation_start = datetime(2026, 8, 26, 19, tzinfo=UTC)
    train_dates = (date(2026, 8, 26), date(2026, 8, 27))
    oos_dates = (
        date(2026, 8, 28),
        date(2026, 8, 31),
        date(2026, 9, 1),
        date(2026, 9, 2),
        date(2026, 9, 3),
        date(2026, 9, 4),
        date(2026, 9, 7),
        date(2026, 9, 8),
        date(2026, 9, 9),
        date(2026, 9, 10),
    )

    def slots(dates: tuple[date, ...]) -> tuple[datetime, ...]:
        return tuple(
            datetime.combine(day, datetime.min.time(), tzinfo=UTC)
            + timedelta(hours=19, minutes=minute)
            for day in dates
            for minute in (0, 5)
        )

    train_slots = slots(train_dates)
    oos_slots = slots(oos_dates)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(
                session,
                registered_decision=registered.decision,
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                quote_received_at=evaluation_start,
                price_available_at=evaluation_start,
            )
            await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text("UPDATE universe_snapshots SET published_at=:at"),
                    {"at": evaluation_start - timedelta(seconds=1)},
                )
            finally:
                await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
            base_snapshot = await select_authoritative_snapshot(
                session,
                instrument_id=INSTRUMENTS[0][0],
                as_of=evaluation_start,
            )
            orchestrator = await session.scalar(
                select(AgentPromptVersionRecord)
                .where(AgentPromptVersionRecord.role == "orchestrator")
                .order_by(AgentPromptVersionRecord.id.desc())
                .limit(1)
            )
            assert base_snapshot is not None
            assert orchestrator is not None
            session.add(
                AgentModelConfigRecord(
                    id=compatible_model_id,
                    model_id=MODEL_ID,
                    version=ACTIONABLE_CONFIG_VERSION,
                    config_hash="9" * 64,
                    max_input_tokens=aggregate_input_limit(ACTIONABLE_CONFIG_VERSION),
                    max_tool_calls=1,
                    max_output_tokens=8192,
                    input_usd_per_million=Decimal("0"),
                    output_usd_per_million=Decimal("0"),
                    pricing_available_at=evaluation_start - timedelta(days=1),
                    pricing_expires_at=oos_slots[-1] + timedelta(days=1),
                )
            )
            await session.flush()
            source = await admit_research_run(
                session,
                actor_id="agent-version-incumbent-source",
                route="agent-version-source-v1",
                idempotency_key="agent-version-incumbent-source",
                request_hash="8" * 64,
                request=ResearchRequest(
                    instrument_id=INSTRUMENTS[0][0],
                    data_snapshot_id=base_snapshot.id,
                    decision_time=evaluation_start,
                ),
                prompt_version_id=orchestrator.id,
                model_config_id=compatible_model_id,
                daily_limit_usd=Decimal("3"),
                now=evaluation_start,
            )
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text("UPDATE agent_research_runs SET status='completed' WHERE id=:run_id"),
                    {"run_id": source.run_id},
                )
            finally:
                await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))

            prompts = load_prompt_assets()
            challenger_orchestrator_id: UUID | None = None
            for index, role in enumerate(ROLE_ORDER, start=1):
                content = f"{prompts[role].content}\nchallenger version"
                prompt_id = UUID(int=0x63000000000000000000000000000820 + index)
                if role == "orchestrator":
                    challenger_orchestrator_id = prompt_id
                session.add(
                    AgentPromptVersionRecord(
                        id=prompt_id,
                        role=role,
                        version="agent-version-challenger-v1",
                        content_hash=sha256(content.encode()).hexdigest(),
                        content=content,
                    )
                )
            assert challenger_orchestrator_id is not None
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=evaluation_calendar_id,
                    exchange="XNYS",
                    version="agent-version-pair-v1",
                    source="exchange_calendars",
                    range_start=date(2026, 8, 25),
                    range_end=oos_dates[-1],
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="8" * 64,
                    published_at=evaluation_start - timedelta(seconds=1),
                )
            )
            session.add_all(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=evaluation_calendar_id,
                    session=session_date,
                    decision_at=datetime.combine(session_date, datetime.min.time(), tzinfo=UTC)
                    + timedelta(hours=20),
                    market_open_at=datetime.combine(session_date, datetime.min.time(), tzinfo=UTC)
                    + timedelta(hours=13, minutes=30),
                    market_close_at=datetime.combine(session_date, datetime.min.time(), tzinfo=UTC)
                    + timedelta(hours=20),
                )
                for session_date in (date(2026, 8, 25),) + train_dates + oos_dates
            )
            await session.flush()
            registration = await register_agent_version_evaluation(
                session,
                request=AgentVersionEvaluationRequest(
                    instrument_id=INSTRUMENTS[0][0],
                    incumbent_version_source_run_id=source.run_id,
                    challenger_prompt_version_id=challenger_orchestrator_id,
                    challenger_model_config_id=compatible_model_id,
                    initial_capital=Decimal("10000"),
                    train_slots=train_slots,
                    oos_slots=oos_slots,
                ),
                actor_id=evaluation_actor,
                idempotency_key="agent-version-pair-registration",
                now=evaluation_start,
            )

        async with sessions.begin() as session:
            prepared = await advance_agent_version_slot(
                session,
                evaluation_id=registration.evaluation_id,
                decision_slot_at=train_slots[0],
                daily_limit_usd=Decimal("3"),
                now=train_slots[0],
            )
        assert prepared.status == "running", prepared.reason_code
        assert prepared.incumbent_run_id is not None
        assert prepared.challenger_run_id is not None

        output_at = evaluation_start + timedelta(seconds=5)
        admitted: dict[UUID, ActionableResearchDecision] = {}
        async with sessions.begin() as session:
            for index, run_id in enumerate(
                (prepared.incumbent_run_id, prepared.challenger_run_id), start=1
            ):
                run = await session.get(AgentResearchRunRecord, run_id)
                assert run is not None
                research_manifest = next(
                    row
                    for row in run.evidence_manifest
                    if row["source"] != "nexus_agent_paper_context"
                )
                evidence = await session.get(ResearchEvidenceRef, UUID(research_manifest["id"]))
                assert evidence is not None
                frozen_evidence_rows: list[ResearchEvidenceRef] = []
                for row in run.evidence_manifest:
                    frozen = await session.get(ResearchEvidenceRef, UUID(row["id"]))
                    assert frozen is not None
                    frozen_evidence_rows.append(frozen)
                frozen_evidence = tuple(frozen_evidence_rows)
                intent = _safe_no_trade_intent(
                    run=run,
                    evidence=evidence,
                    valid_until=output_at + timedelta(days=1),
                )
                intent = intent.model_copy(
                    update={
                        "evidence_refs": tuple(
                            EvidenceRef(
                                source=row.source,
                                object_id=row.provider_object_id,
                                available_at=row.available_at,
                                location=row.location,
                                content_hash=row.content_hash,
                            )
                            for row in frozen_evidence
                        )
                    }
                )
                intent_document = intent.model_dump(mode="json")
                payload = {"quality_status": "partial"}
                output_id = UUID(int=0x63000000000000000000000000000900 + index)
                content_hash = sha256(
                    json.dumps(
                        {
                            "status": "completed",
                            "role": None,
                            "payload": payload,
                            "intent": intent_document,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                        default=str,
                    ).encode()
                ).hexdigest()
                await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
                await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
                try:
                    await session.execute(
                        text("UPDATE agent_research_runs SET status='completed' WHERE id=:run_id"),
                        {"run_id": run_id},
                    )
                    await session.execute(
                        text(
                            "UPDATE jobs SET status='succeeded',started_at=:at,finished_at=:at,"
                            "attempt_count=1,result_code='AGENT_RESEARCH_COMPLETED',"
                            "version=version+1 WHERE payload->>'run_id'=:run_id"
                        ),
                        {"at": output_at, "run_id": str(run_id)},
                    )
                finally:
                    await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
                    await session.execute(
                        text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER")
                    )
                session.add(
                    AgentResearchOutputRecord(
                        id=output_id,
                        run_id=run_id,
                        sequence=1,
                        status="completed",
                        role=None,
                        prompt_hash=None,
                        content_hash=content_hash,
                        payload=payload,
                        intent=intent_document,
                    )
                )
                await session.flush()
                session.add_all(
                    AgentResearchOutputEvidenceRecord(
                        output_id=output_id,
                        evidence_ref_id=row.id,
                        source=row.source,
                        provider_object_id=row.provider_object_id,
                        revision=row.revision,
                        available_at=row.available_at,
                        location=row.location,
                        content_hash=row.content_hash,
                    )
                    for row in frozen_evidence
                )
                admitted[run_id] = ActionableResearchDecision(
                    instrument_id=run.instrument_id,
                    output_id=output_id,
                    intent_hash=content_hash,
                    stance="buy_candidate",
                    sizing_action="increase",
                    evidence_ref_ids=tuple(row.id for row in frozen_evidence),
                )

        async def fake_load_actionable(
            _session: AsyncSession,
            *,
            run_id: UUID,
            decision_time: datetime,
            allow_safe_no_trade: bool,
        ) -> ActionableResearchDecision:
            assert decision_time in {train_slots[0], output_at}
            assert allow_safe_no_trade is True
            return admitted[run_id]

        monkeypatch.setattr(
            "nexus.paper.agent_cycle.load_actionable_research_output", fake_load_actionable
        )
        monkeypatch.setattr(
            "nexus.paper.agent_decision_store.load_actionable_research_output",
            fake_load_actionable,
        )

        async with sessions.begin() as session:
            pending = await resume_agent_version_slot(
                session,
                evaluation_id=registration.evaluation_id,
                decision_slot_at=train_slots[0],
                now=output_at,
            )
            pair_count = int(
                await session.scalar(select(func.count()).select_from(AgentVersionCyclePairRecord))
                or 0
            )
            version_job_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(Job)
                    .where(
                        Job.payload["run_id"]
                        .as_string()
                        .in_((str(prepared.incumbent_run_id), str(prepared.challenger_run_id)))
                    )
                )
                or 0
            )
            order_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(PaperOrderRecord)
                    .where(
                        PaperOrderRecord.account_id.in_(
                            (
                                registration.incumbent_account_id,
                                registration.challenger_account_id,
                            )
                        )
                    )
                )
                or 0
            )
            broker_intent_count = int(
                await session.scalar(text("SELECT count(*) FROM broker_execution_intents")) or 0
            )
        assert pending.status == "running", pending.reason_code
        assert pending.reason_code == "PAIR_EXECUTION_PENDING"
        assert pair_count == 0
        assert version_job_count == 2
        assert order_count == 2
        assert broker_intent_count == 0

        assert prepared.incumbent_cycle_id is not None
        assert prepared.challenger_cycle_id is not None
        async with sessions() as session:
            orders = tuple(
                await session.scalars(
                    select(PaperOrderRecord)
                    .where(
                        PaperOrderRecord.account_id.in_(
                            (
                                registration.incumbent_account_id,
                                registration.challenger_account_id,
                            )
                        )
                    )
                    .order_by(PaperOrderRecord.account_id)
                )
            )
        order_by_account = {order.account_id: order for order in orders}
        assert set(order_by_account) == {
            registration.incumbent_account_id,
            registration.challenger_account_id,
        }

        evaluated_times: list[datetime] = []
        provider = _SchwabPriceHistoryProvider()
        for cycle_id, account_id in (
            (prepared.incumbent_cycle_id, registration.incumbent_account_id),
            (prepared.challenger_cycle_id, registration.challenger_account_id),
        ):
            async with sessions() as session:
                scope = await resolve_agent_paper_market_scope(session, cycle_id=cycle_id)
            evaluated_at = scope.window_end + timedelta(seconds=2)
            evaluated_times.append(evaluated_at)
            market = await collect_agent_paper_window(
                sessions,
                cycle_id=cycle_id,
                provider=provider,
                object_store=_ObjectStore(),
                evaluated_at=evaluated_at,
            )
            assert market.purpose == "paper_fill"
            async with sessions.begin() as session:
                filled = await advance_internal_paper_order(
                    session,
                    order_id=order_by_account[account_id].id,
                    evaluated_at=evaluated_at,
                    idempotency_key=f"agent-version-fill:{account_id}",
            )
            assert filled.action == "filled"
        assert provider.calls == 1

        final_now = max(evaluated_times)
        async with sessions.begin() as session:
            paired = await resume_agent_version_slot(
                session,
                evaluation_id=registration.evaluation_id,
                decision_slot_at=train_slots[0],
                now=final_now,
            )
            pair_count = int(
                await session.scalar(select(func.count()).select_from(AgentVersionCyclePairRecord))
                or 0
            )
            fills_by_account = {
                account_id: int(
                    await session.scalar(
                        select(func.count())
                        .select_from(PaperFillRecord)
                        .where(PaperFillRecord.account_id == account_id)
                    )
                    or 0
                )
                for account_id in (
                    registration.incumbent_account_id,
                    registration.challenger_account_id,
                )
            }
            fills = {
                account_id: tuple(
                    await session.scalars(
                        select(PaperFillRecord)
                        .where(PaperFillRecord.account_id == account_id)
                        .order_by(PaperFillRecord.fill_seq)
                    )
                )
                for account_id in (
                    registration.incumbent_account_id,
                    registration.challenger_account_id,
                )
            }
            position_adjustments = {
                account_id: tuple(
                    await session.scalars(
                        select(PaperPositionAdjustmentEventRecord)
                        .where(PaperPositionAdjustmentEventRecord.account_id == account_id)
                        .order_by(PaperPositionAdjustmentEventRecord.event_seq)
                    )
                )
                for account_id in (
                    registration.incumbent_account_id,
                    registration.challenger_account_id,
                )
            }
            market_windows = tuple(
                await session.scalars(
                    select(AgentPaperMarketWindowRecord)
                    .where(
                        AgentPaperMarketWindowRecord.cycle_id.in_(
                            (prepared.incumbent_cycle_id, prepared.challenger_cycle_id)
                        )
                    )
                    .order_by(AgentPaperMarketWindowRecord.cycle_id)
                )
            )
            for account_id in fills_by_account:
                transactions = tuple(
                    await session.scalars(
                        select(PaperLedgerTransactionRecord).where(
                            PaperLedgerTransactionRecord.account_id == account_id
                        )
                    )
                )
                transaction_ids = tuple(row.id for row in transactions)
                entries = tuple(
                    await session.scalars(
                        select(PaperLedgerEntryRecord).where(
                            PaperLedgerEntryRecord.transaction_id.in_(transaction_ids)
                        )
                    )
                )
                assert all(
                    sum(
                        (
                            entry.debit_amount
                            for entry in entries
                            if entry.transaction_id == transaction.id
                        ),
                        Decimal(0),
                    )
                    == sum(
                        (
                            entry.credit_amount
                            for entry in entries
                            if entry.transaction_id == transaction.id
                        ),
                        Decimal(0),
                    )
                    for transaction in transactions
                )
            broker_intent_count = int(
                await session.scalar(text("SELECT count(*) FROM broker_execution_intents")) or 0
            )
        assert paired.status == "paired", paired.reason_code
        assert paired.reason_code == "PAIR_RECORDED"
        assert pair_count == 1
        assert fills_by_account == {
            registration.incumbent_account_id: 1,
            registration.challenger_account_id: 1,
        }
        incumbent_fills = fills[registration.incumbent_account_id]
        challenger_fills = fills[registration.challenger_account_id]
        assert tuple(
            (row.instrument_id, row.quantity, row.price, row.notional, row.fee)
            for row in incumbent_fills
        ) == tuple(
            (row.instrument_id, row.quantity, row.price, row.notional, row.fee)
            for row in challenger_fills
        )
        incumbent_adjustments = position_adjustments[registration.incumbent_account_id]
        challenger_adjustments = position_adjustments[registration.challenger_account_id]
        assert tuple(
            (row.instrument_id, row.event_type, row.quantity_delta, row.cost_delta)
            for row in incumbent_adjustments
        ) == tuple(
            (row.instrument_id, row.event_type, row.quantity_delta, row.cost_delta)
            for row in challenger_adjustments
        )
        assert len(market_windows) == 2
        assert len({row.source_authority_id for row in market_windows}) == 1
        assert len({row.raw_artifact_id for row in market_windows}) == 1
        assert len({row.price_series_manifest_id for row in market_windows}) == 1
        assert len({row.content_hash for row in market_windows}) == 2
        assert broker_intent_count == 0

        async with sessions.begin() as session:
            replay = await resume_agent_version_slot(
                session,
                evaluation_id=registration.evaluation_id,
                decision_slot_at=train_slots[0],
                now=final_now,
            )
            assert (
                int(
                    await session.scalar(
                        select(func.count()).select_from(AgentVersionCyclePairRecord)
                    )
                    or 0
                )
                == 1
            )
        assert replay.model_copy(update={"replayed": False}) == paired
        assert replay.replayed is True
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_quasi_realtime_authority_uses_database_time_window_and_same_session(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE exchange_calendar_authorities DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
            try:
                published_at = datetime(2026, 8, 25, 20, tzinfo=UTC)
                await session.execute(
                    text("UPDATE exchange_calendar_authorities SET published_at=:at"),
                    {"at": published_at},
                )
                await session.execute(
                    text("UPDATE universe_snapshots SET published_at=:at"),
                    {"at": published_at},
                )
                await session.execute(
                    text(
                        "UPDATE universe_snapshots SET key='us_liquid',"
                        "version='us_liquid_v1',source='fosun_local_read_only'"
                    )
                )
            finally:
                await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE exchange_calendar_authorities ENABLE TRIGGER USER")
                )
        async with sessions() as session:
            at_open = await load_quasi_realtime_cycle_authority(
                session,
                account_id=seed.account_id,
                now=datetime(2026, 8, 26, 11, tzinfo=UTC),
                expected_calendar_authority_id=CALENDAR_ID,
            )
            before_close = await load_quasi_realtime_cycle_authority(
                session,
                account_id=seed.account_id,
                now=datetime(2026, 8, 26, 19, 59, 59, tzinfo=UTC),
                expected_calendar_authority_id=CALENDAR_ID,
            )
            with pytest.raises(ValueError, match="AGENT_PAPER_MARKET_WINDOW_CLOSED"):
                await load_quasi_realtime_cycle_authority(
                    session,
                    account_id=seed.account_id,
                    now=datetime(2026, 8, 26, 20, tzinfo=UTC),
                    expected_calendar_authority_id=CALENDAR_ID,
                )

        assert at_open.decision_session == date(2026, 8, 26)
        assert at_open.execution_session == at_open.decision_session
        assert at_open.decision_time == datetime(2026, 8, 26, 11, tzinfo=UTC)
        assert before_close.decision_time == datetime(2026, 8, 26, 19, 59, 59, tzinfo=UTC)
        assert at_open.quality_report_id is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_service_calendar_selector_works_before_window_and_pins_one_authority(
    migrated_database_url: str,
) -> None:
    await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE exchange_calendar_authorities DISABLE TRIGGER USER")
            )
            try:
                await session.execute(
                    text(
                        "UPDATE exchange_calendar_authorities SET published_at=:at "
                        "WHERE id=:calendar_id"
                    ),
                    {
                        "at": datetime(2026, 8, 25, 20, tzinfo=UTC),
                        "calendar_id": CALENDAR_ID,
                    },
                )
            finally:
                await session.execute(
                    text("ALTER TABLE exchange_calendar_authorities ENABLE TRIGGER USER")
                )
        async with sessions() as session:
            selected = await agent_cycle_authority.select_agent_paper_service_calendar_authority(
                session,
                now=datetime(2026, 8, 26, 9, 0, tzinfo=UTC),
            )
        assert selected.session == date(2026, 8, 26)
        assert selected.next_session > selected.session
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_cycle_loader_rejects_authority_switch(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as session:
            with pytest.raises(
                ValueError,
                match="AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH",
            ):
                await load_quasi_realtime_cycle_authority(
                    session,
                    account_id=seed.account_id,
                    now=datetime(2026, 8, 26, 13, 0, tzinfo=UTC),
                    expected_calendar_authority_id=UUID(int=0),
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_code",
    [
        "AGENT_PAPER_CALENDAR_UNAVAILABLE",
        "AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH",
        "AGENT_PAPER_MARKET_WINDOW_CLOSED",
    ],
)
async def test_trigger_once_persists_and_replays_calendar_failure(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    failure_code: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    fixed_now = datetime(2026, 8, 26, 13, tzinfo=UTC)
    provider_calls = 0

    async def reserve_with_fixed_time(
        session: AsyncSession,
        *,
        account_id: UUID,
        client_idempotency_key: str,
        now: datetime,
    ):
        del now
        return await reserve_schwab_agent_paper_trigger(
            session,
            account_id=account_id,
            client_idempotency_key=client_idempotency_key,
            now=fixed_now,
        )

    async def reject_calendar(*_args, **_kwargs):
        nonlocal provider_calls
        assert _kwargs["process_environment"] == {"NEXUS_SCHWAB_APP_KEY": "registered-key"}
        provider_calls += 1
        raise ValueError(failure_code)

    monkeypatch.setattr(
        agent_paper_once,
        "reserve_schwab_agent_paper_trigger",
        reserve_with_fixed_time,
    )
    monkeypatch.setattr(agent_paper_once, "trigger_in_session", reject_calendar)
    monkeypatch.setattr(agent_paper_once, "S3ProviderEvidenceStore", lambda *_a, **_k: object())
    settings = agent_paper_once.AgentPaperTriggerSettings(
        database_url=migrated_database_url,
        object_store_endpoint="http://127.0.0.1:9000",
        alpaca_key="key",
        alpaca_secret="secret",
    )
    request = agent_paper_once.AgentPaperTriggerRequest(
        action="trigger",
        account_id=seed.account_id,
        idempotency_key=failure_code.lower(),
    )

    with pytest.raises(ValueError, match=failure_code):
        await agent_paper_once.trigger_once(
            settings,
            request,
            repo_root=ENGINE_ROOT.parent,
            process_environment={"NEXUS_SCHWAB_APP_KEY": "registered-key"},
        )

    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as session:
            claim = await session.scalar(
                select(IdempotencyRecord).where(
                    IdempotencyRecord.route == "agent-paper-schwab-trigger-v2",
                    IdempotencyRecord.idempotency_key == floor_decision_slot(fixed_now).isoformat(),
                )
            )
        assert claim is not None
        assert claim.response_payload == {
            "outcome": "failed",
            "error_code": failure_code,
        }
    finally:
        await engine.dispose()

    with pytest.raises(ValueError, match=failure_code):
        await agent_paper_once.trigger_once(settings, request, repo_root=ENGINE_ROOT.parent)
    assert provider_calls == 1


@pytest.mark.asyncio
async def test_trigger_once_does_not_persist_or_replay_unknown_provider_failure(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    fixed_now = datetime(2026, 8, 26, 13, 5, tzinfo=UTC)
    provider_calls = 0

    async def reserve_with_fixed_time(
        session: AsyncSession,
        *,
        account_id: UUID,
        client_idempotency_key: str,
        now: datetime,
    ):
        del now
        return await reserve_schwab_agent_paper_trigger(
            session,
            account_id=account_id,
            client_idempotency_key=client_idempotency_key,
            now=fixed_now,
        )

    async def reject_provider(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        raise RuntimeError("private-provider-failure")

    monkeypatch.setattr(
        agent_paper_once,
        "reserve_schwab_agent_paper_trigger",
        reserve_with_fixed_time,
    )
    monkeypatch.setattr(agent_paper_once, "trigger_in_session", reject_provider)
    monkeypatch.setattr(agent_paper_once, "S3ProviderEvidenceStore", lambda *_a, **_k: object())
    settings = agent_paper_once.AgentPaperTriggerSettings(
        database_url=migrated_database_url,
        object_store_endpoint="http://127.0.0.1:9000",
        alpaca_key="key",
        alpaca_secret="secret",
    )
    request = agent_paper_once.AgentPaperTriggerRequest(
        action="trigger",
        account_id=seed.account_id,
        idempotency_key="unknown-provider-failure",
    )

    with pytest.raises(RuntimeError, match="private-provider-failure"):
        await agent_paper_once.trigger_once(settings, request, repo_root=ENGINE_ROOT.parent)
    with pytest.raises(ValueError, match="AGENT_PAPER_RESULT_UNKNOWN"):
        await agent_paper_once.trigger_once(settings, request, repo_root=ENGINE_ROOT.parent)
    assert provider_calls == 1


@pytest.mark.asyncio
async def test_service_calendar_selector_observes_half_day_dst_and_weekend(
    migrated_database_url: str,
) -> None:
    calendar_id = UUID("63000000-0000-0000-0000-000000000091")
    sessions_to_add = (
        (
            date(2026, 7, 6),
            datetime(2026, 7, 6, 13, 30, tzinfo=UTC),
            datetime(2026, 7, 6, 20, tzinfo=UTC),
        ),
        (
            date(2026, 7, 7),
            datetime(2026, 7, 7, 13, 30, tzinfo=UTC),
            datetime(2026, 7, 7, 20, tzinfo=UTC),
        ),
        (
            date(2026, 11, 27),
            datetime(2026, 11, 27, 14, 30, tzinfo=UTC),
            datetime(2026, 11, 27, 18, tzinfo=UTC),
        ),
        (
            date(2026, 11, 30),
            datetime(2026, 11, 30, 14, 30, tzinfo=UTC),
            datetime(2026, 11, 30, 21, tzinfo=UTC),
        ),
        (
            date(2026, 12, 1),
            datetime(2026, 12, 1, 14, 30, tzinfo=UTC),
            datetime(2026, 12, 1, 21, tzinfo=UTC),
        ),
        (
            date(2026, 12, 2),
            datetime(2026, 12, 2, 14, 30, tzinfo=UTC),
            datetime(2026, 12, 2, 21, tzinfo=UTC),
        ),
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=calendar_id,
                    exchange="XNYS",
                    version="agent-paper-service-calendar",
                    source="fixture",
                    range_start=date(2026, 7, 6),
                    range_end=date(2026, 12, 3),
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="8" * 64,
                    published_at=datetime(2026, 7, 1, tzinfo=UTC),
                    created_at=datetime(2026, 7, 1, tzinfo=UTC),
                )
            )
            await session.flush()
            session.add_all(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=calendar_id,
                    session=session_date,
                    decision_at=market_close,
                    market_open_at=market_open,
                    market_close_at=market_close,
                    created_at=datetime(2026, 7, 1, tzinfo=UTC),
                )
                for session_date, market_open, market_close in sessions_to_add
            )

        async with sessions() as session:
            summer = await agent_cycle_authority.select_agent_paper_service_calendar_authority(
                session,
                now=datetime(2026, 7, 6, 9, tzinfo=UTC),
            )
            half_day = await agent_cycle_authority.select_agent_paper_service_calendar_authority(
                session,
                now=datetime(2026, 11, 27, 12, tzinfo=UTC),
            )
            after_half_day = (
                await agent_cycle_authority.select_agent_paper_service_calendar_authority(
                    session,
                    now=datetime(2026, 11, 27, 18, tzinfo=UTC),
                )
            )
            weekend = await agent_cycle_authority.select_agent_paper_service_calendar_authority(
                session,
                now=datetime(2026, 11, 28, 15, tzinfo=UTC),
            )
            winter = await agent_cycle_authority.select_agent_paper_service_calendar_authority(
                session,
                now=datetime(2026, 12, 1, 9, tzinfo=UTC),
            )

        assert half_day.market_close_at == datetime(2026, 11, 27, 18, tzinfo=UTC)
        assert after_half_day.session == date(2026, 11, 30)
        assert after_half_day.next_session == date(2026, 12, 1)
        assert summer.decision_window_start_at == datetime(2026, 7, 6, 11, tzinfo=UTC)
        assert winter.decision_window_start_at == datetime(2026, 12, 1, 12, tzinfo=UTC)
        assert summer.market_open_at == datetime(2026, 7, 6, 13, 30, tzinfo=UTC)
        assert winter.market_open_at == datetime(2026, 12, 1, 14, 30, tzinfo=UTC)
        assert weekend.session == date(2026, 11, 30)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_service_calendar_bound_loader_keeps_expected_authority_after_half_day_close(
    migrated_database_url: str,
) -> None:
    bound_id = UUID("63000000-0000-0000-0000-000000000095")
    newer_id = UUID("63000000-0000-0000-0000-000000000096")
    published_at = datetime(2026, 11, 26, tzinfo=UTC)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            for authority_id, version, publication, content_hash in (
                (bound_id, "agent-paper-bound-calendar", published_at, "4" * 64),
                (
                    newer_id,
                    "agent-paper-newer-calendar",
                    published_at + timedelta(hours=1),
                    "5" * 64,
                ),
            ):
                session.add(
                    ExchangeCalendarAuthorityRecord(
                        id=authority_id,
                        exchange="XNYS",
                        version=version,
                        source="fixture",
                        range_start=date(2026, 11, 27),
                        range_end=date(2026, 12, 1),
                        query_parameters={},
                        completeness_status="complete",
                        content_hash=content_hash,
                        published_at=publication,
                        created_at=publication,
                    )
                )
            await session.flush()
            for authority_id in (bound_id, newer_id):
                session.add_all(
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=authority_id,
                        session=session_date,
                        decision_at=market_close,
                        market_open_at=market_open,
                        market_close_at=market_close,
                        created_at=published_at,
                    )
                    for session_date, market_open, market_close in (
                        (
                            date(2026, 11, 27),
                            datetime(2026, 11, 27, 14, 30, tzinfo=UTC),
                            datetime(2026, 11, 27, 18, tzinfo=UTC),
                        ),
                        (
                            date(2026, 11, 30),
                            datetime(2026, 11, 30, 14, 30, tzinfo=UTC),
                            datetime(2026, 11, 30, 21, tzinfo=UTC),
                        ),
                        (
                            date(2026, 12, 1),
                            datetime(2026, 12, 1, 14, 30, tzinfo=UTC),
                            datetime(2026, 12, 1, 21, tzinfo=UTC),
                        ),
                    )
                )

        after_close = datetime(2026, 11, 27, 18, 1, tzinfo=UTC)
        async with sessions() as session:
            loaded = await load_agent_paper_service_calendar_authority(
                session,
                now=after_close,
                expected_calendar_authority_id=bound_id,
            )
            with pytest.raises(ValueError, match="AGENT_PAPER_CALENDAR_UNAVAILABLE"):
                await load_agent_paper_service_calendar_authority(
                    session,
                    now=after_close,
                    expected_calendar_authority_id=UUID(int=0),
                )
            with pytest.raises(ValueError, match="AGENT_PAPER_CALENDAR_UNAVAILABLE"):
                await load_agent_paper_service_calendar_authority(
                    session,
                    now=datetime(2026, 12, 2, 15, tzinfo=UTC),
                    expected_calendar_authority_id=bound_id,
                )

        assert loaded.calendar_authority_id == bound_id
        assert loaded.market_close_at == datetime(2026, 11, 27, 18, tzinfo=UTC)
        assert loaded.next_session == date(2026, 11, 30)
        assert loaded.next_decision_window_start_at == datetime(2026, 11, 30, 12, tzinfo=UTC)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_current_trends_rejects_short_bound_calendar_before_provider_call(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    authority_id = UUID("63000000-0000-0000-0000-000000000097")
    instrument_id = UUID("63000000-0000-0000-0000-000000000098")
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            database_now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(database_now, datetime)
            local_date = database_now.astimezone(ZoneInfo("America/New_York")).date()
            prior_session = local_date - timedelta(days=1)
            short_start = prior_session - timedelta(days=30)
            published_at = database_now - timedelta(seconds=1)
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=authority_id,
                    exchange="XNYS",
                    version="agent-paper-short-trends-calendar",
                    source="fixture",
                    range_start=short_start,
                    range_end=local_date + timedelta(days=1),
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="6" * 64,
                    published_at=published_at,
                    created_at=published_at,
                )
            )
            await session.flush()
            session.add_all(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=authority_id,
                    session=session_date,
                    decision_at=datetime.combine(
                        session_date,
                        datetime.min.time(),
                        tzinfo=UTC,
                    )
                    + timedelta(hours=21),
                    market_open_at=datetime.combine(
                        session_date,
                        datetime.min.time(),
                        tzinfo=UTC,
                    )
                    + timedelta(hours=14, minutes=30),
                    market_close_at=datetime.combine(
                        session_date,
                        datetime.min.time(),
                        tzinfo=UTC,
                    )
                    + timedelta(hours=21),
                    created_at=published_at,
                )
                for session_date in (short_start, local_date, local_date + timedelta(days=1))
            )

        async def prior_scope(*_args: object, **_kwargs: object) -> object:
            return agent_paper_loop.PriorDailyScope(
                instrument_id=instrument_id,
                prior_session=prior_session,
                authority_ready=True,
                decision_session=local_date,
            )

        async def unexpected_provider(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("short bound calendar must stop before Provider")

        monkeypatch.setattr(agent_paper_loop, "_inspect_prior_daily_scope", prior_scope)
        monkeypatch.setattr(agent_paper_loop, "collect_prices", unexpected_provider)

        with pytest.raises(ValueError, match="AGENT_PAPER_CALENDAR_UNAVAILABLE"):
            await agent_paper_loop.prepare_current_trends_once(
                SimpleNamespace(
                    database_url=migrated_database_url,
                    alpaca_key=object(),
                    alpaca_secret=object(),
                ),
                account_id=UUID(int=1),
                repo_root=tmp_path,
                now=database_now,
                expected_calendar_authority_id=authority_id,
            )
        assert not (tmp_path / ".secrets").exists()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_bound_calendar_stays_fixed_across_close_weekend_and_all_cycle_consumers(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    bound_id = CALENDAR_ID
    newer_id = UUID("63000000-0000-0000-0000-000000000099")
    limited_id = UUID("63000000-0000-0000-0000-000000000100")
    friday = date(2026, 8, 28)
    monday = date(2026, 8, 31)
    tuesday = date(2026, 9, 1)
    history_start = friday - timedelta(days=366)
    bound_published_at = datetime(2026, 8, 27, 12, tzinfo=UTC)
    bind_at = datetime(2026, 8, 28, 17, tzinfo=UTC)
    after_close = datetime(2026, 8, 28, 20, 1, tzinfo=UTC)
    weekend_at = datetime(2026, 8, 30, 16, tzinfo=UTC)
    monday_at = datetime(2026, 8, 31, 15, tzinfo=UTC)

    def calendar_session(
        authority_id: UUID,
        session_date: date,
        *,
        created_at: datetime,
        market_close_at: datetime | None = None,
    ) -> ExchangeCalendarSessionRecord:
        day_start = datetime.combine(session_date, datetime.min.time(), tzinfo=UTC)
        close_at = market_close_at or day_start + timedelta(hours=20)
        return ExchangeCalendarSessionRecord(
            calendar_authority_id=authority_id,
            session=session_date,
            decision_at=close_at,
            market_open_at=day_start + timedelta(hours=13, minutes=30),
            market_close_at=close_at,
            created_at=created_at,
        )

    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE exchange_calendar_authorities DISABLE TRIGGER USER")
            )
            await session.execute(
                text("ALTER TABLE exchange_calendar_sessions DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
            await session.execute(text("ALTER TABLE instrument_aliases DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE exchange_calendar_authorities SET "
                        "range_start=:range_start,range_end=:range_end,"
                        "published_at=:published_at,created_at=:published_at "
                        "WHERE id=:authority_id"
                    ),
                    {
                        "range_start": history_start,
                        "range_end": tuesday,
                        "published_at": bound_published_at,
                        "authority_id": bound_id,
                    },
                )
                await session.execute(
                    text(
                        "UPDATE exchange_calendar_sessions SET created_at=:published_at "
                        "WHERE calendar_authority_id=:authority_id"
                    ),
                    {"published_at": bound_published_at, "authority_id": bound_id},
                )
                await session.execute(
                    text(
                        "UPDATE universe_snapshots SET key='us_liquid',"
                        "version='us_liquid_v1',source='fosun_local_read_only',"
                        "published_at=:published_at,created_at=:published_at"
                    ),
                    {"published_at": bound_published_at},
                )
                await session.execute(
                    text(
                        "UPDATE instrument_aliases SET symbol='AAPL',created_at=:published_at "
                        "WHERE instrument_id=:instrument_id AND alias_type='ticker'"
                    ),
                    {
                        "published_at": bound_published_at,
                        "instrument_id": INSTRUMENTS[0][0],
                    },
                )
            finally:
                await session.execute(text("ALTER TABLE instrument_aliases ENABLE TRIGGER USER"))
                await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE exchange_calendar_sessions ENABLE TRIGGER USER")
                )
                await session.execute(
                    text("ALTER TABLE exchange_calendar_authorities ENABLE TRIGGER USER")
                )
            session.add_all(
                calendar_session(bound_id, session_date, created_at=bound_published_at)
                for session_date in (history_start, friday, monday, tuesday)
            )

        async with sessions() as session:
            bound = await agent_cycle_authority.select_agent_paper_service_calendar_authority(
                session,
                now=bind_at,
            )
        assert bound.calendar_authority_id == bound_id

        newer_published_at = bind_at + timedelta(minutes=1)
        async with sessions.begin() as session:
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=newer_id,
                    exchange="XNYS",
                    version="agent-paper-newer-after-bind",
                    source="fixture",
                    range_start=history_start,
                    range_end=tuesday,
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="7" * 64,
                    published_at=newer_published_at,
                    created_at=newer_published_at,
                )
            )
            await session.flush()
            session.add_all(
                calendar_session(
                    newer_id,
                    session_date,
                    created_at=newer_published_at,
                    market_close_at=(
                        datetime(2026, 8, 28, 20, 30, tzinfo=UTC)
                        if session_date == friday
                        else None
                    ),
                )
                for session_date in (history_start, friday, monday, tuesday)
            )

        async with sessions() as session:
            closed = await load_agent_paper_service_calendar_authority(
                session,
                now=after_close,
                expected_calendar_authority_id=bound_id,
            )
            weekend = await load_agent_paper_service_calendar_authority(
                session,
                now=weekend_at,
                expected_calendar_authority_id=bound_id,
            )
            next_session = await load_agent_paper_service_calendar_authority(
                session,
                now=monday_at,
                expected_calendar_authority_id=bound_id,
            )
            would_reselect = (
                await agent_cycle_authority.select_agent_paper_service_calendar_authority(
                    session,
                    now=monday_at,
                )
            )
        assert closed.calendar_authority_id == bound_id
        assert closed.next_session == monday
        assert weekend.calendar_authority_id == bound_id
        assert weekend.session == monday
        assert next_session.calendar_authority_id == bound_id
        assert next_session.session == monday
        assert would_reselect.calendar_authority_id == newer_id

        fixed_func = SimpleNamespace(
            clock_timestamp=lambda: literal(monday_at),
            max=func.max,
        )
        monkeypatch.setattr(agent_paper_loop, "func", fixed_func)
        loop_snapshot = await inspect_agent_paper_loop(
            EngineSettings(database_url=migrated_database_url),
            account_id=seed.account_id,
            expected_calendar_authority_id=bound_id,
        )
        assert loop_snapshot.calendar_authority_id == bound_id
        assert loop_snapshot.performance_due_at == datetime(2026, 8, 28, 20, tzinfo=UTC)

        decision = SimpleNamespace(
            decision_time=monday_at,
            decision_session=monday,
            universe_snapshot_id=UUID("51000000-0000-0000-0000-000000000002"),
        )
        async with sessions.begin() as session:
            await _seed_adjusted_authority(
                session,
                decision,
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                available_at=monday_at - timedelta(minutes=30),
            )
            prior_page = ProviderPage(
                items=(
                    PriceBar(
                        instrument_id=INSTRUMENTS[0][0],
                        session=friday,
                        open=Decimal("100"),
                        high=Decimal("101"),
                        low=Decimal("99"),
                        close=Decimal("100.5"),
                        volume=Decimal("1000000"),
                        observed_at=datetime(2026, 8, 28, 20, tzinfo=UTC),
                        available_at=monday_at - timedelta(minutes=30),
                        received_at=monday_at - timedelta(minutes=30),
                        revision="bound-calendar-prior-friday",
                        quality_flags=(),
                    ),
                ),
                next_cursor=None,
                raw_payload=b'{"bound_calendar":"prior_friday"}',
                source_request_id=None,
                as_of=monday_at - timedelta(minutes=30),
                provenance=MarketDataProvenance(
                    source="alpaca",
                    feed="sip",
                    adjustment="all",
                    interval="1Day",
                    mode="delayed",
                    delivery_mode="delayed",
                ),
            )
            result = await ingest_price_page(
                session,
                _ObjectStore(),
                prior_page,
                commit=False,
                source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
            )
            assert result.price_authority is not None

        trigger_settings = agent_paper_once.AgentPaperTriggerSettings(
            database_url=migrated_database_url,
            alpaca_key="test-key",
            alpaca_secret="test-secret",
        )
        prerequisite_ids: list[UUID] = []
        real_service_loader = agent_paper_loop.load_agent_paper_service_calendar_authority

        async def observed_service_loader(*args: object, **kwargs: object) -> object:
            received_id = kwargs.get("expected_calendar_authority_id")
            assert isinstance(received_id, UUID)
            prerequisite_ids.append(received_id)
            return await real_service_loader(*args, **kwargs)

        monkeypatch.setattr(
            agent_paper_loop,
            "load_agent_paper_service_calendar_authority",
            observed_service_loader,
        )
        daily = await agent_paper_loop.prepare_prior_daily_once(
            trigger_settings,
            account_id=seed.account_id,
            repo_root=tmp_path,
            now=monday_at,
            expected_calendar_authority_id=bound_id,
        )
        macro = await agent_paper_loop.prepare_macro_once(
            agent_paper_loop.DailyOnceSettings(database_url=migrated_database_url),
            fred_key=agent_paper_loop.SecretStr("test-key"),
            account_id=seed.account_id,
            repo_root=tmp_path,
            now=monday_at,
            expected_calendar_authority_id=bound_id,
        )
        trends_attempt = tmp_path / ".secrets" / "nexus-agent-paper" / "trends" / friday.isoformat()
        trends_attempt.mkdir(mode=0o700, parents=True)
        trends = await agent_paper_loop.prepare_current_trends_once(
            trigger_settings,
            account_id=seed.account_id,
            repo_root=tmp_path,
            now=monday_at,
            expected_calendar_authority_id=bound_id,
        )
        assert daily == "ready"
        assert macro == "unavailable"
        assert trends == "unavailable"
        assert prerequisite_ids == [bound_id] * 5

        monkeypatch.setattr(
            agent_paper_once,
            "func",
            SimpleNamespace(clock_timestamp=lambda: literal(monday_at)),
        )
        observed_trigger_ids: list[UUID] = []
        real_cycle_loader = agent_paper_once.load_quasi_realtime_cycle_authority

        async def observed_cycle_loader(*args: object, **kwargs: object) -> object:
            received_id = kwargs.get("expected_calendar_authority_id")
            assert isinstance(received_id, UUID)
            observed_trigger_ids.append(received_id)
            return await real_cycle_loader(*args, **kwargs)

        async def collect_proofs(*_args: object, **_kwargs: object) -> tuple[()]:
            return ()

        artifact_id = UUID("63000000-0000-0000-0000-000000000101")

        async def capture_stream(*_args: object, **_kwargs: object) -> object:
            return SimpleNamespace(quote_ids={}, artifact_id=artifact_id)

        async def persist_proofs(*_args: object, **_kwargs: object) -> tuple[()]:
            return ()

        async def start_cycle(*_args: object, **kwargs: object) -> object:
            authority = kwargs["authority"]
            assert authority.calendar_authority_id == bound_id
            return agent_paper_once.AgentPaperCycleResult(
                cycle_id=UUID("63000000-0000-0000-0000-000000000102"),
                status="researching",
                result_code="RESEARCH_JOBS_PREPARED",
                replayed=False,
            )

        monkeypatch.setattr(
            agent_paper_once,
            "load_quasi_realtime_cycle_authority",
            observed_cycle_loader,
        )
        monkeypatch.setattr(agent_paper_once, "collect_source_proofs", collect_proofs)
        monkeypatch.setattr(
            agent_paper_once,
            "capture_and_import_schwab_level_one_once",
            capture_stream,
        )
        monkeypatch.setattr(agent_paper_once, "persist_query_authorities", persist_proofs)
        monkeypatch.setattr(agent_paper_once, "_validate_trigger_quote", lambda *_a, **_k: None)
        monkeypatch.setattr(agent_paper_once, "start_schwab_agent_paper_cycle", start_cycle)
        async with sessions.begin() as session:
            result = await agent_paper_once.trigger_in_session(
                session,
                object(),
                trigger_settings,
                agent_paper_once.AgentPaperTriggerRequest(
                    action="trigger",
                    account_id=seed.account_id,
                    idempotency_key="bound-calendar-next-session",
                ),
                reservation=agent_paper_once.AgentPaperTriggerReservation(
                    claim_id=UUID("63000000-0000-0000-0000-000000000103"),
                    decision_session=monday,
                    decision_slot_at=monday_at.replace(minute=0),
                ),
                repo_root=tmp_path,
                client=object(),
                expected_calendar_authority_id=bound_id,
            )
        assert result.status == "researching"
        assert observed_trigger_ids == [bound_id, bound_id]

        async with sessions.begin() as session:
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=limited_id,
                    exchange="XNYS",
                    version="agent-paper-bound-missing-next",
                    source="fixture",
                    range_start=history_start,
                    range_end=monday,
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="8" * 64,
                    published_at=bound_published_at,
                    created_at=bound_published_at,
                )
            )
            await session.flush()
            session.add_all(
                calendar_session(limited_id, session_date, created_at=bound_published_at)
                for session_date in (friday, monday)
            )
        async with sessions() as session:
            usable_before_close = await load_agent_paper_service_calendar_authority(
                session,
                now=bind_at,
                expected_calendar_authority_id=limited_id,
            )
            with pytest.raises(ValueError, match="AGENT_PAPER_CALENDAR_UNAVAILABLE"):
                await load_agent_paper_service_calendar_authority(
                    session,
                    now=monday_at,
                    expected_calendar_authority_id=limited_id,
                )
        assert usable_before_close.calendar_authority_id == limited_id
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_service_calendar_selector_requires_two_future_sessions_after_close(
    migrated_database_url: str,
) -> None:
    calendar_id = UUID("63000000-0000-0000-0000-000000000092")
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=calendar_id,
                    exchange="XNYS",
                    version="agent-paper-no-following-session",
                    source="fixture",
                    range_start=date(2026, 8, 26),
                    range_end=date(2026, 8, 28),
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="7" * 64,
                    published_at=datetime(2026, 8, 25, tzinfo=UTC),
                    created_at=datetime(2026, 8, 25, tzinfo=UTC),
                )
            )
            await session.flush()
            session.add_all(
                (
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=calendar_id,
                        session=date(2026, 8, 26),
                        decision_at=datetime(2026, 8, 26, 20, tzinfo=UTC),
                        market_open_at=datetime(2026, 8, 26, 13, 30, tzinfo=UTC),
                        market_close_at=datetime(2026, 8, 26, 20, tzinfo=UTC),
                        created_at=datetime(2026, 8, 25, tzinfo=UTC),
                    ),
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=calendar_id,
                        session=date(2026, 8, 27),
                        decision_at=datetime(2026, 8, 27, 20, tzinfo=UTC),
                        market_open_at=datetime(2026, 8, 27, 13, 30, tzinfo=UTC),
                        market_close_at=datetime(2026, 8, 27, 20, tzinfo=UTC),
                        created_at=datetime(2026, 8, 25, tzinfo=UTC),
                    ),
                )
            )

        async with sessions() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_CALENDAR_UNAVAILABLE"):
                await agent_cycle_authority.select_agent_paper_service_calendar_authority(
                    session,
                    now=datetime(2026, 8, 26, 20, tzinfo=UTC),
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_service_calendar_selector_does_not_fallback_from_newer_malformed_authority(
    migrated_database_url: str,
) -> None:
    older_id = UUID("63000000-0000-0000-0000-000000000093")
    newer_id = UUID("63000000-0000-0000-0000-000000000094")
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            for authority_id, version, range_end, published_at, content_hash in (
                (
                    older_id,
                    "agent-paper-valid-older",
                    date(2026, 8, 28),
                    datetime(2026, 8, 24, tzinfo=UTC),
                    "5" * 64,
                ),
                (
                    newer_id,
                    "agent-paper-malformed-newer",
                    date(2026, 8, 27),
                    datetime(2026, 8, 25, tzinfo=UTC),
                    "6" * 64,
                ),
            ):
                session.add(
                    ExchangeCalendarAuthorityRecord(
                        id=authority_id,
                        exchange="XNYS",
                        version=version,
                        source="fixture",
                        range_start=date(2026, 8, 26),
                        range_end=range_end,
                        query_parameters={},
                        completeness_status="complete",
                        content_hash=content_hash,
                        published_at=published_at,
                        created_at=published_at,
                    )
                )
            await session.flush()
            session.add_all(
                [
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=older_id,
                        session=session_date,
                        decision_at=market_close,
                        market_open_at=market_open,
                        market_close_at=market_close,
                        created_at=datetime(2026, 8, 24, tzinfo=UTC),
                    )
                    for session_date, market_open, market_close in (
                        (
                            date(2026, 8, 26),
                            datetime(2026, 8, 26, 13, 30, tzinfo=UTC),
                            datetime(2026, 8, 26, 20, tzinfo=UTC),
                        ),
                        (
                            date(2026, 8, 27),
                            datetime(2026, 8, 27, 13, 30, tzinfo=UTC),
                            datetime(2026, 8, 27, 20, tzinfo=UTC),
                        ),
                    )
                ]
                + [
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=newer_id,
                        session=date(2026, 8, 26),
                        decision_at=datetime(2026, 8, 26, 20, tzinfo=UTC),
                        market_open_at=datetime(2026, 8, 26, 13, 30, tzinfo=UTC),
                        market_close_at=datetime(2026, 8, 26, 20, tzinfo=UTC),
                        created_at=datetime(2026, 8, 25, tzinfo=UTC),
                    ),
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=newer_id,
                        session=date(2026, 8, 28),
                        decision_at=datetime(2026, 8, 28, 20, tzinfo=UTC),
                        market_open_at=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
                        market_close_at=datetime(2026, 8, 28, 20, tzinfo=UTC),
                        created_at=datetime(2026, 8, 25, tzinfo=UTC),
                    ),
                ]
            )

        async with sessions() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_CALENDAR_UNAVAILABLE"):
                await agent_cycle_authority.select_agent_paper_service_calendar_authority(
                    session,
                    now=datetime(2026, 8, 26, 9, tzinfo=UTC),
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_cycle_loader_keeps_expected_authority_when_newer_candidate_exists(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    newer_id = UUID("63000000-0000-0000-0000-000000000095")
    published_at = datetime(2026, 8, 25, 20, tzinfo=UTC)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE exchange_calendar_authorities DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text("UPDATE exchange_calendar_authorities SET published_at=:at"),
                    {"at": published_at - timedelta(hours=1)},
                )
                await session.execute(
                    text(
                        "UPDATE universe_snapshots SET key='us_liquid',"
                        "version='us_liquid_v1',source='fosun_local_read_only',published_at=:at"
                    ),
                    {"at": published_at - timedelta(hours=1)},
                )
            finally:
                await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE exchange_calendar_authorities ENABLE TRIGGER USER")
                )
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=newer_id,
                    exchange="XNYS",
                    version="agent-paper-newer-unrelated",
                    source="fixture",
                    range_start=date(2026, 8, 1),
                    range_end=date(2026, 8, 31),
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="4" * 64,
                    published_at=published_at,
                    created_at=published_at,
                )
            )
            await session.flush()
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=newer_id,
                    session=date(2026, 8, 26),
                    decision_at=datetime(2026, 8, 26, 20, tzinfo=UTC),
                    market_open_at=datetime(2026, 8, 26, 13, 30, tzinfo=UTC),
                    market_close_at=datetime(2026, 8, 26, 20, tzinfo=UTC),
                    created_at=published_at,
                )
            )

        async with sessions() as session:
            authority = await load_quasi_realtime_cycle_authority(
                session,
                account_id=seed.account_id,
                now=datetime(2026, 8, 26, 13, tzinfo=UTC),
                expected_calendar_authority_id=CALENDAR_ID,
            )
        assert authority.calendar_authority_id == CALENDAR_ID
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_cycle_loader_uses_persisted_half_day_close(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    published_at = datetime(2026, 8, 25, 20, tzinfo=UTC)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE exchange_calendar_authorities DISABLE TRIGGER USER")
            )
            await session.execute(
                text("ALTER TABLE exchange_calendar_sessions DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text("UPDATE exchange_calendar_authorities SET published_at=:at"),
                    {"at": published_at},
                )
                await session.execute(
                    text(
                        "UPDATE exchange_calendar_sessions SET market_close_at=:close "
                        "WHERE calendar_authority_id=:calendar_id AND session=:session"
                    ),
                    {
                        "close": datetime(2026, 8, 26, 18, tzinfo=UTC),
                        "calendar_id": CALENDAR_ID,
                        "session": date(2026, 8, 26),
                    },
                )
                await session.execute(
                    text(
                        "UPDATE universe_snapshots SET key='us_liquid',"
                        "version='us_liquid_v1',source='fosun_local_read_only',published_at=:at"
                    ),
                    {"at": published_at},
                )
            finally:
                await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE exchange_calendar_sessions ENABLE TRIGGER USER")
                )
                await session.execute(
                    text("ALTER TABLE exchange_calendar_authorities ENABLE TRIGGER USER")
                )

        async with sessions() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_MARKET_WINDOW_CLOSED"):
                await load_quasi_realtime_cycle_authority(
                    session,
                    account_id=seed.account_id,
                    now=datetime(2026, 8, 26, 18, tzinfo=UTC),
                    expected_calendar_authority_id=CALENDAR_ID,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_cycle_loader_rejects_session_created_after_pinned_time(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    now = datetime(2026, 8, 26, 13, tzinfo=UTC)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE exchange_calendar_authorities DISABLE TRIGGER USER")
            )
            await session.execute(
                text("ALTER TABLE exchange_calendar_sessions DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text("UPDATE exchange_calendar_authorities SET published_at=:at"),
                    {"at": now - timedelta(hours=1)},
                )
                await session.execute(
                    text(
                        "UPDATE exchange_calendar_sessions SET created_at=:created_at "
                        "WHERE calendar_authority_id=:calendar_id AND session=:session"
                    ),
                    {
                        "created_at": now + timedelta(seconds=1),
                        "calendar_id": CALENDAR_ID,
                        "session": date(2026, 8, 26),
                    },
                )
                await session.execute(
                    text(
                        "UPDATE universe_snapshots SET key='us_liquid',"
                        "version='us_liquid_v1',source='fosun_local_read_only',published_at=:at"
                    ),
                    {"at": now - timedelta(hours=1)},
                )
            finally:
                await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE exchange_calendar_sessions ENABLE TRIGGER USER")
                )
                await session.execute(
                    text("ALTER TABLE exchange_calendar_authorities ENABLE TRIGGER USER")
                )

        async with sessions() as session:
            with pytest.raises(
                ValueError,
                match="AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH",
            ):
                await load_quasi_realtime_cycle_authority(
                    session,
                    account_id=seed.account_id,
                    now=now,
                    expected_calendar_authority_id=CALENDAR_ID,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_quasi_realtime_authority_observes_dst_holiday_and_allows_later_slots(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    winter_session = date(2026, 12, 1)
    winter_open = datetime(2026, 12, 1, 14, 30, tzinfo=UTC)
    winter_close = datetime(2026, 12, 1, 21, tzinfo=UTC)
    calendar_id = UUID("63000000-0000-0000-0000-000000000098")
    try:
        async with sessions.begin() as session:
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=calendar_id,
                    exchange="XNYS",
                    version="agent-paper-winter-test",
                    source="fixture",
                    range_start=winter_session,
                    range_end=winter_session,
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="9" * 64,
                    published_at=datetime(2026, 11, 30, tzinfo=UTC),
                    created_at=datetime(2026, 11, 30, tzinfo=UTC),
                )
            )
            await session.flush()
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=calendar_id,
                    session=winter_session,
                    decision_at=winter_close,
                    market_open_at=winter_open,
                    market_close_at=winter_close,
                    created_at=datetime(2026, 11, 30, tzinfo=UTC),
                )
            )
            await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE universe_snapshots SET key='us_liquid',"
                        "version='us_liquid_v1',source='fosun_local_read_only',"
                        "published_at='2026-11-30T00:00:00Z'"
                    )
                )
            finally:
                await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))

        async with sessions() as session:
            winter = await load_quasi_realtime_cycle_authority(
                session,
                account_id=seed.account_id,
                now=datetime(2026, 12, 1, 12, tzinfo=UTC),
                expected_calendar_authority_id=calendar_id,
            )
            with pytest.raises(
                ValueError,
                match="AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH",
            ):
                await load_quasi_realtime_cycle_authority(
                    session,
                    account_id=seed.account_id,
                    now=datetime(2026, 11, 26, 12, tzinfo=UTC),
                    expected_calendar_authority_id=calendar_id,
                )
        assert winter.decision_session == winter_session

        async with sessions.begin() as session:
            session.add(
                AgentPaperCycleRecord(
                    id=UUID("63000000-0000-0000-0000-000000000097"),
                    account_id=winter.account_id,
                    portfolio_id=winter.portfolio_id,
                    decision_session=winter.decision_session,
                    decision_time=winter.decision_time,
                    decision_slot_at=floor_decision_slot(winter.decision_time),
                    execution_session=winter.execution_session,
                    universe_snapshot_id=winter.universe_snapshot_id,
                    quality_report_id=None,
                    decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                    execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                    candidate_manifest=[],
                    candidate_hash=candidate_manifest_hash(()),
                    sizing_policy_version="agent_paper_sizing_v1",
                    sizing_policy_hash="a" * 64,
                    execution_locked=True,
                )
            )
        async with sessions() as session:
            later = await load_quasi_realtime_cycle_authority(
                session,
                account_id=seed.account_id,
                now=datetime(2026, 12, 1, 12, 5, tzinfo=UTC),
                expected_calendar_authority_id=calendar_id,
            )
        assert later.decision_session == winter_session
        assert later.decision_time == datetime(2026, 12, 1, 12, 5, tzinfo=UTC)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_schwab_order_cycle_preserves_frozen_risk_and_is_loop_inspectable(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    decision_time = datetime(2026, 8, 26, 11, 30, tzinfo=UTC)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(
                session,
                registered_decision=registered.decision,
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                quote_received_at=decision_time,
                price_available_at=decision_time,
                base_payload={"portfolio_context": {"private_canary": "must-not-flow"}},
            )
            historical_tables = (
                "source_policies",
                "instrument_aliases",
                "price_series_manifests",
                "price_bar_records",
                "price_bar_set_authorities",
                "research_evidence_refs",
                "risk_query_authorities",
                "sector_exposures",
                "agent_data_snapshot_evidence",
            )
            for table_name in historical_tables:
                await session.execute(text(f"ALTER TABLE {table_name} DISABLE TRIGGER USER"))
                try:
                    await session.execute(
                        text(f"UPDATE {table_name} SET created_at=:at WHERE created_at>:at"),
                        {"at": decision_time},
                    )
                finally:
                    await session.execute(text(f"ALTER TABLE {table_name} ENABLE TRIGGER USER"))
            await session.execute(
                text("ALTER TABLE exchange_calendar_authorities DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
            try:
                published_at = decision_time - timedelta(days=1)
                await session.execute(
                    text("UPDATE exchange_calendar_authorities SET published_at=:at"),
                    {"at": published_at},
                )
                await session.execute(
                    text("UPDATE universe_snapshots SET published_at=:at"),
                    {"at": published_at},
                )
            finally:
                await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE exchange_calendar_authorities ENABLE TRIGGER USER")
                )
            newest_risk_ids = set(
                await session.scalars(
                    select(RiskQueryAuthorityRecord.id).where(
                        RiskQueryAuthorityRecord.source_policy_id
                        == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                        RiskQueryAuthorityRecord.available_at == decision_time,
                    )
                )
            )
            for index, kind in enumerate(("corporate_action", "suspension", "delisting"), start=1):
                older_at = decision_time - timedelta(minutes=1)
                watermark = f"{index + 3}" * 64
                session.add(
                    RiskQueryAuthorityRecord(
                        id=UUID(int=0x63000000000000000000000000000900 + index),
                        source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                        universe_snapshot_id=registered.decision.universe_snapshot_id,
                        decision_session=registered.decision.decision_session,
                        kind=kind,
                        required_instrument_ids=[INSTRUMENTS[0][0]],
                        blocked_instrument_ids=[],
                        complete=True,
                        source_watermark_hash=watermark,
                        available_at=older_at,
                        content_hash=canonical_query_authority_hash(
                            source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                            universe_snapshot_id=registered.decision.universe_snapshot_id,
                            decision_session=registered.decision.decision_session,
                            kind=kind,
                            required_instrument_ids=[INSTRUMENTS[0][0]],
                            blocked_instrument_ids=[],
                            complete=True,
                            source_watermark_hash=watermark,
                            available_at=older_at,
                        ),
                        created_at=decision_time,
                    )
                )
            await session.flush()

        async with sessions() as session:
            await session.execute(text("SET LOCAL enable_indexscan=off"))
            await session.execute(text("SET LOCAL enable_bitmapscan=off"))
            authority = await load_quasi_realtime_cycle_authority(
                session,
                account_id=seed.account_id,
                now=decision_time,
                expected_calendar_authority_id=CALENDAR_ID,
            )
            selected_previous = await session.get(
                ExchangeCalendarSessionRecord,
                (authority.calendar_authority_id, authority.decision_session - timedelta(days=1)),
            )
            if selected_previous is None:
                await session.execute(
                    text("ALTER TABLE exchange_calendar_sessions DISABLE TRIGGER USER")
                )
                try:
                    session.add(
                        ExchangeCalendarSessionRecord(
                            calendar_authority_id=authority.calendar_authority_id,
                            session=authority.decision_session - timedelta(days=1),
                            decision_at=DECISION_TIME - timedelta(days=1),
                            market_open_at=(decision_time - timedelta(days=1)).replace(
                                hour=13, minute=30
                            ),
                            market_close_at=(decision_time - timedelta(days=1)).replace(
                                hour=20, minute=0
                            ),
                            created_at=decision_time - timedelta(days=1),
                        )
                    )
                    await session.flush()
                finally:
                    await session.execute(
                        text("ALTER TABLE exchange_calendar_sessions ENABLE TRIGGER USER")
                    )
            manifest = await prepare_candidates(session, authority=authority)
            assert len(manifest) == 1
            assert set(manifest[0].risk_query_authority_ids) == newest_risk_ids
            cycle = AgentPaperCycleRecord(
                id=UUID("63000000-0000-0000-0000-000000000099"),
                account_id=authority.account_id,
                portfolio_id=authority.portfolio_id,
                decision_session=authority.decision_session,
                decision_time=authority.decision_time,
                decision_slot_at=floor_decision_slot(authority.decision_time),
                execution_session=authority.execution_session,
                universe_snapshot_id=authority.universe_snapshot_id,
                quality_report_id=None,
                decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                candidate_manifest=[row.model_dump(mode="json") for row in manifest],
                candidate_hash=candidate_manifest_hash(manifest),
                sizing_policy_version="agent_paper_sizing_v1",
                sizing_policy_hash="a" * 64,
                execution_locked=True,
            )
            market = await prepare_schwab_realtime_risk_snapshot(session, cycle=cycle)

        assert market.source_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
        assert market.execution_session == market.decision_session
        assert market.prices[0].authority_id == manifest[0].prior_daily_price_authority_id
        assert {proof.authority_id for proof in market.query_proofs} == set(
            manifest[0].risk_query_authority_ids
        )

        monkeypatch.setenv("DATABASE_URL", migrated_database_url)

        async def no_selected_version(session: AsyncSession, *, now: datetime) -> None:
            del session, now
            return None

        monkeypatch.setattr(
            agent_version_evaluation,
            "load_selected_internal_agent_version",
            no_selected_version,
        )
        migration_config = Config(str(ENGINE_ROOT / "alembic.ini"))
        await asyncio.to_thread(
            command.downgrade, migration_config, "0060_agent_paper_loop_heartbeat"
        )
        async with sessions.begin() as session:
            first = await start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=floor_decision_slot(authority.decision_time),
                idempotency_key="schwab-trigger-1",
            )
        async with sessions.begin() as session:
            await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER trg_jobs_state"))
            try:
                await session.execute(
                    text(
                        "UPDATE jobs SET job_type='agent.research',"
                        "job_key='agent.research' || substring("
                        "job_key FROM 21) WHERE job_type='agent.paper.research'"
                    )
                )
            finally:
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER trg_jobs_state"))
        async with sessions() as session:
            assert await session.scalar(select(Job.job_type)) == "agent.research"
        await asyncio.to_thread(command.upgrade, migration_config, "0061_agent_paper_job_isolation")
        async with sessions.begin() as session:
            replay = await start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=floor_decision_slot(authority.decision_time),
                idempotency_key="schwab-trigger-1",
            )
            job_count = await session.scalar(select(func.count()).select_from(Job))
            job_type = await session.scalar(select(Job.job_type))
        async with sessions.begin() as session:
            resumed = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=first.cycle_id,
                idempotency_key="schwab-resume-1",
            )
            resumed_job_count = await session.scalar(select(func.count()).select_from(Job))

        assert first.status == "researching"
        assert replay == first.model_copy(update={"replayed": True})
        assert resumed == first.model_copy(update={"replayed": True})
        assert len(first.research_run_ids) == 1
        assert job_count == resumed_job_count == 1
        assert job_type == "agent.paper.research"

        with pytest.raises(DBAPIError, match="agent paper snapshots prevent downgrade"):
            await asyncio.to_thread(
                command.downgrade, migration_config, "0060_agent_paper_loop_heartbeat"
            )
        async with sessions() as session:
            assert await session.scalar(select(Job.job_type)) == "agent.paper.research"
            assert (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                == "0061_agent_paper_job_isolation"
            )

        assert (
            await run_agent_worker_once(EngineSettings(database_url=migrated_database_url)) is False
        )
        async with sessions() as session:
            queued_job_status = await session.scalar(select(Job.status))
        assert queued_job_status == "queued"

        async with sessions() as session:
            run = await session.get(AgentResearchRunRecord, first.research_run_ids[0])
            assert run is not None
            assert run.data_snapshot_id != manifest[0].research_snapshot_id
            paper_snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
            assert paper_snapshot is not None
            assert "portfolio_context" not in paper_snapshot.payload
            selected = await select_authoritative_snapshot(
                session,
                instrument_id=run.instrument_id,
                as_of=run.decision_time,
            )
            assert selected is not None
            assert selected.id == manifest[0].research_snapshot_id
            evidence_records = await load_frozen_evidence(
                session,
                run,
                allow_fixture_evidence=False,
            )
            assert (
                await revalidate_frozen_agent_paper_context(
                    session,
                    run=run,
                    evidence_records=evidence_records,
                )
                is FrozenAgentPaperContextStatus.VALID
            )
            assert paper_snapshot.source == "nexus_agent_paper"
            assert paper_snapshot.schema_version == "agent_paper_research_v1"
            await session.execute(text("ALTER TABLE data_snapshots DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE data_snapshots SET source='nexus_research',"
                        "schema_version='research_filings_v1' WHERE id=:snapshot_id"
                    ),
                    {"snapshot_id": paper_snapshot.id},
                )
            finally:
                await session.execute(text("ALTER TABLE data_snapshots ENABLE TRIGGER USER"))
            session.expire(paper_snapshot)
            assert (
                await revalidate_frozen_agent_paper_context(
                    session,
                    run=run,
                    evidence_records=evidence_records,
                )
                is FrozenAgentPaperContextStatus.VALID
            )
            tainted_payload = {
                **paper_snapshot.payload,
                "portfolio_context": {"private_canary": "must-fail-closed"},
            }
            tainted_revision = sha256(
                json.dumps(tainted_payload, sort_keys=True).encode()
            ).hexdigest()
            taint_savepoint = await session.begin_nested()
            await session.execute(text("ALTER TABLE data_snapshots DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE data_snapshots SET payload=CAST(:payload AS jsonb),"
                        "revision=:revision WHERE id=:snapshot_id"
                    ),
                    {
                        "payload": json.dumps(tainted_payload),
                        "revision": tainted_revision,
                        "snapshot_id": paper_snapshot.id,
                    },
                )
            finally:
                await session.execute(text("ALTER TABLE data_snapshots ENABLE TRIGGER USER"))
            session.expire(paper_snapshot)
            assert (
                await revalidate_frozen_agent_paper_context(
                    session,
                    run=run,
                    evidence_records=evidence_records,
                )
                is FrozenAgentPaperContextStatus.INVALID
            )
            await taint_savepoint.rollback()
            session.expire(paper_snapshot)
            wrong_run = SimpleNamespace(
                id=UUID(int=0x63000000000000000000000000000999),
                data_snapshot_id=run.data_snapshot_id,
                instrument_id=run.instrument_id,
                decision_time=run.decision_time,
            )
            assert (
                await revalidate_frozen_agent_paper_context(
                    session,
                    run=wrong_run,
                    evidence_records=evidence_records,
                )
                is FrozenAgentPaperContextStatus.INVALID
            )
            tools = await database_read_only_tools(
                session,
                run,
                allow_fixture_evidence=False,
            )
            frozen = await tools.execute(
                "get_security_snapshot",
                {
                    "instrument_id": run.instrument_id,
                    "data_snapshot_id": run.data_snapshot_id,
                    "as_of": run.decision_time,
                },
            )
        context_items = [
            item for item in frozen["items"] if item["source"] == "nexus_agent_paper_context"
        ]
        assert len(context_items) == 1
        context = context_items[0]["agent_paper_context"]
        assert context["decision_quote"] == {
            "source": "schwab",
            "feed": "l1",
            "delivery_mode": "real_time",
            "delayed": False,
            "bid": "99.900000000000",
            "ask": "100.100000000000",
            "observed_at": "2026-08-26T11:29:50Z",
            "available_at": "2026-08-26T11:30:00Z",
            "received_at": "2026-08-26T11:30:00Z",
        }
        assert context["prior_daily"] == {
            "source": "alpaca",
            "feed": "sip",
            "delivery_mode": "delayed",
            "adjustment": "all",
            "session": "2026-08-25",
            "close": "100.000000000000",
        }
        assert context["paper_portfolio"] == {
            "candidate_position_weight": "0.000000000000",
            "cash_weight": "1.000000000000",
            "projection_hash": context["paper_portfolio"]["projection_hash"],
        }
        assert context["execution_locked"] is True
        assert "account_id" not in context
        assert "quantity" not in json.dumps(context)
        assert "available_cash" not in json.dumps(context)

        output_at = decision_time + timedelta(seconds=10)
        output_id = UUID("63000000-0000-0000-0000-000000000777")
        admitted = ActionableResearchDecision(
            instrument_id=manifest[0].instrument_id,
            output_id=output_id,
            intent_hash="9" * 64,
            stance="buy_candidate",
            sizing_action="increase",
            evidence_ref_ids=(UUID(int=778),),
        )
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE strategy_decision_targets DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE strategy_decisions DISABLE TRIGGER USER"))
            try:
                await session.execute(text("DELETE FROM strategy_decision_targets"))
                await session.execute(text("DELETE FROM strategy_decisions"))
            finally:
                await session.execute(text("ALTER TABLE strategy_decisions ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE strategy_decision_targets ENABLE TRIGGER USER")
                )
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text("UPDATE agent_research_runs SET status='completed' WHERE id=:run_id"),
                    {"run_id": first.research_run_ids[0]},
                )
                await session.execute(
                    text(
                        "UPDATE jobs SET status='succeeded',started_at=:at,finished_at=:at,"
                        "attempt_count=1,result_code='AGENT_RESEARCH_COMPLETED',version=version+1"
                    ),
                    {"at": output_at},
                )
            finally:
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
                await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
            session.add(
                AgentResearchOutputRecord(
                    id=output_id,
                    run_id=first.research_run_ids[0],
                    sequence=1,
                    status="completed",
                    role=None,
                    prompt_hash=None,
                    content_hash="7" * 64,
                    payload={},
                    intent={},
                    created_at=output_at,
                )
            )

        async def fake_load_actionable(
            *_args, decision_time: datetime, **_kwargs
        ) -> ActionableResearchDecision:
            assert decision_time == output_at
            assert _kwargs["allow_safe_no_trade"] is True
            return admitted

        monkeypatch.setattr(
            "nexus.paper.agent_cycle.load_actionable_research_output", fake_load_actionable
        )
        monkeypatch.setattr(
            "nexus.paper.agent_decision_store.load_actionable_research_output",
            fake_load_actionable,
        )
        async with sessions.begin() as session:
            savepoint = await session.begin_nested()
            session.add(
                PaperPositionAdjustmentEventRecord(
                    id=UUID("63000000-0000-0000-0000-000000000778"),
                    account_id=seed.account_id,
                    instrument_id=manifest[0].instrument_id,
                    event_seq=1,
                    event_type="correction",
                    quantity_delta=Decimal("1"),
                    cost_delta=Decimal("100"),
                    source_fill_id=None,
                    occurred_at=decision_time,
                    payload_hash="8" * 64,
                )
            )
            await session.flush()
            projection_conflict = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=first.cycle_id,
                idempotency_key="schwab-resume-projection-conflict",
            )
            assert projection_conflict.status == "unavailable"
            assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
            assert (
                await session.scalar(select(func.count()).select_from(StrategyDecisionRecord)) == 0
            )
            await savepoint.rollback()
        async with sessions.begin() as session:
            awaiting_market = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=first.cycle_id,
                idempotency_key="schwab-resume-2",
            )
        assert awaiting_market.status == "awaiting_market", awaiting_market.result_code
        assert len(awaiting_market.order_ids) == 1

        async with sessions() as session:
            scope = await resolve_agent_paper_market_scope(session, cycle_id=first.cycle_id)
            with pytest.raises(PaperMarketError, match="PAPER_MARKET_CYCLE_REJECTED"):
                await resolve_agent_paper_market_scope(
                    session,
                    cycle_id=first.cycle_id,
                    instrument_id=scope.instrument_id,
                )
        assert scope.window_start == datetime(2026, 8, 26, 11, 31, tzinfo=UTC)
        assert scope.window_end == datetime(2026, 8, 26, 11, 36, tzinfo=UTC)
        inspection_calendar_id = UUID("63000000-0000-0000-0000-000000000998")
        async with sessions.begin() as session:
            inspection_now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(inspection_now, datetime)
            inspection_date = inspection_now.astimezone(ZoneInfo("America/New_York")).date()
            published_at = inspection_now - timedelta(seconds=1)
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=inspection_calendar_id,
                    exchange="XNYS",
                    version="agent-paper-loop-inspection-current",
                    source="fixture",
                    range_start=inspection_date,
                    range_end=inspection_date + timedelta(days=2),
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="3" * 64,
                    published_at=published_at,
                    created_at=published_at,
                )
            )
            await session.flush()
            session.add_all(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=inspection_calendar_id,
                    session=session_date,
                    decision_at=(
                        datetime.combine(session_date, datetime.min.time(), tzinfo=UTC)
                        + timedelta(hours=21)
                    ),
                    market_open_at=(
                        datetime.combine(session_date, datetime.min.time(), tzinfo=UTC)
                        + timedelta(hours=14, minutes=30)
                    ),
                    market_close_at=(
                        datetime.combine(session_date, datetime.min.time(), tzinfo=UTC)
                        + timedelta(hours=21)
                    ),
                    created_at=published_at,
                )
                for session_date in (
                    inspection_date,
                    inspection_date + timedelta(days=1),
                    inspection_date + timedelta(days=2),
                )
            )
        loop_snapshot = await inspect_agent_paper_loop(
            EngineSettings(database_url=migrated_database_url),
            account_id=seed.account_id,
            expected_calendar_authority_id=inspection_calendar_id,
        )
        assert loop_snapshot.cycle_id == first.cycle_id
        assert loop_snapshot.order_id == awaiting_market.order_ids[0]
        assert loop_snapshot.market_window_end_at == scope.window_end + timedelta(seconds=60)
        evaluated_at = scope.window_end + timedelta(seconds=2)
        async with sessions.begin() as session:
            no_window = await advance_internal_paper_order(
                session,
                order_id=awaiting_market.order_ids[0],
                evaluated_at=evaluated_at,
                idempotency_key="schwab-paper-no-window-1",
            )
        assert no_window.action == "no_data"
        assert no_window.fill_id is None

        provider = _SchwabPriceHistoryProvider()
        window = await collect_agent_paper_window(
            sessions,
            cycle_id=first.cycle_id,
            provider=provider,
            object_store=_ObjectStore(),
            evaluated_at=evaluated_at,
        )
        window_replay = await collect_agent_paper_window(
            sessions,
            cycle_id=first.cycle_id,
            provider=provider,
            object_store=_ObjectStore(),
            evaluated_at=evaluated_at,
        )
        assert window.purpose == "paper_fill"
        assert (window.source, window.feed, window.delivery_mode) == (
            "schwab",
            "price_history",
            "unknown",
        )
        assert window_replay == window.model_copy(update={"replayed": True})
        assert provider.calls == 1
        wrong_collect_window = replace(
            loop_snapshot,
            market_window_start_at=scope.window_start + timedelta(minutes=5),
            market_window_end_at=scope.window_end + timedelta(minutes=5, seconds=60),
        )
        assert not await agent_paper_loop.confirm_agent_paper_shutdown(
            EngineSettings(database_url=migrated_database_url),
            account_id=seed.account_id,
            expected_calendar_authority_id=inspection_calendar_id,
            action=agent_paper_loop.AgentPaperLoopAction.COLLECT_WINDOW,
            before_snapshot=wrong_collect_window,
            action_result=None,
        )
        assert await agent_paper_loop.confirm_agent_paper_shutdown(
            EngineSettings(database_url=migrated_database_url),
            account_id=seed.account_id,
            expected_calendar_authority_id=inspection_calendar_id,
            action=agent_paper_loop.AgentPaperLoopAction.COLLECT_WINDOW,
            before_snapshot=loop_snapshot,
            action_result=None,
        )
        advance_snapshot = await inspect_agent_paper_loop(
            EngineSettings(database_url=migrated_database_url),
            account_id=seed.account_id,
            expected_calendar_authority_id=inspection_calendar_id,
        )
        assert advance_snapshot.market_window_id == window.authority_id
        assert advance_snapshot.market_window_start_at == window.window_start
        assert advance_snapshot.market_window_end_at == window.window_end
        async with sessions.begin() as session:
            filled = await advance_internal_paper_order(
                session,
                order_id=awaiting_market.order_ids[0],
                evaluated_at=evaluated_at,
                idempotency_key=(f"agent-paper-loop-advance:{awaiting_market.order_ids[0]}"),
            )
        async with sessions.begin() as session:
            fill_replay = await advance_internal_paper_order(
                session,
                order_id=awaiting_market.order_ids[0],
                evaluated_at=evaluated_at,
                idempotency_key=(f"agent-paper-loop-advance:{awaiting_market.order_ids[0]}"),
            )
        assert filled.action == "filled"
        assert fill_replay == filled
        assert await agent_paper_loop.confirm_agent_paper_shutdown(
            EngineSettings(database_url=migrated_database_url),
            account_id=seed.account_id,
            expected_calendar_authority_id=inspection_calendar_id,
            action=agent_paper_loop.AgentPaperLoopAction.ADVANCE_ORDER,
            before_snapshot=advance_snapshot,
            action_result=None,
        )
        assert not await agent_paper_loop.confirm_agent_paper_shutdown(
            EngineSettings(database_url=migrated_database_url),
            account_id=seed.account_id,
            expected_calendar_authority_id=inspection_calendar_id,
            action=agent_paper_loop.AgentPaperLoopAction.ADVANCE_ORDER,
            before_snapshot=replace(advance_snapshot, market_window_id=UUID(int=404)),
            action_result=None,
        )
        async with sessions() as session:
            debits = await session.scalar(select(func.sum(PaperLedgerEntryRecord.debit_amount)))
            credits = await session.scalar(select(func.sum(PaperLedgerEntryRecord.credit_amount)))
            fill_count = await session.scalar(select(func.count(PaperFillRecord.id)))
            execution_window_count = await session.scalar(
                select(func.count(PaperExecutionWindowAuthorityRecord.id))
            )
            execution_member_count = await session.scalar(
                select(func.count(PaperExecutionWindowMemberRecord.bar_id))
            )
            reservation_events = tuple(
                await session.scalars(
                    select(PaperReservationEventRecord.event_type)
                    .where(PaperReservationEventRecord.account_id == seed.account_id)
                    .order_by(PaperReservationEventRecord.event_seq)
                )
            )
        assert debits == credits
        assert fill_count == execution_window_count == 1
        assert execution_member_count == 5
        assert "consumed" in reservation_events
        assert "released" in reservation_events

        async with sessions() as session:
            source = await load_agent_paper_intent_source(
                session,
                cycle_id=first.cycle_id,
                now=evaluated_at,
            )
            persisted_order = await session.get(
                PaperOrderRecord,
                awaiting_market.order_ids[0],
            )
            assert persisted_order is not None
            expected_decision_id = persisted_order.strategy_decision_id
            broker_counts_list: list[int] = []
            for table in (
                "broker_execution_intents",
                "broker_batch_approvals",
                "broker_commands",
                "jobs",
            ):
                broker_counts_list.append(
                    int(await session.scalar(text(f"SELECT count(*) FROM {table}")) or 0)
                )
            broker_counts = tuple(broker_counts_list)
        assert source.cycle_id == first.cycle_id
        assert source.strategy_decision_id == expected_decision_id
        assert source.execution_locked is True
        assert source.risk_policy_hash
        assert len(source.orders) == 1
        assert source.orders[0].paper_order_id == awaiting_market.order_ids[0]
        assert source.orders[0].instrument_id == manifest[0].instrument_id
        assert source.orders[0].side == "buy"
        assert source.orders[0].quantity == filled.filled_quantity
        assert source.evidence_hashes == tuple(sorted(set(source.evidence_hashes)))
        assert all(len(value) == 64 for value in source.evidence_hashes)
        assert broker_counts[:3] == (0, 0, 0)
        assert broker_counts[3] == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_manifest_resumes_research_without_duplicate_jobs(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_close = datetime(2026, 8, 27, 20, tzinfo=UTC)
    seed = await seed_paper_batch(migrated_database_url, execution_market_close_at=market_close)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(session, registered_decision=registered.decision)

        async with sessions.begin() as session:
            first = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
        assert first.status == "researching", first.result_code
        assert first.replayed is False
        assert len(first.research_run_ids) == 1

        async with sessions.begin() as session:
            cycle = await session.scalar(select(AgentPaperCycleRecord))
            assert cycle is not None
            original_candidate = cycle.candidate_manifest[0]
            legacy_candidate = LegacyCandidateManifestEntry(
                instrument_id=UUID(original_candidate["instrument_id"]),
                symbol=original_candidate["symbol"],
                inclusion_reason=original_candidate["inclusion_reason"],
                research_snapshot_id=UUID(original_candidate["research_snapshot_id"]),
            )
            await session.execute(text("ALTER TABLE agent_paper_cycles DISABLE TRIGGER USER"))
            try:
                cycle.candidate_manifest = [legacy_candidate.model_dump(mode="json")]
                cycle.candidate_hash = candidate_manifest_hash((legacy_candidate,))
                await session.flush()
            finally:
                await session.execute(text("ALTER TABLE agent_paper_cycles ENABLE TRIGGER USER"))
            second = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
            assert second == first.model_copy(update={"replayed": True})
            assert await session.scalar(select(func.count()).select_from(Job)) == 1
            cycle_count = await session.scalar(
                select(func.count()).select_from(AgentPaperCycleRecord)
            )
            assert cycle_count == 1
            cycle = await session.scalar(select(AgentPaperCycleRecord))
            assert cycle is not None
            assert len(cycle.candidate_manifest) == 1
            candidate = cycle.candidate_manifest[0]
            assert candidate["symbol"] == "AAPL"
            assert set(candidate) == {
                "instrument_id",
                "symbol",
                "inclusion_reason",
                "research_snapshot_id",
            }
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleEventRecord))
                == 2
            )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_IDEMPOTENCY_CONFLICT"):
                await advance_agent_paper_cycle(
                    session,
                    account_id=seed.account_id,
                    decision_time=DECISION_TIME + timedelta(seconds=1),
                    idempotency_key="agent-paper-cycle-1",
                )

        admitted: dict[UUID, ActionableResearchDecision] = {}
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE strategy_decision_targets DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE strategy_decisions DISABLE TRIGGER USER"))
            try:
                await session.execute(text("DELETE FROM strategy_decision_targets"))
                await session.execute(text("DELETE FROM strategy_decisions"))
            finally:
                await session.execute(text("ALTER TABLE strategy_decisions ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE strategy_decision_targets ENABLE TRIGGER USER")
                )
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE agent_research_runs SET status='completed' WHERE id = ANY(:run_ids)"
                    ),
                    {"run_ids": list(first.research_run_ids)},
                )
            finally:
                await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
            await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE jobs SET status='succeeded',started_at=:at,finished_at=:at,"
                        "attempt_count=1,result_code='AGENT_RESEARCH_COMPLETED',version=version+1"
                    ),
                    {"at": DECISION_TIME},
                )
            finally:
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
            for index, (run_id, instrument) in enumerate(
                zip(first.research_run_ids, INSTRUMENTS[:1], strict=True), start=1
            ):
                output = AgentResearchOutputRecord(
                    id=UUID(int=0x63000000000000000000000000000500 + index),
                    run_id=run_id,
                    sequence=1,
                    status="completed",
                    role=None,
                    prompt_hash=None,
                    content_hash=f"{index}" * 64,
                    payload={},
                    intent={},
                )
                session.add(output)
                admitted[run_id] = ActionableResearchDecision(
                    instrument_id=instrument[0],
                    output_id=output.id,
                    intent_hash=f"{index + 3}" * 64,
                    stance="buy_candidate" if index == 1 else "watch",
                    sizing_action="increase" if index == 1 else "none",
                    evidence_ref_ids=(UUID(int=0x63000000000000000000000000000200 + index),),
                )

        async def fake_load_actionable(
            _session: AsyncSession,
            *,
            run_id: UUID,
            decision_time: datetime,
            allow_safe_no_trade: bool,
        ) -> ActionableResearchDecision:
            assert decision_time == DECISION_TIME
            assert allow_safe_no_trade is True
            return admitted[run_id]

        monkeypatch.setattr(
            "nexus.paper.agent_cycle.load_actionable_research_output", fake_load_actionable
        )
        monkeypatch.setattr(
            "nexus.paper.agent_decision_store.load_actionable_research_output",
            fake_load_actionable,
        )
        async with sessions.begin() as session:
            completed = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
        assert completed.status == "awaiting_market", completed.result_code
        assert completed.strategy_decision_id is not None
        assert completed.batch_id is not None
        assert len(completed.order_ids) == 1
        assert completed.replayed is False

        window_start = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)
        async with sessions() as session:
            legacy_scope = await resolve_agent_paper_market_scope(
                session, cycle_id=completed.cycle_id
            )
        assert legacy_scope.execution_source_policy_id == PAPER_EXECUTION_SOURCE_POLICY_ID
        assert legacy_scope.window_start == window_start
        assert legacy_scope.window_end == window_start + timedelta(minutes=5)

        async with sessions.begin() as session:
            replayed = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
            assert replayed == completed.model_copy(update={"replayed": True})

        evaluated_at = window_start + timedelta(minutes=25, seconds=2)
        market = await collect_agent_paper_window(
            sessions,
            cycle_id=completed.cycle_id,
            instrument_id=INSTRUMENTS[0][0],
            window_start=window_start,
            provider=_IexProvider(),
            object_store=_ObjectStore(),
            evaluated_at=evaluated_at,
        )
        assert market.purpose == "paper_fill"
        async with sessions.begin() as session:
            advanced = await advance_internal_paper_order(
                session,
                order_id=completed.order_ids[0],
                evaluated_at=evaluated_at,
                idempotency_key="agent-paper-first-fill",
            )
        assert advanced.action == "partially_filled"
        async with sessions() as session:
            historical_service = PaperReadService(session, evaluated_at - timedelta(seconds=1))
            historical = await historical_service.latest_agent_cycle()
            historical_page = await historical_service.agent_cycles(
                decision_session=DECISION_TIME.date(),
                limit=1,
            )
            projected = await PaperReadService(session, evaluated_at).latest_agent_cycle()
        assert historical is not None
        assert historical.status == "awaiting_market"
        assert historical_page.version_observation is not None
        assert historical_page.version_history == (historical_page.version_observation,)
        assert historical_page.version_observation.order_count == 1
        assert historical_page.version_observation.fill_count == 0
        assert "VERSION_ORDER_PATH_UNOBSERVED" in (historical_page.version_observation.reason_codes)
        assert projected is not None
        assert projected.status == "partially_filled"
        assert projected.result_code == "PAPER_ORDERS_PARTIALLY_FILLED"
        second_window_start = window_start + timedelta(minutes=5)
        second_evaluated_at = evaluated_at + timedelta(minutes=5)
        await collect_agent_paper_window(
            sessions,
            cycle_id=completed.cycle_id,
            instrument_id=INSTRUMENTS[0][0],
            window_start=second_window_start,
            provider=_IexProvider(),
            object_store=_ObjectStore(),
            evaluated_at=second_evaluated_at,
        )
        async with sessions.begin() as session:
            final_fill = await advance_internal_paper_order(
                session,
                order_id=completed.order_ids[0],
                evaluated_at=second_evaluated_at,
                idempotency_key="agent-paper-final-fill",
            )
        assert final_fill.action == "filled"
        async with sessions() as session:
            read_service = PaperReadService(session, second_evaluated_at)
            projected = await read_service.latest_agent_cycle()
            cycle_page = await read_service.agent_cycles(
                decision_session=DECISION_TIME.date(),
                limit=1,
            )
        assert projected is not None
        assert projected.status == "completed"
        assert projected.result_code == "PAPER_CYCLE_COMPLETED"
        assert cycle_page.summary.cycle_count == 1
        assert cycle_page.summary.completed_count == 1
        assert cycle_page.version_observation is not None
        assert cycle_page.version_history == (cycle_page.version_observation,)
        assert cycle_page.version_observation.prompt_version == "agent-paper-orchestrator-v1"
        assert cycle_page.version_observation.model_id == "fixture-agent-paper"
        assert cycle_page.version_observation.model_version == "codex-sdk-chatgpt-v4-actionable"
        assert cycle_page.version_observation.observed_sessions == 1
        assert cycle_page.version_observation.cycle_count == 1
        assert cycle_page.version_observation.completed_count == 1
        assert cycle_page.version_observation.unavailable_count == 0
        assert cycle_page.version_observation.order_count == 1
        assert cycle_page.version_observation.fill_count == 2
        assert cycle_page.version_observation.challenger_available is False
        assert cycle_page.version_observation.lifecycle_state == "incumbent_frozen"
        assert cycle_page.version_observation.verdict == "continue_observing"
        assert cycle_page.version_observation.reason_codes == (
            "VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE",
            "VERSION_CHALLENGER_UNAVAILABLE",
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_agent_version_history_groups_exact_identity_and_keeps_latest_twenty(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(
                session,
                registered_decision=registered.decision,
            )

        async with sessions.begin() as session:
            started = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-version-history-source",
            )
        assert len(started.research_run_ids) == 1
        source_run_id = started.research_run_ids[0]

        expected_latest_prompt_hash = ""
        expected_latest_model_hash = ""
        async with sessions.begin() as session:
            for index in range(1, 22):
                identity_index = min(index, 20)
                prompt_hash = sha256(
                    f"agent-paper-history-prompt-{identity_index}".encode()
                ).hexdigest()
                model_hash = sha256(
                    f"agent-paper-history-model-{identity_index}".encode()
                ).hexdigest()
                if index == 21:
                    expected_latest_prompt_hash = prompt_hash
                    expected_latest_model_hash = model_hash
                prompt_id = UUID(int=0xF4000000000000000000000000000100 + index)
                model_id = UUID(int=0xF4000000000000000000000000000200 + index)
                cycle_id = UUID(int=0xF4000000000000000000000000000300 + index)
                run_id = UUID(int=0xF4000000000000000000000000000400 + index)
                decision_time = DECISION_TIME + timedelta(minutes=5 * index)
                session.add(
                    AgentPromptVersionRecord(
                        id=prompt_id,
                        role="orchestrator",
                        version=f"history-prompt-label-{index}",
                        content_hash=prompt_hash,
                        content=f"history prompt label {index}",
                    )
                )
                session.add(
                    AgentModelConfigRecord(
                        id=model_id,
                        model_id=f"history-model-label-{index}",
                        version="history-config-v1",
                        config_hash=model_hash,
                        max_input_tokens=100,
                        max_tool_calls=1,
                        max_output_tokens=100,
                        input_usd_per_million=Decimal("1"),
                        output_usd_per_million=Decimal("1"),
                        pricing_available_at=DECISION_TIME - timedelta(days=1),
                        pricing_expires_at=DECISION_TIME + timedelta(days=2),
                    )
                )
                await session.flush()
                await session.execute(
                    text(
                        """
                        INSERT INTO agent_paper_cycles (
                            id, account_id, portfolio_id, decision_session,
                            decision_time, decision_slot_at, execution_session,
                            universe_snapshot_id, quality_report_id,
                            decision_price_policy_id, execution_price_policy_id,
                            candidate_manifest, candidate_hash, sizing_policy_version,
                            sizing_policy_hash, execution_locked, created_at
                        )
                        SELECT
                            :cycle_id, account_id, portfolio_id, decision_session,
                            :decision_time, :decision_time, execution_session,
                            universe_snapshot_id, quality_report_id,
                            decision_price_policy_id, execution_price_policy_id,
                            candidate_manifest, candidate_hash, sizing_policy_version,
                            sizing_policy_hash, execution_locked, created_at
                        FROM agent_paper_cycles
                        WHERE id = :source_cycle_id
                        """
                    ),
                    {
                        "cycle_id": cycle_id,
                        "decision_time": decision_time,
                        "source_cycle_id": started.cycle_id,
                    },
                )
                await session.execute(
                    text(
                        """
                        INSERT INTO agent_research_runs (
                            id, instrument_id, data_snapshot_id, prompt_version_id,
                            model_config_id, decision_time, actor_id, idempotency_key,
                            status, retry_count, schema_hash, model_hash, prompt_hash,
                            prompt_manifest, evidence_hash, evidence_manifest, tool_hash,
                            input_hash, reserved_cost_usd, settled_cost_usd,
                            pricing_version, input_tokens, output_tokens, created_at
                        )
                        SELECT
                            :run_id, instrument_id, data_snapshot_id, :prompt_id,
                            :model_id, :decision_time, actor_id, :idempotency_key,
                            status, retry_count, schema_hash, :model_hash, :prompt_hash,
                            prompt_manifest, evidence_hash, evidence_manifest, tool_hash,
                            input_hash, reserved_cost_usd, settled_cost_usd,
                            pricing_version, input_tokens, output_tokens, created_at
                        FROM agent_research_runs
                        WHERE id = :source_run_id
                        """
                    ),
                    {
                        "run_id": run_id,
                        "prompt_id": prompt_id,
                        "model_id": model_id,
                        "decision_time": decision_time,
                        "idempotency_key": f"agent-paper-version-history-run-{index}",
                        "model_hash": model_hash,
                        "prompt_hash": prompt_hash,
                        "source_run_id": source_run_id,
                    },
                )
                await session.execute(
                    text(
                        """
                        INSERT INTO agent_paper_cycle_events (
                            cycle_id, sequence, from_status, to_status, result_code,
                            fact_ids, event_hash, occurred_at, created_at
                        )
                        SELECT
                            :cycle_id, sequence, from_status, to_status, result_code,
                            CASE
                                WHEN result_code = 'RESEARCH_JOBS_PREPARED'
                                THEN jsonb_build_object(
                                    'research_run_ids',
                                    jsonb_build_array(CAST(:run_id AS text))
                                )
                                ELSE fact_ids
                            END,
                            event_hash, :decision_time, created_at
                        FROM agent_paper_cycle_events
                        WHERE cycle_id = :source_cycle_id
                        ORDER BY sequence
                        """
                    ),
                    {
                        "cycle_id": cycle_id,
                        "run_id": str(run_id),
                        "decision_time": decision_time,
                        "source_cycle_id": started.cycle_id,
                    },
                )

        async with sessions() as session:
            page = await PaperReadService(
                session,
                datetime(2099, 1, 1, tzinfo=UTC),
            ).agent_cycles(decision_session=DECISION_TIME.date(), limit=1)

        assert page.version_observation is not None
        assert page.version_observation == page.version_history[0]
        assert len(page.version_history) == 20
        assert len({(item.prompt_hash, item.model_hash) for item in page.version_history}) == 20
        assert [item.last_decision_at for item in page.version_history] == sorted(
            (item.last_decision_at for item in page.version_history),
            reverse=True,
        )
        assert page.version_history[0].prompt_hash == expected_latest_prompt_hash
        assert page.version_history[0].model_hash == expected_latest_model_hash
        assert page.version_history[0].prompt_version == "history-prompt-label-21"
        assert page.version_history[0].model_id == "history-model-label-21"
        assert page.version_history[0].cycle_count == 2
        assert page.version_history[0].lifecycle_state == "incumbent_frozen"
        assert page.version_history[0].verdict == "continue_observing"
        assert page.version_history[-1].prompt_version == "history-prompt-label-1"
        assert all(
            item.lifecycle_state == "historical_frozen"
            and item.verdict == "retained_as_frozen_history"
            and item.reason_codes
            == (
                "VERSION_FROZEN_AUDIT_HISTORY",
                "VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE",
                "VERSION_ORDER_PATH_UNOBSERVED",
                "VERSION_SEQUENTIAL_HISTORY_NOT_COMPARABLE",
            )
            for item in page.version_history[1:]
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mixed_terminal_research_jobs_stop_without_replacement_runs(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(session, registered_decision=registered.decision)
        async with sessions.begin() as session:
            first = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-terminal-failure",
            )
        assert first.status == "researching"

        async with sessions.begin() as session:
            jobs = tuple(await session.scalars(select(Job).order_by(Job.id)))
            await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
            try:
                for index, job in enumerate(jobs):
                    status = "succeeded" if index == 0 else "dead_letter"
                    await session.execute(
                        text(
                            "UPDATE jobs SET status=:status,started_at=:at,finished_at=:at,"
                            "attempt_count=1,version=version+1 WHERE id=:job_id"
                        ),
                        {"status": status, "at": DECISION_TIME, "job_id": job.id},
                    )
            finally:
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
        async with sessions.begin() as session:
            stopped = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-terminal-failure",
            )
            run_count = await session.scalar(
                select(func.count()).select_from(AgentResearchRunRecord)
            )
            job_count = await session.scalar(select(func.count()).select_from(Job))
        assert stopped.status == "unavailable"
        assert stopped.result_code == "RESEARCH_OUTPUT_NOT_ACTIONABLE"
        assert stopped.research_run_ids == first.research_run_ids
        assert run_count == 1
        assert job_count == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_shutdown_confirmation_reads_back_exact_trigger_slot_from_postgres(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(session, registered_decision=registered.decision)
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=date(2026, 8, 28),
                    decision_at=datetime(2026, 8, 28, 21, tzinfo=UTC),
                )
            )
        async with sessions.begin() as session:
            created = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="shutdown-confirm-trigger",
            )
        assert created.status == "researching"

        inspection_time = DECISION_TIME + timedelta(seconds=1)
        monkeypatch.setattr(
            agent_paper_loop,
            "func",
            SimpleNamespace(clock_timestamp=lambda: literal(inspection_time), max=func.max),
        )
        before = agent_paper_loop.AgentPaperLoopSnapshot(
            now=DECISION_TIME,
            calendar_authority_id=CALENDAR_ID,
        )

        assert await agent_paper_loop.confirm_agent_paper_shutdown(
            EngineSettings(database_url=migrated_database_url),
            account_id=seed.account_id,
            expected_calendar_authority_id=CALENDAR_ID,
            action=agent_paper_loop.AgentPaperLoopAction.TRIGGER,
            before_snapshot=before,
            action_result=None,
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_shutdown_confirmation_rejects_newer_performance_close_when_original_missing(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    newer_close = await seed_second_paper_close(
        migrated_database_url,
        execution=seed.execution,
        close=Decimal("112"),
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=date(2026, 8, 31),
                    decision_at=datetime(2026, 8, 31, 21, tzinfo=UTC),
                )
            )
            monkeypatch.setattr(
                "nexus.paper.performance.current_paper_performance_price_authority_available",
                lambda: True,
            )
            await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=newer_close,
            )
        monkeypatch.setattr(
            agent_paper_loop,
            "func",
            SimpleNamespace(
                clock_timestamp=lambda: literal(newer_close + timedelta(seconds=1)),
                max=func.max,
            ),
        )
        before = agent_paper_loop.AgentPaperLoopSnapshot(
            now=seed.as_of,
            calendar_authority_id=CALENDAR_ID,
            performance_due_at=seed.as_of,
        )

        assert not await agent_paper_loop.confirm_agent_paper_shutdown(
            EngineSettings(database_url=migrated_database_url),
            account_id=seed.execution.account_id,
            expected_calendar_authority_id=CALENDAR_ID,
            action=agent_paper_loop.AgentPaperLoopAction.PUBLISH_PERFORMANCE,
            before_snapshot=before,
            action_result=None,
        )
    finally:
        await engine.dispose()
