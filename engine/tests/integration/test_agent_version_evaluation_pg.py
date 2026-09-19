"""PostgreSQL registration tests for isolated Agent-version evaluation."""

import asyncio
import hashlib
import json
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import nexus.jobs.daily_local as daily_local
import nexus.ops.agent_paper_once as agent_paper_once
import nexus.paper.agent_version_evaluation as agent_version_evaluation
import nexus.paper.agent_version_runner as agent_version_runner
import pytest
from helpers.paper_api import app, headers
from nexus.agents.codex_sdk import MODEL_ID
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSION, aggregate_input_limit
from nexus.agents.committee import ROLE_ORDER
from nexus.ops.agent_version_evaluation import EvaluationRequest, StatusResult, execute
from nexus.paper.agent_cycle_authority import CycleAuthority
from nexus.paper.agent_cycle_models import AgentPaperCycleResult, candidate_manifest_hash
from nexus.paper.agent_version_evaluation import (
    close_incomplete_agent_version_evaluation,
    close_unrecoverable_agent_version_evaluation,
    finalize_agent_version_evaluation,
    load_selected_internal_agent_version,
    register_agent_version_evaluation,
)
from nexus.paper.agent_version_models import (
    AgentVersionEvaluationRegistration,
    AgentVersionEvaluationRequest,
    AgentVersionSlotResult,
    evaluation_policy_hash,
)
from nexus.paper.agent_version_runner import (
    advance_agent_version_slot,
    resolve_internal_agent_version_source,
    resume_agent_version_slot,
)
from nexus.paper.performance import publish_paper_performance_snapshot
from nexus.paper.projection import load_paper_projection
from nexus.paper.read_service import PaperReadService
from nexus.persistence.models.agent_versions import (
    AgentVersionEvaluationDecisionRecord,
    AgentVersionEvaluationRecord,
)
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias, UniverseSnapshot
from nexus.persistence.models.paper import (
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperAccountRecord,
    PaperDrawdownLockEventRecord,
    PaperLedgerAccountRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderRecord,
)
from nexus.persistence.models.portfolio import Account, Portfolio
from nexus.persistence.models.runtime import Job
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.settings import EngineSettings
from sqlalchemy import func, literal, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
INSTRUMENT_ID = UUID("72000000-0000-0000-0000-000000000001")
SNAPSHOT_ID = UUID("72000000-0000-0000-0000-000000000002")
CALENDAR_ID = UUID("72000000-0000-0000-0000-000000000003")
INCUMBENT_MODEL_ID = UUID("72000000-0000-0000-0000-000000000004")
INCUMBENT_RUN_ID = UUID("72000000-0000-0000-0000-000000000006")

TRAIN_DATES = (date(2026, 10, 1), date(2026, 10, 2))
OOS_DATES = (
    date(2026, 10, 5),
    date(2026, 10, 6),
    date(2026, 10, 7),
    date(2026, 10, 8),
    date(2026, 10, 9),
    date(2026, 10, 12),
    date(2026, 10, 13),
    date(2026, 10, 14),
    date(2026, 10, 15),
    date(2026, 10, 16),
)


def _slots(
    dates: tuple[date, ...], minute_offset: int = 0, gap_minutes: int = 15
) -> tuple[datetime, ...]:
    return tuple(
        datetime.combine(day, time(14, minute, tzinfo=UTC))
        for day in dates
        for minute in (minute_offset, minute_offset + gap_minutes)
    )


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _prompt_id(prefix: int, ordinal: int) -> UUID:
    return UUID(f"72000000-0000-0000-0000-{prefix + ordinal:012d}")


async def _seed_authority(
    session: AsyncSession,
    *,
    symbol: str = "AAPL",
    alias_type: str = "ticker",
    alias_created_at: datetime | None = None,
    same_version: bool = False,
    model_version: str = ACTIONABLE_CONFIG_VERSION,
    runtime_model_id: str = MODEL_ID,
    model_max_input_tokens: int = aggregate_input_limit(ACTIONABLE_CONFIG_VERSION),
    model_max_output_tokens: int = 8192,
    model_input_price: Decimal = Decimal("0"),
    model_output_price: Decimal = Decimal("0"),
    pricing_expires_at: datetime = datetime(2027, 1, 1, tzinfo=UTC),
    omit_calendar_date: date | None = None,
    omit_prior_calendar_session: bool = False,
    corrupt_incumbent_prompt_hash: bool = False,
) -> tuple[UUID, UUID]:
    session.add_all(
        [
            Instrument(
                id=INSTRUMENT_ID,
                asset_type="common_stock",
                exchange="NASDAQ",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="large_cap",
            ),
            DataSnapshot(
                id=SNAPSHOT_ID,
                source="fixture",
                provider_object_id="agent-version-registration",
                revision="v1",
                observed_at=NOW,
                available_at=NOW,
                received_at=NOW,
                schema_version="v1",
                payload={},
            ),
            ExchangeCalendarAuthorityRecord(
                id=CALENDAR_ID,
                exchange="XNYS",
                version="agent-version-registration-v1",
                source="exchange_calendars",
                range_start=date(2026, 9, 30),
                range_end=OOS_DATES[-1],
                query_parameters={},
                completeness_status="complete",
                content_hash="c" * 64,
                published_at=NOW,
            ),
        ]
    )
    await session.flush()
    session.add(
        InstrumentAlias(
            instrument_id=INSTRUMENT_ID,
            symbol=symbol,
            alias_type=alias_type,
            valid_from=date(2020, 1, 1),
            valid_to=None,
            **({"created_at": alias_created_at} if alias_created_at is not None else {}),
        )
    )
    for session_date in (date(2026, 9, 30),) + TRAIN_DATES + OOS_DATES:
        if session_date == omit_calendar_date or (
            omit_prior_calendar_session and session_date == date(2026, 9, 30)
        ):
            continue
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=session_date,
                decision_at=datetime.combine(session_date, time(21, tzinfo=UTC)),
                market_open_at=datetime.combine(session_date, time(13, 30, tzinfo=UTC)),
                market_close_at=datetime.combine(session_date, time(20, tzinfo=UTC)),
            )
        )

    incumbent_manifest: dict[str, object] = {}
    challenger_manifest: dict[str, object] = {}
    for ordinal, role in enumerate(ROLE_ORDER, start=1):
        incumbent_content = f"incumbent {role}"
        challenger_content = incumbent_content if same_version else f"challenger {role}"
        incumbent_id = _prompt_id(100, ordinal)
        challenger_id = _prompt_id(200, ordinal)
        incumbent_hash = hashlib.sha256(incumbent_content.encode()).hexdigest()
        challenger_hash = hashlib.sha256(challenger_content.encode()).hexdigest()
        session.add(
            AgentPromptVersionRecord(
                id=incumbent_id,
                role=role,
                version="incumbent-v1",
                content_hash=incumbent_hash,
                content=incumbent_content,
            )
        )
        if not same_version:
            session.add(
                AgentPromptVersionRecord(
                    id=challenger_id,
                    role=role,
                    version="challenger-v1",
                    content_hash=challenger_hash,
                    content=challenger_content,
                )
            )
        incumbent_manifest[role] = {
            "id": str(incumbent_id),
            "version": "incumbent-v1",
            "content_hash": incumbent_hash,
        }
        challenger_manifest[role] = {
            "id": str(challenger_id),
            "version": "challenger-v1",
            "content_hash": challenger_hash,
        }

    model_hash = "a" * 64
    session.add(
        AgentModelConfigRecord(
            id=INCUMBENT_MODEL_ID,
            model_id=runtime_model_id,
            version=model_version,
            config_hash=model_hash,
            max_input_tokens=model_max_input_tokens,
            max_tool_calls=1,
            max_output_tokens=model_max_output_tokens,
            input_usd_per_million=model_input_price,
            output_usd_per_million=model_output_price,
            pricing_available_at=datetime(2026, 1, 1, tzinfo=UTC),
            pricing_expires_at=pricing_expires_at,
        )
    )
    await session.flush()
    session.add(
        AgentResearchRunRecord(
            id=INCUMBENT_RUN_ID,
            instrument_id=INSTRUMENT_ID,
            data_snapshot_id=SNAPSHOT_ID,
            prompt_version_id=_prompt_id(100, 1),
            model_config_id=INCUMBENT_MODEL_ID,
            decision_time=NOW,
            actor_id="agent-version-test",
            idempotency_key="incumbent-source",
            status="completed",
            retry_count=0,
            schema_hash="d" * 64,
            model_hash=model_hash,
            prompt_hash=("9" * 64 if corrupt_incumbent_prompt_hash else _hash(incumbent_manifest)),
            prompt_manifest=incumbent_manifest,
            evidence_hash="e" * 64,
            evidence_manifest=[{}],
            tool_hash="f" * 64,
            input_hash="1" * 64,
            reserved_cost_usd=Decimal("1"),
            settled_cost_usd=Decimal("0"),
            pricing_version=model_version,
            input_tokens=1,
            output_tokens=1,
        )
    )
    await session.flush()
    prompt_id = _prompt_id(100 if same_version else 200, 1)
    return prompt_id, INCUMBENT_MODEL_ID


def _request(
    prompt_id: UUID, model_id: UUID, *, minute_offset: int = 0
) -> AgentVersionEvaluationRequest:
    return AgentVersionEvaluationRequest(
        instrument_id=INSTRUMENT_ID,
        incumbent_version_source_run_id=INCUMBENT_RUN_ID,
        challenger_prompt_version_id=prompt_id,
        challenger_model_config_id=model_id,
        initial_capital=Decimal("10000"),
        train_slots=_slots(TRAIN_DATES, minute_offset),
        oos_slots=_slots(OOS_DATES, minute_offset),
    )


async def _factory(database_url: str) -> tuple[async_sessionmaker[AsyncSession], object]:
    engine = create_async_engine(database_url)
    return async_sessionmaker(engine, expire_on_commit=False), engine


async def _seed_legacy_invalid_evaluation(
    session: AsyncSession,
    *,
    omit_prior_calendar_session: bool = True,
    gap_minutes: int = 15,
) -> UUID:
    prompt_id, model_id = await _seed_authority(
        session,
        omit_prior_calendar_session=omit_prior_calendar_session,
    )
    incumbent_run = await session.get(AgentResearchRunRecord, INCUMBENT_RUN_ID)
    assert incumbent_run is not None
    evaluation_id = UUID("72000000-0000-0000-0000-000000000090")
    incumbent_account_id = UUID("72000000-0000-0000-0000-000000000091")
    challenger_account_id = UUID("72000000-0000-0000-0000-000000000092")
    incumbent_portfolio_id = UUID("72000000-0000-0000-0000-000000000093")
    challenger_portfolio_id = UUID("72000000-0000-0000-0000-000000000094")
    incumbent_paper_id = UUID("72000000-0000-0000-0000-000000000095")
    challenger_paper_id = UUID("72000000-0000-0000-0000-000000000096")
    session.add_all(
        (
            Account(
                id=incumbent_account_id,
                provider="internal_paper_evaluation",
                status="ACTIVE",
            ),
            Account(
                id=challenger_account_id,
                provider="internal_paper_evaluation",
                status="ACTIVE",
            ),
        )
    )
    await session.flush()
    session.add_all(
        (
            Portfolio(
                id=incumbent_portfolio_id,
                account_id=incumbent_account_id,
                name="Legacy invalid incumbent",
            ),
            Portfolio(
                id=challenger_portfolio_id,
                account_id=challenger_account_id,
                name="Legacy invalid challenger",
            ),
        )
    )
    await session.flush()
    session.add_all(
        (
            PaperAccountRecord(
                id=incumbent_paper_id,
                account_id=incumbent_account_id,
                portfolio_id=incumbent_portfolio_id,
                code="legacy-invalid-incumbent",
                currency="USD",
                account_model="cash",
                position_model="long_only",
                initial_capital=Decimal("10000"),
                capability="simulation_write_internal",
            ),
            PaperAccountRecord(
                id=challenger_paper_id,
                account_id=challenger_account_id,
                portfolio_id=challenger_portfolio_id,
                code="legacy-invalid-challenger",
                currency="USD",
                account_model="cash",
                position_model="long_only",
                initial_capital=Decimal("10000"),
                capability="simulation_write_internal",
            ),
        )
    )
    await session.flush()
    session.add(
        AgentVersionEvaluationRecord(
            id=evaluation_id,
            incumbent_account_id=incumbent_paper_id,
            challenger_account_id=challenger_paper_id,
            instrument_id=INSTRUMENT_ID,
            calendar_authority_id=CALENDAR_ID,
            incumbent_version_source_run_id=INCUMBENT_RUN_ID,
            challenger_prompt_version_id=prompt_id,
            challenger_model_config_id=model_id,
            incumbent_prompt_hash=incumbent_run.prompt_hash,
            incumbent_model_hash=incumbent_run.model_hash,
            challenger_prompt_hash="2" * 64,
            challenger_model_hash="a" * 64,
            train_slots=list(_slots(TRAIN_DATES, gap_minutes=gap_minutes)),
            oos_slots=list(_slots(OOS_DATES, gap_minutes=gap_minutes)),
            policy_version="agent_version_evaluation_v1",
            policy_hash=evaluation_policy_hash(),
            authority_hash="3" * 64,
        )
    )
    await session.flush()
    return evaluation_id


@pytest.mark.asyncio
async def test_registration_creates_equal_cash_accounts_and_no_external_facts(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            before = {
                table: int(await session.scalar(text(f"SELECT count(*) FROM {table}")) or 0)
                for table in (
                    "jobs",
                    "agent_paper_cycles",
                    "agent_research_runs",
                    "agent_research_outputs",
                    "portfolio_decision_runs",
                    "paper_orders",
                    "paper_fills",
                    "paper_ledger_entries",
                    "account_external_id_envelopes",
                    "broker_account_bindings",
                )
            }
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-v1",
                now=NOW,
            )
            assert registration.execution_locked is True
            assert registration.model_jobs_created == 0
            assert registration.replayed is False
            records = tuple(
                await session.scalars(
                    select(PaperAccountRecord).where(
                        PaperAccountRecord.id.in_(
                            (
                                registration.incumbent_account_id,
                                registration.challenger_account_id,
                            )
                        )
                    )
                )
            )
            assert len(records) == 2
            assert {record.initial_capital for record in records} == {Decimal("10000")}
            assert {record.capability for record in records} == {"simulation_write_internal"}
            providers = set(
                await session.scalars(
                    select(Account.provider)
                    .join(Portfolio, Portfolio.account_id == Account.id)
                    .join(PaperAccountRecord, PaperAccountRecord.portfolio_id == Portfolio.id)
                    .where(PaperAccountRecord.id.in_(record.id for record in records))
                )
            )
            assert providers == {"internal_paper_evaluation"}
            after = {
                table: int(await session.scalar(text(f"SELECT count(*) FROM {table}")) or 0)
                for table in before
            }
            assert after == before

        async with factory.begin() as session:
            replay = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-v1",
                now=NOW,
            )
            assert replay.model_copy(update={"replayed": False}) == registration
            assert replay.replayed is True
            assert (
                int(
                    await session.scalar(
                        select(func.count()).select_from(AgentVersionEvaluationRecord)
                    )
                    or 0
                )
                == 1
            )

        async with factory.begin() as session:
            authority_replay = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-v2",
                now=NOW,
            )
            assert authority_replay.evaluation_id == registration.evaluation_id
            assert authority_replay.replayed is True
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_daily_performance_targets_wait_for_evaluation_ledger_initialization(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-performance-target-v1",
                now=NOW,
            )
            evaluation_accounts = {
                registration.incumbent_account_id,
                registration.challenger_account_id,
            }

            before = set(await daily_local._performance_target_ids(session))
            assert evaluation_accounts.isdisjoint(before)

            evaluation = await session.get(
                AgentVersionEvaluationRecord,
                registration.evaluation_id,
            )
            assert evaluation is not None
            assert await agent_version_runner._ensure_equal_internal_accounts(
                session,
                evaluation=evaluation,
            )

            after = set(await daily_local._performance_target_ids(session))
            assert evaluation_accounts <= after
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_registered_evaluation_projects_as_locked_read_only_fact(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-readback-v1",
                now=NOW,
            )
        request_time = NOW
        async with factory() as session:
            visible_accounts = (
                await session.execute(
                    select(PaperAccountRecord.id, Account.provider)
                    .join(Portfolio, Portfolio.id == PaperAccountRecord.portfolio_id)
                    .join(Account, Account.id == Portfolio.account_id)
                )
            ).all()
            assert (
                sum(provider != "internal_paper_evaluation" for _, provider in visible_accounts)
                == 1
            )
            page = await PaperReadService(session, request_time).agent_cycles(
                decision_session=TRAIN_DATES[0],
                limit=20,
            )

        evaluation = page.version_evaluation
        assert evaluation is not None
        assert evaluation.evaluation_id == registration.evaluation_id
        assert evaluation.status == "observing"
        assert evaluation.train_completed == 0
        assert evaluation.oos_completed == 0
        assert evaluation.oos_missing == 20
        assert len(evaluation.slots) == 24
        assert tuple(slot.phase for slot in evaluation.slots[:4]) == ("train",) * 4
        assert tuple(slot.phase for slot in evaluation.slots[4:]) == ("oos",) * 20
        assert all(slot.status == "scheduled" for slot in evaluation.slots)
        assert all(slot.incumbent.status == "pending" for slot in evaluation.slots)
        assert all(slot.challenger.status == "pending" for slot in evaluation.slots)
        assert evaluation.slots[0].decision_slot_at == _request(prompt_id, model_id).train_slots[0]
        assert evaluation.slots[0].reason_code == "REGISTERED_SLOT_SCHEDULED"
        assert tuple(row.key for row in evaluation.comparisons) == (
            "hold_current",
            "baseline_v1",
            "market_benchmark",
        )
        assert evaluation.execution_locked is True
        assert evaluation.broker_write is False
        assert evaluation.live_write is False
        wire = evaluation.model_dump_json()
        assert str(registration.incumbent_account_id) not in wire
        assert str(registration.challenger_account_id) not in wire

        path = f"/internal/v1/paper/cycles?decision_session={TRAIN_DATES[0]}&limit=20"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url)),
            base_url="http://testserver",
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        api_evaluation = response.json()["data"]["version_evaluation"]
        assert api_evaluation["evaluation_id"] == str(registration.evaluation_id)
        assert api_evaluation["status"] == "observing"
        assert len(api_evaluation["slots"]) == 24
        assert api_evaluation["slots"][0]["status"] == "scheduled"
        assert api_evaluation["execution_locked"] is True
        assert api_evaluation["broker_write"] is False
        assert api_evaluation["live_write"] is False

        cli_status = await execute(
            EngineSettings(database_url=migrated_database_url),
            EvaluationRequest(
                action="status",
                evaluation_id=registration.evaluation_id,
            ),
        )
        assert isinstance(cli_status, StatusResult)
        assert cli_status.status == "observing"
        assert cli_status.execution_locked is True
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_read_projection_marks_terminal_lane_failure_for_attention(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    slot = _slots(TRAIN_DATES)[0]
    universe_id = UUID("72000000-0000-0000-0000-000000000701")
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-terminal-lane-readback-v1",
                now=NOW,
            )
            session.add(
                UniverseSnapshot(
                    id=universe_id,
                    key="us_liquid",
                    version="us_liquid_v1",
                    effective_from=TRAIN_DATES[0],
                    source="fosun_local_read_only",
                    content_hash="7" * 64,
                    published_at=NOW,
                )
            )
            accounts = {
                account.id: account
                for account in await session.scalars(
                    select(PaperAccountRecord).where(
                        PaperAccountRecord.id.in_(
                            (
                                registration.incumbent_account_id,
                                registration.challenger_account_id,
                            )
                        )
                    )
                )
            }
            for index, (account_id, status, result_code) in enumerate(
                (
                    (registration.incumbent_account_id, "failed", "AGENT_FAILED"),
                    (
                        registration.challenger_account_id,
                        "unavailable",
                        "AUTHORITY_UNAVAILABLE",
                    ),
                ),
                start=1,
            ):
                cycle_id = UUID(f"72000000-0000-0000-0000-{710 + index:012d}")
                session.add(
                    AgentPaperCycleRecord(
                        id=cycle_id,
                        account_id=account_id,
                        portfolio_id=accounts[account_id].portfolio_id,
                        decision_session=TRAIN_DATES[0],
                        decision_time=slot,
                        decision_slot_at=slot,
                        execution_session=TRAIN_DATES[0],
                        universe_snapshot_id=universe_id,
                        quality_report_id=None,
                        decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                        execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                        candidate_manifest=[],
                        candidate_hash=candidate_manifest_hash(()),
                        sizing_policy_version="agent_paper_sizing_v1",
                        sizing_policy_hash=f"{index}" * 64,
                        execution_locked=True,
                    )
                )
                await session.flush()
                session.add(
                    AgentPaperCycleEventRecord(
                        cycle_id=cycle_id,
                        sequence=1,
                        from_status=None,
                        to_status="prepared",
                        result_code="CYCLE_PREPARED",
                        fact_ids={},
                        event_hash=f"{index + 2}" * 64,
                        occurred_at=slot,
                    )
                )
                session.add(
                    AgentPaperCycleEventRecord(
                        cycle_id=cycle_id,
                        sequence=2,
                        from_status="prepared",
                        to_status=status,
                        result_code=result_code,
                        fact_ids={},
                        event_hash=f"{index + 4}" * 64,
                        occurred_at=slot + timedelta(seconds=1),
                    )
                )

        async with factory() as session:
            page = await PaperReadService(session, slot + timedelta(seconds=10)).agent_cycles(
                decision_session=TRAIN_DATES[0], limit=20
            )

        assert page.version_evaluation is not None
        first_slot = page.version_evaluation.slots[0]
        assert first_slot.status == "attention"
        assert first_slot.reason_code == "PAIR_LANE_TERMINAL_FAILURE"
        assert first_slot.incumbent.status == "failed"
        assert first_slot.challenger.status == "unavailable"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_finalization_is_fail_closed_and_idempotent_without_oos_pairs(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            request = _request(prompt_id, model_id)
            registration = await register_agent_version_evaluation(
                session,
                request=request,
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-finalization-v1",
                now=NOW,
            )

        final_close = datetime.combine(OOS_DATES[-1], time(20, tzinfo=UTC))
        with pytest.raises(ValueError, match="AGENT_VERSION_WINDOW_OPEN"):
            async with factory.begin() as session:
                await finalize_agent_version_evaluation(
                    session,
                    evaluation_id=registration.evaluation_id,
                    now=final_close - timedelta(seconds=1),
                )

        async with factory.begin() as session:
            finalized = await finalize_agent_version_evaluation(
                session,
                evaluation_id=registration.evaluation_id,
                now=final_close + timedelta(seconds=1),
            )
            selected = await load_selected_internal_agent_version(
                session,
                now=final_close + timedelta(seconds=1),
            )
            decision_count = int(
                await session.scalar(
                    select(func.count()).select_from(AgentVersionEvaluationDecisionRecord)
                )
                or 0
            )

        assert finalized.status == "UNKNOWN"
        assert finalized.reason_code == "OOS_AUTHORITY_INCOMPLETE"
        assert finalized.selected_version_source_run_id == INCUMBENT_RUN_ID
        assert finalized.execution_locked is True
        assert finalized.replayed is False
        assert selected is None
        assert decision_count == 1

        async with factory.begin() as session:
            replay = await finalize_agent_version_evaluation(
                session,
                evaluation_id=registration.evaluation_id,
                now=final_close + timedelta(minutes=1),
            )
            assert (
                int(
                    await session.scalar(
                        select(func.count()).select_from(AgentVersionEvaluationDecisionRecord)
                    )
                    or 0
                )
                == 1
            )
        assert replay.model_copy(update={"replayed": False}) == finalized
        assert replay.replayed is True
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_metric_facts_use_one_official_close_per_oos_session(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-session-close-metrics-v1",
                now=NOW,
            )
            evaluation = await session.get(
                AgentVersionEvaluationRecord,
                registration.evaluation_id,
            )
            assert evaluation is not None
            assert await agent_version_runner._ensure_equal_internal_accounts(
                session,
                evaluation=evaluation,
            )
            closes = tuple(datetime.combine(day, time(20, tzinfo=UTC)) for day in OOS_DATES)
            monkeypatch.setattr(
                "nexus.paper.performance.func",
                type(
                    "PerformanceFunctions",
                    (),
                    {
                        "max": staticmethod(func.max),
                        "clock_timestamp": staticmethod(
                            lambda: literal(closes[-1] + timedelta(seconds=1))
                        ),
                    },
                ),
            )
            for account_id in (
                registration.incumbent_account_id,
                registration.challenger_account_id,
            ):
                for close in closes:
                    await publish_paper_performance_snapshot(
                        session,
                        account_id=account_id,
                        as_of=close,
                        enqueue_session_summary=False,
                    )

            facts = await agent_version_evaluation._load_agent_version_metric_facts(
                session,
                evaluation=evaluation,
                now=closes[-1] + timedelta(seconds=1),
            )

        assert facts.cost_authority_complete is True
        assert facts.cumulative_excess_return == Decimal("0")
        assert facts.positive_excess_session_count == 0
        assert facts.authority_complete is False
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_evaluation_without_prior_session_can_only_be_invalidated_before_work(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        async with factory.begin() as session:
            evaluation_id = await _seed_legacy_invalid_evaluation(session)

        async with factory.begin() as session:
            invalidated = await agent_version_evaluation.invalidate_agent_version_evaluation(
                session,
                evaluation_id=evaluation_id,
                now=NOW,
            )
            decision_count = int(
                await session.scalar(
                    select(func.count()).select_from(AgentVersionEvaluationDecisionRecord)
                )
                or 0
            )

        assert invalidated.status == "UNKNOWN"
        assert invalidated.reason_code == "CALENDAR_PRIOR_SESSION_MISSING"
        assert invalidated.selected_version_source_run_id == INCUMBENT_RUN_ID
        assert invalidated.execution_locked is True
        assert invalidated.replayed is False
        assert decision_count == 1

        async with factory.begin() as session:
            replay = await agent_version_evaluation.invalidate_agent_version_evaluation(
                session,
                evaluation_id=evaluation_id,
                now=NOW + timedelta(minutes=1),
            )
        assert replay.model_copy(update={"replayed": False}) == invalidated
        assert replay.replayed is True
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_valid_open_evaluation_cannot_use_legacy_invalidation(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-valid-invalidation-v1",
                now=NOW,
            )

        with pytest.raises(ValueError, match="AGENT_VERSION_EVALUATION_NOT_INVALID"):
            async with factory.begin() as session:
                await agent_version_evaluation.invalidate_agent_version_evaluation(
                    session,
                    evaluation_id=registration.evaluation_id,
                    now=NOW,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_unsafe_schedule_can_only_be_invalidated_before_work(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        async with factory.begin() as session:
            evaluation_id = await _seed_legacy_invalid_evaluation(
                session,
                omit_prior_calendar_session=False,
                gap_minutes=5,
            )

        async with factory.begin() as session:
            invalidated = await agent_version_evaluation.invalidate_agent_version_evaluation(
                session,
                evaluation_id=evaluation_id,
                now=NOW,
            )

        assert invalidated.status == "UNKNOWN"
        assert invalidated.reason_code == "EVALUATION_SCHEDULE_UNSAFE"
        assert invalidated.selected_version_source_run_id == INCUMBENT_RUN_ID
        assert invalidated.execution_locked is True
        assert invalidated.replayed is False
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_missed_registered_slot_closes_evaluation_without_replaying_work(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    first_slot = _slots(TRAIN_DATES)[0]
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-missed-slot-v1",
                now=NOW,
            )

        async with factory.begin() as session:
            closed = await close_incomplete_agent_version_evaluation(
                session,
                evaluation_id=registration.evaluation_id,
                now=first_slot + timedelta(seconds=61),
            )
            cycle_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(AgentPaperCycleRecord)
                    .where(
                        AgentPaperCycleRecord.account_id.in_(
                            (
                                registration.incumbent_account_id,
                                registration.challenger_account_id,
                            )
                        )
                    )
                )
                or 0
            )

        assert closed.status == "UNKNOWN"
        assert closed.reason_code == "REGISTERED_SLOT_MISSED"
        assert closed.selected_version_source_run_id == INCUMBENT_RUN_ID
        assert closed.execution_locked is True
        assert cycle_count == 0

        async with factory.begin() as session:
            replay = await close_incomplete_agent_version_evaluation(
                session,
                evaluation_id=registration.evaluation_id,
                now=first_slot + timedelta(seconds=62),
            )
        assert replay.model_copy(update={"replayed": False}) == closed
        assert replay.replayed is True

        with pytest.raises(ValueError, match="AGENT_VERSION_EVALUATION_CLOSED"):
            async with factory.begin() as session:
                await advance_agent_version_slot(
                    session,
                    evaluation_id=registration.evaluation_id,
                    decision_slot_at=first_slot,
                    daily_limit_usd=Decimal("3"),
                    now=first_slot + timedelta(seconds=62),
                )
        with pytest.raises(ValueError, match="AGENT_VERSION_EVALUATION_CLOSED"):
            async with factory.begin() as session:
                await resume_agent_version_slot(
                    session,
                    evaluation_id=registration.evaluation_id,
                    decision_slot_at=first_slot,
                    now=first_slot + timedelta(seconds=62),
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_incomplete_evaluation_cannot_close_before_first_slot_is_missed(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    first_slot = _slots(TRAIN_DATES)[0]
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-not-yet-missed-slot-v1",
                now=NOW,
            )

        with pytest.raises(ValueError, match="AGENT_VERSION_SLOT_NOT_MISSED"):
            async with factory.begin() as session:
                await close_incomplete_agent_version_evaluation(
                    session,
                    evaluation_id=registration.evaluation_id,
                    now=first_slot + timedelta(seconds=60),
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_known_shared_market_preparation_failure_closes_with_exact_reason(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    first_slot = _slots(TRAIN_DATES)[0]
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-market-preparation-failure-v1",
                now=NOW,
            )

        async with factory.begin() as session:
            closed = await close_unrecoverable_agent_version_evaluation(
                session,
                evaluation_id=registration.evaluation_id,
                decision_slot_at=first_slot,
                reason_code="PAIR_SCHWAB_L1_INCOMPLETE",
                now=first_slot + timedelta(seconds=1),
            )

        assert closed.status == "UNKNOWN"
        assert closed.reason_code == "PAIR_SCHWAB_L1_INCOMPLETE"
        assert closed.execution_locked is True
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_first_slot_initialization_replays_without_work_and_rejects_corruption(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    slot = _slots(TRAIN_DATES)[0]
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-runner-v1",
                now=NOW,
            )

        async with factory.begin() as session:
            first = await advance_agent_version_slot(
                session,
                evaluation_id=registration.evaluation_id,
                decision_slot_at=slot,
                daily_limit_usd=Decimal("3"),
                now=slot,
            )
            incumbent = await load_paper_projection(
                session,
                account_id=registration.incumbent_account_id,
                as_of=slot,
            )
            challenger = await load_paper_projection(
                session,
                account_id=registration.challenger_account_id,
                as_of=slot,
            )
            counts = {
                "ledger_accounts": int(
                    await session.scalar(select(func.count()).select_from(PaperLedgerAccountRecord))
                    or 0
                ),
                "ledger_transactions": int(
                    await session.scalar(
                        select(func.count()).select_from(PaperLedgerTransactionRecord)
                    )
                    or 0
                ),
                "ledger_entries": int(
                    await session.scalar(select(func.count()).select_from(PaperLedgerEntryRecord))
                    or 0
                ),
                "drawdown_events": int(
                    await session.scalar(
                        select(func.count()).select_from(PaperDrawdownLockEventRecord)
                    )
                    or 0
                ),
            }

        assert first.status == "incomparable"
        assert first.reason_code == "PAIR_AUTHORITY_UNAVAILABLE"
        assert first.phase == "train"
        assert first.execution_locked is True
        assert first.replayed is False
        assert counts == {
            "ledger_accounts": 21,
            "ledger_transactions": 3,
            "ledger_entries": 6,
            "drawdown_events": 3,
        }
        assert incumbent.available_cash == challenger.available_cash == Decimal("10000")
        assert incumbent.reserved_cash == challenger.reserved_cash == Decimal("0")
        assert incumbent.positions == challenger.positions == ()
        assert incumbent.open_orders == challenger.open_orders == ()

        async with factory.begin() as session:
            replay = await advance_agent_version_slot(
                session,
                evaluation_id=registration.evaluation_id,
                decision_slot_at=slot,
                daily_limit_usd=Decimal("3"),
                now=slot,
            )
            replay_counts = {
                "ledger_accounts": int(
                    await session.scalar(select(func.count()).select_from(PaperLedgerAccountRecord))
                    or 0
                ),
                "ledger_transactions": int(
                    await session.scalar(
                        select(func.count()).select_from(PaperLedgerTransactionRecord)
                    )
                    or 0
                ),
                "ledger_entries": int(
                    await session.scalar(select(func.count()).select_from(PaperLedgerEntryRecord))
                    or 0
                ),
                "drawdown_events": int(
                    await session.scalar(
                        select(func.count()).select_from(PaperDrawdownLockEventRecord)
                    )
                    or 0
                ),
                "cycles": int(
                    await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord))
                    or 0
                ),
                "jobs": int(await session.scalar(select(func.count()).select_from(Job)) or 0),
                "orders": int(
                    await session.scalar(select(func.count()).select_from(PaperOrderRecord)) or 0
                ),
            }
        assert replay.model_copy(update={"replayed": False}) == first
        assert replay.replayed is True
        assert replay_counts == {**counts, "cycles": 0, "jobs": 0, "orders": 0}

        async with factory.begin() as session:
            resumed = await resume_agent_version_slot(
                session,
                evaluation_id=registration.evaluation_id,
                decision_slot_at=slot,
                now=slot,
            )
            incumbent_source = await resolve_internal_agent_version_source(
                session,
                account_id=registration.incumbent_account_id,
                decision_slot_at=slot,
            )
            with pytest.raises(ValueError, match="AGENT_VERSION_SOURCE_UNAVAILABLE"):
                await resolve_internal_agent_version_source(
                    session,
                    account_id=registration.challenger_account_id,
                    decision_slot_at=slot,
                )
        assert resumed.status == "incomparable"
        assert resumed.reason_code == "PAIR_CYCLES_UNAVAILABLE"
        assert resumed.replayed is True
        assert incumbent_source.source_run_id == INCUMBENT_RUN_ID

        async with factory.begin() as session:
            original_reference_hash = await session.scalar(
                select(PaperLedgerTransactionRecord.reference_hash).where(
                    PaperLedgerTransactionRecord.account_id == registration.incumbent_account_id,
                    PaperLedgerTransactionRecord.transaction_type == "funding",
                    PaperLedgerTransactionRecord.reference_type == "initial_funding",
                )
            )
            assert original_reference_hash is not None
            await session.execute(
                text("ALTER TABLE paper_ledger_transactions DISABLE TRIGGER USER")
            )
            try:
                await session.execute(
                    text(
                        "UPDATE paper_ledger_transactions SET reference_hash=:corrupt_hash "
                        "WHERE account_id=:account_id AND transaction_type='funding' "
                        "AND reference_type='initial_funding'"
                    ),
                    {
                        "account_id": registration.incumbent_account_id,
                        "corrupt_hash": "f" * 64,
                    },
                )
            finally:
                await session.execute(
                    text("ALTER TABLE paper_ledger_transactions ENABLE TRIGGER USER")
                )

        async with factory.begin() as session:
            with pytest.raises(
                ValueError,
                match="AGENT_VERSION_ACCOUNT_INITIALIZATION_INVALID",
            ):
                await advance_agent_version_slot(
                    session,
                    evaluation_id=registration.evaluation_id,
                    decision_slot_at=slot,
                    daily_limit_usd=Decimal("3"),
                    now=slot,
                )

        async with factory.begin() as session:
            await session.execute(
                text("ALTER TABLE paper_ledger_transactions DISABLE TRIGGER USER")
            )
            await session.execute(
                text("ALTER TABLE paper_drawdown_lock_events DISABLE TRIGGER USER")
            )
            try:
                await session.execute(
                    text(
                        "UPDATE paper_ledger_transactions SET reference_hash=:reference_hash "
                        "WHERE account_id=:account_id AND transaction_type='funding' "
                        "AND reference_type='initial_funding'"
                    ),
                    {
                        "account_id": registration.incumbent_account_id,
                        "reference_hash": original_reference_hash,
                    },
                )
                await session.execute(
                    text(
                        "UPDATE paper_drawdown_lock_events SET event_seq=2 "
                        "WHERE account_id=:account_id AND event_seq=1"
                    ),
                    {"account_id": registration.incumbent_account_id},
                )
            finally:
                await session.execute(
                    text("ALTER TABLE paper_drawdown_lock_events ENABLE TRIGGER USER")
                )
                await session.execute(
                    text("ALTER TABLE paper_ledger_transactions ENABLE TRIGGER USER")
                )

        async with factory.begin() as session:
            with pytest.raises(
                ValueError,
                match="AGENT_VERSION_ACCOUNT_INITIALIZATION_INVALID",
            ):
                await advance_agent_version_slot(
                    session,
                    evaluation_id=registration.evaluation_id,
                    decision_slot_at=slot,
                    daily_limit_usd=Decimal("3"),
                    now=slot,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_slot_runner_wires_both_frozen_lanes_to_same_authority(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    slot = _slots(TRAIN_DATES)[0]
    prepared_at = slot + timedelta(seconds=30)
    calls: list[dict[str, object]] = []
    incumbent_cycle_id = UUID("72000000-0000-0000-0000-000000000801")
    challenger_cycle_id = UUID("72000000-0000-0000-0000-000000000802")
    incumbent_run_id = UUID("72000000-0000-0000-0000-000000000803")
    challenger_run_id = UUID("72000000-0000-0000-0000-000000000804")

    async def fake_start(_session: AsyncSession, **kwargs: object) -> AgentPaperCycleResult:
        calls.append(kwargs)
        incumbent = kwargs["version_lane"] == "incumbent"
        return AgentPaperCycleResult(
            cycle_id=incumbent_cycle_id if incumbent else challenger_cycle_id,
            status="researching",
            result_code="RESEARCH_JOBS_PREPARED",
            research_run_ids=(incumbent_run_id if incumbent else challenger_run_id,),
            replayed=False,
        )

    async def fake_validate(_session: AsyncSession, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(agent_version_runner, "start_schwab_agent_paper_cycle", fake_start)
    monkeypatch.setattr(agent_version_runner, "_validate_prepared_pair", fake_validate)
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            session.add(
                UniverseSnapshot(
                    id=UUID("72000000-0000-0000-0000-000000000805"),
                    key="us_liquid",
                    version="us_liquid_v1",
                    effective_from=TRAIN_DATES[0],
                    source="fosun_local_read_only",
                    content_hash="8" * 64,
                    published_at=NOW,
                )
            )
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-runner-wiring-v1",
                now=NOW,
            )

        async with factory.begin() as session:
            result = await advance_agent_version_slot(
                session,
                evaluation_id=registration.evaluation_id,
                decision_slot_at=slot,
                daily_limit_usd=Decimal("3"),
                now=prepared_at,
            )

        assert result.status == "running"
        assert result.reason_code == "PAIR_JOBS_PREPARED"
        assert result.incumbent_cycle_id == incumbent_cycle_id
        assert result.challenger_cycle_id == challenger_cycle_id
        assert result.incumbent_run_id == incumbent_run_id
        assert result.challenger_run_id == challenger_run_id
        assert result.replayed is False
        assert [call["version_lane"] for call in calls] == ["incumbent", "challenger"]
        assert calls[0]["version_source_run_id"] == INCUMBENT_RUN_ID
        assert calls[0].get("version_prompt_id") is None
        assert calls[1].get("version_source_run_id") is None
        assert calls[1]["version_prompt_id"] == prompt_id
        assert calls[1]["version_model_id"] == model_id
        incumbent_authority = calls[0]["authority"]
        challenger_authority = calls[1]["authority"]
        assert isinstance(incumbent_authority, CycleAuthority)
        assert isinstance(challenger_authority, CycleAuthority)
        assert incumbent_authority.calendar_authority_id == CALENDAR_ID
        assert incumbent_authority.decision_time == prepared_at
        assert challenger_authority.decision_time == prepared_at
        assert challenger_authority.universe_snapshot_id == UUID(
            "72000000-0000-0000-0000-000000000805"
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_slot_preparation_replays_persisted_result_without_provider_call(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    slot = _slots(TRAIN_DATES)[0]
    universe_id = UUID("72000000-0000-0000-0000-000000000806")
    provider_calls = 0
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            session.add(
                UniverseSnapshot(
                    id=universe_id,
                    key="us_liquid",
                    version="us_liquid_v1",
                    effective_from=TRAIN_DATES[0],
                    source="fosun_local_read_only",
                    content_hash="8" * 64,
                    published_at=NOW,
                )
            )
            registration = await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-preparation-replay-v1",
                now=NOW,
            )
            incumbent = await session.get(PaperAccountRecord, registration.incumbent_account_id)
            assert incumbent is not None

        async def prepare(*_args, **_kwargs) -> CycleAuthority:
            nonlocal provider_calls
            provider_calls += 1
            return CycleAuthority(
                account_id=registration.incumbent_account_id,
                portfolio_id=incumbent.portfolio_id,
                decision_session=TRAIN_DATES[0],
                decision_time=slot + timedelta(seconds=20),
                execution_session=TRAIN_DATES[0],
                calendar_authority_id=CALENDAR_ID,
                universe_snapshot_id=universe_id,
            )

        async def advance(*_args, **_kwargs) -> AgentVersionSlotResult:
            return AgentVersionSlotResult(
                evaluation_id=registration.evaluation_id,
                decision_slot_at=slot,
                phase="train",
                incumbent_cycle_id=UUID(int=801),
                challenger_cycle_id=UUID(int=802),
                incumbent_run_id=UUID(int=803),
                challenger_run_id=UUID(int=804),
                status="running",
                reason_code="PAIR_JOBS_PREPARED",
                replayed=False,
            )

        monkeypatch.setattr(
            agent_paper_once, "S3ProviderEvidenceStore", lambda *_args, **_kwargs: object()
        )
        monkeypatch.setattr(agent_paper_once, "prepare_schwab_decision_authority", prepare)
        monkeypatch.setattr(agent_paper_once, "advance_agent_version_slot", advance)
        settings = agent_paper_once.AgentPaperTriggerSettings(
            database_url=migrated_database_url,
            object_store_endpoint="http://127.0.0.1:9000",
            alpaca_key="key",
            alpaca_secret="secret",
        )

        first = await agent_paper_once.prepare_agent_version_slot_once(
            settings,
            evaluation_id=registration.evaluation_id,
            decision_slot_at=slot,
            repo_root=agent_paper_once.Path.cwd(),
            expected_calendar_authority_id=CALENDAR_ID,
        )
        assert first.status == "running"

        async with factory.begin() as session:
            session.add(
                AgentPaperCycleRecord(
                    id=UUID("72000000-0000-0000-0000-000000000807"),
                    account_id=registration.incumbent_account_id,
                    portfolio_id=incumbent.portfolio_id,
                    decision_session=TRAIN_DATES[0],
                    decision_time=slot + timedelta(seconds=20),
                    decision_slot_at=slot,
                    execution_session=TRAIN_DATES[0],
                    universe_snapshot_id=universe_id,
                    quality_report_id=None,
                    decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                    execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                    candidate_manifest=[],
                    candidate_hash=candidate_manifest_hash(()),
                    sizing_policy_version="agent_paper_sizing_v1",
                    sizing_policy_hash="1" * 64,
                    execution_locked=True,
                )
            )

        replay = await agent_paper_once.prepare_agent_version_slot_once(
            settings,
            evaluation_id=registration.evaluation_id,
            decision_slot_at=slot,
            repo_root=agent_paper_once.Path.cwd(),
            expected_calendar_authority_id=CALENDAR_ID,
        )

        assert replay.model_copy(update={"replayed": False}) == first
        assert replay.replayed is True
        assert provider_calls == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_registration_rejects_idempotency_conflict_and_second_open_evaluation(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            await register_agent_version_evaluation(
                session,
                request=_request(prompt_id, model_id),
                actor_id="test-agent-version-evaluation",
                idempotency_key="register-v1",
                now=NOW,
            )
        with pytest.raises(IdempotencyConflict):
            async with factory.begin() as session:
                await register_agent_version_evaluation(
                    session,
                    request=_request(prompt_id, model_id, minute_offset=1),
                    actor_id="test-agent-version-evaluation",
                    idempotency_key="register-v1",
                    now=NOW,
                )
        with pytest.raises(ValueError, match="AGENT_VERSION_EVALUATION_ALREADY_OPEN"):
            async with factory.begin() as session:
                await register_agent_version_evaluation(
                    session,
                    request=_request(prompt_id, model_id, minute_offset=1),
                    actor_id="test-agent-version-evaluation",
                    idempotency_key="register-v2",
                    now=NOW,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_registration_creates_only_one_open_evaluation(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)

        async def register(minute_offset: int) -> AgentVersionEvaluationRegistration | str:
            try:
                async with factory.begin() as session:
                    return await register_agent_version_evaluation(
                        session,
                        request=_request(prompt_id, model_id, minute_offset=minute_offset),
                        actor_id="test-agent-version-evaluation",
                        idempotency_key=f"concurrent-{minute_offset}",
                        now=NOW,
                    )
            except ValueError as error:
                return str(error)

        results = await asyncio.gather(register(0), register(1))
        assert (
            sum(isinstance(result, AgentVersionEvaluationRegistration) for result in results) == 1
        )
        assert (
            sum("AGENT_VERSION_EVALUATION_ALREADY_OPEN" in str(result) for result in results) == 1
        )
        async with factory() as session:
            assert (
                int(
                    await session.scalar(
                        select(func.count()).select_from(AgentVersionEvaluationRecord)
                    )
                    or 0
                )
                == 1
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("seed_changes", "reason"),
    [
        (
            {"model_version": "codex-sdk-strategy-rnd-v1"},
            "AGENT_VERSION_MODEL_INCOMPATIBLE",
        ),
        (
            {"runtime_model_id": "gpt-5.6-luna"},
            "AGENT_VERSION_MODEL_INCOMPATIBLE",
        ),
        (
            {"model_max_input_tokens": 64000},
            "AGENT_VERSION_MODEL_INCOMPATIBLE",
        ),
        (
            {"model_max_output_tokens": 4096},
            "AGENT_VERSION_MODEL_INCOMPATIBLE",
        ),
        (
            {"model_input_price": Decimal("1")},
            "AGENT_VERSION_MODEL_INCOMPATIBLE",
        ),
        (
            {"model_output_price": Decimal("1")},
            "AGENT_VERSION_MODEL_INCOMPATIBLE",
        ),
        (
            {"corrupt_incumbent_prompt_hash": True},
            "AGENT_VERSION_INCUMBENT_INVALID",
        ),
        (
            {"omit_calendar_date": OOS_DATES[-1]},
            "AGENT_VERSION_CALENDAR_INVALID",
        ),
        (
            {"omit_prior_calendar_session": True},
            "AGENT_VERSION_CALENDAR_INVALID",
        ),
    ],
)
async def test_registration_rejects_unusable_incumbent_or_calendar(
    migrated_database_url: str,
    seed_changes: dict[str, object],
    reason: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        with pytest.raises(ValueError, match=reason):
            async with factory.begin() as session:
                prompt_id, model_id = await _seed_authority(session, **seed_changes)
                await register_agent_version_evaluation(
                    session,
                    request=_request(prompt_id, model_id),
                    actor_id="test-agent-version-evaluation",
                    idempotency_key="register-v1",
                    now=NOW,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "symbol",
        "alias_type",
        "alias_created_at",
        "same_version",
        "model_version",
        "pricing_expiry",
        "reason",
    ),
    [
        (
            "MSFT",
            "ticker",
            None,
            False,
            ACTIONABLE_CONFIG_VERSION,
            datetime(2027, 1, 1, tzinfo=UTC),
            "AGENT_VERSION_INSTRUMENT_INVALID",
        ),
        (
            "AAPL",
            "cik",
            None,
            False,
            ACTIONABLE_CONFIG_VERSION,
            datetime(2027, 1, 1, tzinfo=UTC),
            "AGENT_VERSION_INSTRUMENT_INVALID",
        ),
        (
            "AAPL",
            "ticker",
            NOW.replace(day=1, month=10),
            False,
            ACTIONABLE_CONFIG_VERSION,
            datetime(2027, 1, 1, tzinfo=UTC),
            "AGENT_VERSION_INSTRUMENT_INVALID",
        ),
        (
            "AAPL",
            "ticker",
            None,
            True,
            ACTIONABLE_CONFIG_VERSION,
            datetime(2027, 1, 1, tzinfo=UTC),
            "AGENT_VERSION_IDENTITY_UNCHANGED",
        ),
        (
            "AAPL",
            "ticker",
            None,
            False,
            "codex-sdk-strategy-rnd-v1",
            datetime(2027, 1, 1, tzinfo=UTC),
            "AGENT_VERSION_MODEL_INCOMPATIBLE",
        ),
        (
            "AAPL",
            "ticker",
            None,
            False,
            ACTIONABLE_CONFIG_VERSION,
            NOW,
            "AGENT_VERSION_PRICING_INVALID",
        ),
    ],
)
async def test_registration_fails_closed_on_invalid_authority(
    migrated_database_url: str,
    symbol: str,
    alias_type: str,
    alias_created_at: datetime | None,
    same_version: bool,
    model_version: str,
    pricing_expiry: datetime,
    reason: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        with pytest.raises(ValueError, match=reason):
            async with factory.begin() as session:
                prompt_id, model_id = await _seed_authority(
                    session,
                    symbol=symbol,
                    alias_type=alias_type,
                    alias_created_at=alias_created_at,
                    same_version=same_version,
                    model_version=model_version,
                    pricing_expires_at=pricing_expiry,
                )
                await register_agent_version_evaluation(
                    session,
                    request=_request(prompt_id, model_id),
                    actor_id="test-agent-version-evaluation",
                    idempotency_key="register-v1",
                    now=NOW,
                )
    finally:
        await engine.dispose()
