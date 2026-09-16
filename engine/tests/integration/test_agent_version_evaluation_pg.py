"""PostgreSQL registration tests for isolated Agent-version evaluation."""

import asyncio
import hashlib
import json
from datetime import UTC, date, datetime, time
from decimal import Decimal
from uuid import UUID

import nexus.paper.agent_version_runner as agent_version_runner
import pytest
from nexus.agents.codex_sdk import MODEL_ID
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSION, aggregate_input_limit
from nexus.agents.committee import ROLE_ORDER
from nexus.paper.agent_cycle_authority import CycleAuthority
from nexus.paper.agent_cycle_models import AgentPaperCycleResult
from nexus.paper.agent_version_evaluation import register_agent_version_evaluation
from nexus.paper.agent_version_models import (
    AgentVersionEvaluationRegistration,
    AgentVersionEvaluationRequest,
)
from nexus.paper.agent_version_runner import (
    advance_agent_version_slot,
    resolve_internal_agent_version_source,
    resume_agent_version_slot,
)
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.agent_versions import AgentVersionEvaluationRecord
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
from sqlalchemy import func, select, text
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


def _slots(dates: tuple[date, ...], minute_offset: int = 0) -> tuple[datetime, ...]:
    return tuple(
        datetime.combine(day, time(14, minute, tzinfo=UTC))
        for day in dates
        for minute in (minute_offset, minute_offset + 5)
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
                range_start=TRAIN_DATES[0],
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
    for session_date in TRAIN_DATES + OOS_DATES:
        if session_date == omit_calendar_date:
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
            prompt_hash=(
                "9" * 64 if corrupt_incumbent_prompt_hash else _hash(incumbent_manifest)
            ),
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


@pytest.mark.asyncio
async def test_registration_creates_equal_cash_accounts_and_no_external_facts(
    migrated_database_url: str,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    try:
        async with factory.begin() as session:
            prompt_id, model_id = await _seed_authority(session)
            before = {
                table: int(
                    await session.scalar(text(f"SELECT count(*) FROM {table}")) or 0
                )
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
                table: int(
                    await session.scalar(text(f"SELECT count(*) FROM {table}")) or 0
                )
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
            assert int(
                await session.scalar(select(func.count()).select_from(AgentVersionEvaluationRecord))
                or 0
            ) == 1

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
async def test_first_slot_initializes_equal_internal_accounts_without_work_replay(
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
                    await session.scalar(
                        select(func.count()).select_from(PaperLedgerAccountRecord)
                    )
                    or 0
                ),
                "ledger_transactions": int(
                    await session.scalar(
                        select(func.count()).select_from(PaperLedgerTransactionRecord)
                    )
                    or 0
                ),
                "ledger_entries": int(
                    await session.scalar(
                        select(func.count()).select_from(PaperLedgerEntryRecord)
                    )
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
                    await session.scalar(
                        select(func.count()).select_from(PaperLedgerAccountRecord)
                    )
                    or 0
                ),
                "ledger_transactions": int(
                    await session.scalar(
                        select(func.count()).select_from(PaperLedgerTransactionRecord)
                    )
                    or 0
                ),
                "ledger_entries": int(
                    await session.scalar(
                        select(func.count()).select_from(PaperLedgerEntryRecord)
                    )
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
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_slot_runner_wires_both_frozen_lanes_to_same_authority(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, engine = await _factory(migrated_database_url)
    slot = _slots(TRAIN_DATES)[0]
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
                now=slot,
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
        assert challenger_authority.universe_snapshot_id == UUID(
            "72000000-0000-0000-0000-000000000805"
        )
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
            sum(isinstance(result, AgentVersionEvaluationRegistration) for result in results)
            == 1
        )
        assert (
            sum(
                "AGENT_VERSION_EVALUATION_ALREADY_OPEN" in str(result)
                for result in results
            )
            == 1
        )
        async with factory() as session:
            assert int(
                await session.scalar(select(func.count()).select_from(AgentVersionEvaluationRecord))
                or 0
            ) == 1
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
