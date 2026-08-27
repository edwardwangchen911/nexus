"""Release migration checks for append-only proposal Shadow facts."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from helpers.migration_helpers import _table_names
from helpers.migration_schema_expectations import SHADOW_TABLES
from helpers.optimization import complete_comparison_metrics
from helpers.portfolio_api import (
    INSTRUMENT_ID,
    PORTFOLIOS,
    seed_portfolio_list,
    settings,
)
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    BaselineSelectionAuthorityRecord,
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
)
from nexus.persistence.models.shadow import (
    ShadowCheckpointMetricRecord,
    ShadowCheckpointRecord,
    ShadowGroupRecord,
    ShadowPortfolioRecord,
    ShadowSealRecord,
)
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from sqlalchemy import inspect, select, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

SHADOW_FUNCTIONS = {
    "nexus_date_array_strictly_increasing",
    "nexus_reject_shadow_fact_mutation",
    "nexus_shadow_group_complete",
    "nexus_shadow_checkpoint_complete",
    "nexus_shadow_unsealed",
    "nexus_shadow_seal_matches_lifecycle",
}

_BASELINE_ID = UUID("0198d500-1000-7000-8000-000000000041")
_OPTIMIZER_POLICY_ID = UUID("e690b91d-1e24-5de3-b2b2-566a463fca25")
_OBSERVED_AT = datetime(2026, 8, 24, 20, tzinfo=UTC)
_VARIANTS = ("hold_current", "minimum_change", "strategy_target")


@dataclass(frozen=True)
class _ProposalRoot:
    group_id: UUID
    proposal_ids: dict[str, UUID]
    calendar_id: UUID
    calendar_version: str
    sessions: tuple[date, ...]


@dataclass(frozen=True)
class _ShadowRoot:
    proposal: _ProposalRoot
    shadow_group_id: UUID
    portfolio_ids: dict[str, UUID]


async def _seed_proposal_root(session: AsyncSession, *, label: str) -> _ProposalRoot:
    data_snapshot = DataSnapshot(
        id=uuid4(),
        source="fixture",
        provider_object_id=f"shadow-migration-{label}",
        revision="1",
        observed_at=_OBSERVED_AT,
        available_at=_OBSERVED_AT,
        received_at=_OBSERVED_AT,
        schema_version="1",
        payload={},
    )
    report = await session.scalar(
        select(PortfolioDiagnosticReportRecord).where(
            PortfolioDiagnosticReportRecord.portfolio_snapshot_id == _BASELINE_ID,
            PortfolioDiagnosticReportRecord.policy_id == REAL_ADVISORY_POLICY_ID,
        )
    )
    if report is None:
        report = PortfolioDiagnosticReportRecord(
            id=uuid4(),
            portfolio_id=PORTFOLIOS[0],
            portfolio_snapshot_id=_BASELINE_ID,
            snapshot_as_of=_OBSERVED_AT,
            trading_day=date(2026, 8, 24),
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_version="real_advisory_policy_v1",
            optimization_need="OPTIMIZATION_CANDIDATE",
            diagnostic_input_hash="d" * 64,
        )
        session.add(report)
    selection = BaselineSelectionAuthorityRecord(
        id=uuid4(),
        policy_version="baseline_v1",
        data_snapshot_id=data_snapshot.id,
        decision_at=_OBSERVED_AT,
        available_at=_OBSERVED_AT,
        evidence_refs=[f"selection:{label}"],
        candidates=[
            {
                "instrument_id": str(INSTRUMENT_ID),
                "provenance_refs": [f"candidate:{label}"],
            }
        ],
        selected_instrument_ids=[str(INSTRUMENT_ID)],
        evidence_hash="a" * 64,
    )
    session.add_all((data_snapshot, selection))
    await session.flush()

    proposal_group = OptimizationProposalGroupRecord(
        id=uuid4(),
        baseline_portfolio_snapshot_id=_BASELINE_ID,
        portfolio_scope="internal_paper",
        selection_authority_id=selection.id,
        selection_evidence_hash=selection.evidence_hash,
        status="compared",
        version=1,
        policy_id=_OPTIMIZER_POLICY_ID,
        diagnostic_report_id=report.id,
        data_snapshot_id=data_snapshot.id,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=COST_POLICY_ID,
        diagnostic_input_hash=report.diagnostic_input_hash,
        valuation_time=_OBSERVED_AT,
        baseline_ready=True,
        diagnostic_complete=True,
        source_conflict=False,
        partial=False,
        requires_fx=False,
        cost_complete=True,
        authority_stale=False,
    )
    session.add(proposal_group)
    await session.flush()

    proposal_ids: dict[str, UUID] = {}
    for variant in _VARIANTS:
        proposal = OptimizationProposalRecord(
            id=uuid4(),
            proposal_group_id=proposal_group.id,
            variant=variant,
            baseline_portfolio_snapshot_id=_BASELINE_ID,
            analysis_only=False,
            strategy_evidence_complete=True,
            portfolio_scope="internal_paper",
        )
        session.add(proposal)
        await session.flush()
        proposal_ids[variant] = proposal.id
        session.add(
            ProposalComparisonRecord(
                id=uuid4(),
                proposal_id=proposal.id,
                metrics=complete_comparison_metrics(
                    report_id=report.id,
                    data_id=data_snapshot.id,
                    source_id=SOURCE_POLICY_ID,
                    optimizer_id=_OPTIMIZER_POLICY_ID,
                    cost_id=COST_POLICY_ID,
                ),
                tax_impact="unknown",
                requires_fx=False,
                non_executable=False,
                cost_complete=True,
            )
        )

    frozen_sessions = tuple(date(2026, 8, 25) + timedelta(days=index) for index in range(68))
    calendar = ExchangeCalendarAuthorityRecord(
        id=uuid4(),
        exchange="XNYS",
        version=f"shadow-{label}",
        source="fixture",
        range_start=frozen_sessions[0],
        range_end=frozen_sessions[-1],
        query_parameters={"fixture": label},
        completeness_status="complete",
        content_hash="c" * 64,
        published_at=_OBSERVED_AT,
    )
    session.add(calendar)
    await session.flush()
    session.add_all(
        ExchangeCalendarSessionRecord(
            calendar_authority_id=calendar.id,
            session=frozen_session,
            decision_at=_OBSERVED_AT + timedelta(days=index + 1),
        )
        for index, frozen_session in enumerate(frozen_sessions)
    )
    return _ProposalRoot(
        group_id=proposal_group.id,
        proposal_ids=proposal_ids,
        calendar_id=calendar.id,
        calendar_version=calendar.version,
        sessions=frozen_sessions,
    )


async def _insert_shadow_root(
    session: AsyncSession,
    proposal: _ProposalRoot,
    *,
    variants: tuple[str, ...] = _VARIANTS,
) -> _ShadowRoot:
    shadow_group = _shadow_group_record(proposal)
    session.add(shadow_group)
    await session.flush()

    portfolio_ids: dict[str, UUID] = {}
    digest_characters = {
        "hold_current": "d",
        "minimum_change": "e",
        "strategy_target": "f",
    }
    for variant in variants:
        portfolio = ShadowPortfolioRecord(
            id=uuid4(),
            shadow_group_id=shadow_group.id,
            proposal_group_id=proposal.group_id,
            proposal_id=proposal.proposal_ids[variant],
            variant=variant,
            target_weights_hash=digest_characters[variant] * 64,
            initial_nav="100.000000000000",
            initial_cash="100.000000000000",
        )
        session.add(portfolio)
        portfolio_ids[variant] = portfolio.id
    await session.flush()

    group = await session.get(OptimizationProposalGroupRecord, proposal.group_id)
    assert group is not None
    group.status = "shadowing"
    group.version = 2
    await session.flush()
    return _ShadowRoot(
        proposal=proposal,
        shadow_group_id=shadow_group.id,
        portfolio_ids=portfolio_ids,
    )


def _shadow_group_record(proposal: _ProposalRoot) -> ShadowGroupRecord:
    return ShadowGroupRecord(
        id=uuid4(),
        proposal_group_id=proposal.group_id,
        baseline_portfolio_snapshot_id=_BASELINE_ID,
        proposal_bundle_hash="b" * 64,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=COST_POLICY_ID,
        calendar_authority_id=proposal.calendar_id,
        calendar_version=proposal.calendar_version,
        calendar_hash="c" * 64,
        signal_session=date(2026, 8, 24),
        started_at=_OBSERVED_AT,
        start_session=proposal.sessions[0],
        evaluation_end_session=proposal.sessions[62],
        grace_end_session=proposal.sessions[67],
        evaluation_sessions=list(proposal.sessions[:63]),
        grace_sessions=list(proposal.sessions[63:]),
        initial_nav="100.000000000000",
        schema_version="proposal_shadow_v1",
    )


async def _seed_valid_shadow(database_url: str) -> _ShadowRoot:
    await seed_portfolio_list(database_url)
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        proposal = await _seed_proposal_root(session, label="primary")
        return await _insert_shadow_root(session, proposal)


def _config(monkeypatch: pytest.MonkeyPatch, database_url: str) -> Config:
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", database_url)
    return Config(str(engine_root / "alembic.ini"))


async def _function_definition(database_url: str, signature: str) -> str:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return str(
                (
                    await connection.execute(
                        text("SELECT pg_get_functiondef(CAST(:signature AS regprocedure))"),
                        {"signature": signature},
                    )
                ).scalar_one()
            )
    finally:
        await engine.dispose()


async def _shadow_function_names(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return set(
                (
                    await connection.execute(
                        text(
                            "SELECT proname FROM pg_proc "
                            "WHERE pronamespace = 'public'::regnamespace "
                            "AND (proname LIKE 'nexus_shadow_%' "
                            "OR proname IN ('nexus_reject_shadow_fact_mutation', "
                            "'nexus_date_array_strictly_increasing'))"
                        )
                    )
                ).scalars()
            )
    finally:
        await engine.dispose()


def _assert_shadow_schema(connection: Connection) -> None:
    inspector = inspect(connection)
    assert SHADOW_TABLES <= set(inspector.get_table_names())

    expected_uniques = {
        "shadow_groups": {"uq_shadow_groups_proposal_group"},
        "shadow_portfolios": {
            "uq_shadow_portfolios_group_variant",
            "uq_shadow_portfolios_proposal",
        },
        "shadow_session_authorities": {
            "uq_shadow_authorities_group_session",
            "uq_shadow_authorities_group_ordinal",
        },
        "shadow_session_price_inputs": {"uq_shadow_price_inputs_session_instrument"},
        "shadow_portfolio_states": {"uq_shadow_states_portfolio_session"},
        "shadow_simulated_fills": {"uq_shadow_fills_outcome"},
        "shadow_checkpoints": {"uq_shadow_checkpoints_group_ordinal"},
        "shadow_checkpoint_metrics": {"uq_shadow_metrics_checkpoint_variant"},
        "shadow_seals": {"uq_shadow_seals_group"},
    }
    for table_name, required_names in expected_uniques.items():
        actual_names = {item["name"] for item in inspector.get_unique_constraints(table_name)}
        assert required_names <= actual_names

    expected_checks = {
        "shadow_groups": {
            "ck_shadow_groups_evaluation_session_count",
            "ck_shadow_groups_grace_session_count",
            "ck_shadow_groups_initial_nav_fixed",
            "ck_shadow_groups_schema_version_v1",
        },
        "shadow_session_authorities": {"ck_shadow_session_authorities_ordinal_bounded"},
        "shadow_checkpoints": {"ck_shadow_checkpoints_ordinal_allowed"},
    }
    for table_name, required_names in expected_checks.items():
        actual_names = {item["name"] for item in inspector.get_check_constraints(table_name)}
        assert required_names <= actual_names

    for table_name in SHADOW_TABLES:
        immutable_trigger = f"trg_{table_name}_immutable"[:63]
        assert connection.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_trigger "
                "WHERE tgrelid = CAST(:table_name AS regclass) "
                "AND tgname = :trigger_name AND NOT tgisinternal)"
            ),
            {"table_name": table_name, "trigger_name": immutable_trigger},
        ).scalar_one()
        if table_name != "shadow_groups":
            unsealed_trigger = f"trg_{table_name}_unsealed_insert"[:63]
            assert connection.execute(
                text(
                    "SELECT EXISTS (SELECT 1 FROM pg_trigger "
                    "WHERE tgrelid = CAST(:table_name AS regclass) "
                    "AND tgname = :trigger_name AND NOT tgisinternal)"
                ),
                {"table_name": table_name, "trigger_name": unsealed_trigger},
            ).scalar_one()


def test_0013_is_single_head_and_round_trips_from_0012(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """The Shadow release is one reversible successor of deterministic backtests."""
    config = _config(monkeypatch, temporary_database_url)
    assert ScriptDirectory.from_config(config).get_heads() == [
        "0023_evolution_authority"
    ]

    command.upgrade(config, "0012_backtests")
    assert SHADOW_TABLES.isdisjoint(_table_names(temporary_database_url))
    command.upgrade(config, "0013_shadow")
    assert SHADOW_TABLES.issubset(_table_names(temporary_database_url))
    command.downgrade(config, "0012_backtests")
    assert SHADOW_TABLES.isdisjoint(_table_names(temporary_database_url))
    assert asyncio.run(_shadow_function_names(temporary_database_url)) == set()
    command.upgrade(config, "0013_shadow")
    assert SHADOW_TABLES.issubset(_table_names(temporary_database_url))


def test_0013_head_has_named_constraints_and_database_guards(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """Head exposes the complete append-only graph and its named database guards."""
    config = _config(monkeypatch, temporary_database_url)
    command.upgrade(config, "head")

    async def exercise() -> None:
        engine = create_async_engine(temporary_database_url)
        try:
            async with engine.connect() as connection:
                await connection.run_sync(_assert_shadow_schema)
        finally:
            await engine.dispose()

    asyncio.run(exercise())
    assert asyncio.run(_shadow_function_names(temporary_database_url)) == SHADOW_FUNCTIONS


def test_0013_downgrade_restores_exact_pre_shadow_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """Downgrade must not leave the proposal lifecycle referencing removed Shadow tables."""
    config = _config(monkeypatch, temporary_database_url)
    command.upgrade(config, "0012_backtests")
    before = asyncio.run(
        _function_definition(temporary_database_url, "nexus_optimization_group_lifecycle()")
    )

    command.upgrade(config, "0013_shadow")
    during = asyncio.run(
        _function_definition(temporary_database_url, "nexus_optimization_group_lifecycle()")
    )
    assert "shadow_groups" in during
    assert "shadow_seals" in during

    command.downgrade(config, "0012_backtests")
    after = asyncio.run(
        _function_definition(temporary_database_url, "nexus_optimization_group_lifecycle()")
    )
    assert after == before


@pytest.mark.asyncio
async def test_shadow_facts_reject_update_delete_and_orphan_inserts(
    migrated_database_url: str,
) -> None:
    """Every Shadow fact is append-only and every child must resolve a real root."""
    shadow = await _seed_valid_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))

    for statement in (
        "UPDATE shadow_groups SET schema_version = 'changed' WHERE id = :id",
        "DELETE FROM shadow_groups WHERE id = :id",
    ):
        with pytest.raises(DBAPIError, match="shadow fact is append-only"):
            async with factory() as session, session.begin():
                await session.execute(text(statement), {"id": shadow.shadow_group_id})

    with pytest.raises(DBAPIError, match="shadow root is missing"):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO shadow_advance_attempts "
                    "(id, shadow_group_id, requested_session, expected_session, "
                    "observed_at, result, reason_code) VALUES "
                    "(:id, :group_id, '2026-08-25', '2026-08-25', :observed_at, "
                    "'data_partial', 'SHADOW_DATA_PARTIAL')"
                ),
                {
                    "id": uuid4(),
                    "group_id": uuid4(),
                    "observed_at": _OBSERVED_AT,
                },
            )


@pytest.mark.asyncio
async def test_shadow_group_rejects_noncanonical_root_values(
    migrated_database_url: str,
) -> None:
    """Hashes, Decimal anchors, schema versions, and frozen arrays fail closed."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        proposal = await _seed_proposal_root(session, label="invalid-root")

    def uppercase_hash(record: ShadowGroupRecord) -> None:
        record.proposal_bundle_hash = "A" * 64

    def nonfinite_anchor(record: ShadowGroupRecord) -> None:
        record.initial_nav = Decimal("NaN")

    def wrong_schema(record: ShadowGroupRecord) -> None:
        record.schema_version = "proposal_shadow_v2"

    def short_window(record: ShadowGroupRecord) -> None:
        record.evaluation_sessions = record.evaluation_sessions[:-1]

    def unordered_window(record: ShadowGroupRecord) -> None:
        record.evaluation_sessions[1] = record.evaluation_sessions[0]

    for mutate in (
        uppercase_hash,
        nonfinite_anchor,
        wrong_schema,
        short_window,
        unordered_window,
    ):
        with pytest.raises(IntegrityError):
            async with factory() as session, session.begin():
                record = _shadow_group_record(proposal)
                mutate(record)
                session.add(record)
                await session.flush()


@pytest.mark.asyncio
async def test_shadow_group_requires_exact_three_and_same_proposal_root(
    migrated_database_url: str,
) -> None:
    """Two variants, a fourth row, and cross-root proposal ownership are impossible."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))

    with pytest.raises(DBAPIError, match="PROPOSAL_SHADOW_INCOMPLETE"):
        async with factory() as session, session.begin():
            proposal = await _seed_proposal_root(session, label="two-variants")
            await _insert_shadow_root(
                session, proposal, variants=("hold_current", "minimum_change")
            )

    with pytest.raises(IntegrityError, match="fk_shadow_portfolios_proposal_root"):
        async with factory() as session, session.begin():
            first = await _seed_proposal_root(session, label="cross-root-first")
            second = await _seed_proposal_root(session, label="cross-root-second")
            group = _shadow_group_record(first)
            session.add(group)
            await session.flush()
            for variant in ("hold_current", "minimum_change"):
                session.add(
                    ShadowPortfolioRecord(
                        id=uuid4(),
                        shadow_group_id=group.id,
                        proposal_group_id=first.group_id,
                        proposal_id=first.proposal_ids[variant],
                        variant=variant,
                        target_weights_hash="d" * 64,
                        initial_nav="100",
                        initial_cash="100",
                    )
                )
            session.add(
                ShadowPortfolioRecord(
                    id=uuid4(),
                    shadow_group_id=group.id,
                    proposal_group_id=first.group_id,
                    proposal_id=second.proposal_ids["strategy_target"],
                    variant="strategy_target",
                    target_weights_hash="e" * 64,
                    initial_nav="100",
                    initial_cash="100",
                )
            )
            await session.flush()

    async with factory() as session, session.begin():
        proposal = await _seed_proposal_root(session, label="fourth-portfolio")
        shadow = await _insert_shadow_root(session, proposal)
    with pytest.raises(IntegrityError):
        async with factory() as session, session.begin():
            session.add(
                ShadowPortfolioRecord(
                    id=uuid4(),
                    shadow_group_id=shadow.shadow_group_id,
                    proposal_group_id=shadow.proposal.group_id,
                    proposal_id=shadow.proposal.proposal_ids["hold_current"],
                    variant="hold_current",
                    target_weights_hash="f" * 64,
                    initial_nav="100",
                    initial_cash="100",
                )
            )
            await session.flush()


@pytest.mark.asyncio
async def test_checkpoint_requires_all_three_variant_metric_rows(
    migrated_database_url: str,
) -> None:
    """An incomplete checkpoint still needs three explicit unknown metric rows."""
    shadow = await _seed_valid_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(DBAPIError, match="shadow checkpoint requires exact variant metrics"):
        async with factory() as session, session.begin():
            checkpoint = ShadowCheckpointRecord(
                id=uuid4(),
                shadow_group_id=shadow.shadow_group_id,
                session_authority_id=None,
                session=shadow.proposal.sessions[4],
                ordinal=5,
                status="incomplete",
                coverage_count=0,
                reason="SHADOW_CHECKPOINT_INCOMPLETE",
                metrics_hash="a" * 64,
            )
            session.add(checkpoint)
            await session.flush()
            for variant in ("hold_current", "minimum_change"):
                session.add(
                    ShadowCheckpointMetricRecord(
                        id=uuid4(),
                        shadow_group_id=shadow.shadow_group_id,
                        checkpoint_id=checkpoint.id,
                        shadow_portfolio_id=shadow.portfolio_ids[variant],
                        variant=variant,
                        metric_status="unknown",
                        unknown_reason="SHADOW_CHECKPOINT_INCOMPLETE",
                        net_cumulative_return=None,
                        return_delta_vs_hold=None,
                        max_drawdown=None,
                        drawdown_improvement=None,
                        annualized_volatility=None,
                        volatility_improvement=None,
                        cumulative_turnover=None,
                        fee_drag_index_points=None,
                        worst_sessions=None,
                    )
                )

    async with factory() as session, session.begin():
        checkpoint = ShadowCheckpointRecord(
            id=uuid4(),
            shadow_group_id=shadow.shadow_group_id,
            session_authority_id=None,
            session=shadow.proposal.sessions[4],
            ordinal=5,
            status="incomplete",
            coverage_count=0,
            reason="SHADOW_CHECKPOINT_INCOMPLETE",
            metrics_hash="b" * 64,
        )
        session.add(checkpoint)
        await session.flush()
        for variant in _VARIANTS:
            session.add(
                ShadowCheckpointMetricRecord(
                    id=uuid4(),
                    shadow_group_id=shadow.shadow_group_id,
                    checkpoint_id=checkpoint.id,
                    shadow_portfolio_id=shadow.portfolio_ids[variant],
                    variant=variant,
                    metric_status="unknown",
                    unknown_reason="SHADOW_CHECKPOINT_INCOMPLETE",
                    net_cumulative_return=None,
                    return_delta_vs_hold=None,
                    max_drawdown=None,
                    drawdown_improvement=None,
                    annualized_volatility=None,
                    volatility_improvement=None,
                    cumulative_turnover=None,
                    fee_drag_index_points=None,
                    worst_sessions=None,
                )
            )


@pytest.mark.asyncio
async def test_seal_matches_lifecycle_and_blocks_later_children(
    migrated_database_url: str,
) -> None:
    """A seal commits with its matching terminal status and closes the entire graph."""
    shadow = await _seed_valid_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))

    with pytest.raises(DBAPIError, match="shadow seal does not match proposal lifecycle"):
        async with factory() as session, session.begin():
            session.add(
                ShadowSealRecord(
                    id=uuid4(),
                    shadow_group_id=shadow.shadow_group_id,
                    reason="rejected",
                    reason_code="USER_REJECTED",
                    sealed_at=_OBSERVED_AT,
                    final_coverage=0,
                    final_authority_hash="a" * 64,
                )
            )

    async with factory() as session, session.begin():
        session.add(
            ShadowSealRecord(
                id=uuid4(),
                shadow_group_id=shadow.shadow_group_id,
                reason="rejected",
                reason_code="USER_REJECTED",
                sealed_at=_OBSERVED_AT,
                final_coverage=0,
                final_authority_hash="a" * 64,
            )
        )
        group = await session.get(OptimizationProposalGroupRecord, shadow.proposal.group_id)
        assert group is not None
        group.status = "rejected"
        group.version = 3

    with pytest.raises(DBAPIError, match="shadow is sealed"):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO shadow_advance_attempts "
                    "(id, shadow_group_id, requested_session, expected_session, "
                    "observed_at, result, reason_code) VALUES "
                    "(:id, :group_id, '2026-08-25', '2026-08-25', :observed_at, "
                    "'data_partial', 'SHADOW_DATA_PARTIAL')"
                ),
                {
                    "id": uuid4(),
                    "group_id": shadow.shadow_group_id,
                    "observed_at": _OBSERVED_AT,
                },
            )


@pytest.mark.asyncio
async def test_data_incomplete_seal_cannot_precede_frozen_grace_deadline(
    migrated_database_url: str,
) -> None:
    shadow = await _seed_valid_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))

    with pytest.raises(DBAPIError, match="shadow data-incomplete seal precedes grace deadline"):
        async with factory() as session, session.begin():
            session.add(
                ShadowSealRecord(
                    id=uuid4(),
                    shadow_group_id=shadow.shadow_group_id,
                    reason="expired",
                    reason_code="SHADOW_DATA_INCOMPLETE",
                    sealed_at=_OBSERVED_AT,
                    final_coverage=0,
                    final_authority_hash="a" * 64,
                )
            )
            group = await session.get(OptimizationProposalGroupRecord, shadow.proposal.group_id)
            assert group is not None
            group.status = "expired"
            group.version = 3


@pytest.mark.asyncio
async def test_direct_validated_transition_requires_complete_shadow_authority(
    migrated_database_url: str,
) -> None:
    """A forged validated seal cannot bypass 63/189/3/9 and first-fill gates."""
    shadow = await _seed_valid_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(DBAPIError, match="PROPOSAL_SHADOW_VALIDATION_INCOMPLETE"):
        async with factory() as session, session.begin():
            session.add(
                ShadowSealRecord(
                    id=uuid4(),
                    shadow_group_id=shadow.shadow_group_id,
                    reason="validated",
                    reason_code="SHADOW_VALIDATED",
                    sealed_at=_OBSERVED_AT,
                    final_coverage=63,
                    final_authority_hash="a" * 64,
                )
            )
            await session.flush()
            group = await session.get(OptimizationProposalGroupRecord, shadow.proposal.group_id)
            assert group is not None
            group.status = "validated"
            group.version = 3
            await session.flush()
