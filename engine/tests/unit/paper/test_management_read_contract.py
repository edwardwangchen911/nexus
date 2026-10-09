from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nexus.api.errors import NexusError
from nexus.paper.management_read_authority import (
    load_proposal_read_authority,
    load_target_read_authority,
)
from nexus.paper.read_service import PaperReadService, portfolio_target_matches_batch
from nexus.paper.target_service import (
    TargetActivationBlockedError,
    require_activation_projection_unoccupied,
)
from nexus.settings import EngineSettings
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.parametrize(
    "override",
    [
        {"environment": "development"},
        {"environment": "production"},
        {"provider_jobs_enabled": True},
        {"agent_paper_cycle_enabled": True},
        {"database_url": "postgresql+asyncpg://localhost/nexus_main"},
        {"database_url": "postgresql+asyncpg://remote/nexus_test_read"},
        {"database_url": "postgresql+asyncpg://localhost/nexus_test_read?host=remote"},
        {"database_url": "postgresql+asyncpg://localhost/nexus_test_read?dbname=nexus_main"},
        {"database_url": "sqlite:///nexus_test_read"},
        {"database_url": None},
    ],
)
def test_candidate_paper_read_configuration_rejects_unsafe_runtime(override):
    config = {
        "environment": "test",
        "database_url": "postgresql+asyncpg://localhost/nexus_test_read",
        "candidate_stage_capacity_reads": True,
    }
    config.update(override)
    with pytest.raises(ValueError, match="CODEX_CANDIDATE_READ_CONFIGURATION_REJECTED"):
        EngineSettings(**config)


def test_candidate_paper_read_configuration_is_explicit_and_test_only():
    assert not EngineSettings().candidate_stage_capacity_reads
    settings = EngineSettings(
        environment="test",
        database_url="postgresql+asyncpg://localhost/nexus_diag_read",
        candidate_stage_capacity_reads=True,
    )
    assert settings.candidate_stage_capacity_reads
    assert not settings.provider_jobs_enabled and not settings.agent_paper_cycle_enabled


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["account", "proposal", "target"])
async def test_candidate_readers_reject_actual_business_database_before_fact_reads(reader):
    scalar = AsyncMock(return_value="nexus_business")
    session = cast(AsyncSession, SimpleNamespace(scalar=scalar))
    now = datetime(2026, 10, 9, tzinfo=UTC)
    with pytest.raises(ValueError, match="CODEX_CANDIDATE_ISOLATION_REQUIRED"):
        if reader == "account":
            await PaperReadService(session, now, candidate_stage_capacity=True).account()
        elif reader == "proposal":
            await load_proposal_read_authority(
                session,
                account_id=uuid4(),
                decision_id=uuid4(),
                read_as_of=now,
                candidate_stage_capacity=True,
            )
        else:
            await load_target_read_authority(
                session,
                account_id=uuid4(),
                target_id=uuid4(),
                read_as_of=now,
                candidate_stage_capacity=True,
            )
    scalar.assert_awaited_once()
    assert "current_database" in str(scalar.call_args.args[0])


def test_same_target_new_decision_remains_a_valid_batch_association() -> None:
    account_id = uuid4()
    portfolio_id = uuid4()
    target_decision_id = uuid4()
    batch_decision_id = uuid4()
    target_id = uuid4()

    assert portfolio_target_matches_batch(
        account_id=account_id,
        portfolio_id=portfolio_id,
        batch_decision_id=batch_decision_id,
        batch_decision_hash="a" * 64,
        proposal_decision_id=batch_decision_id,
        proposal_decision_hash="a" * 64,
        proposal_target_content_hash="b" * 64,
        target_account_id=account_id,
        target_portfolio_id=portfolio_id,
        target_id=target_id,
        expected_target_id=target_id,
        target_decision_id=target_decision_id,
        target_proposal_decision_id=target_decision_id,
        target_content_hash="b" * 64,
    )


def test_batch_association_rejects_target_content_mismatch() -> None:
    identity = uuid4()
    assert not portfolio_target_matches_batch(
        account_id=identity,
        portfolio_id=identity,
        batch_decision_id=identity,
        batch_decision_hash="a" * 64,
        proposal_decision_id=identity,
        proposal_decision_hash="a" * 64,
        proposal_target_content_hash="b" * 64,
        target_account_id=identity,
        target_portfolio_id=identity,
        target_id=identity,
        expected_target_id=identity,
        target_decision_id=identity,
        target_proposal_decision_id=identity,
        target_content_hash="c" * 64,
    )


def test_activation_result_requires_exact_event_batch_and_target_identity() -> None:
    now = datetime(2026, 9, 19, tzinfo=UTC)
    decision_id = uuid4()
    target_id = uuid4()
    batch_id = uuid4()
    service = PaperReadService(cast(AsyncSession, None), now)
    decision = SimpleNamespace(id=decision_id)
    target = SimpleNamespace(target_version_id=target_id)
    batch = SimpleNamespace(id=batch_id)

    exact = SimpleNamespace(
        result_code="PAPER_BATCH_EVALUATED",
        fact_ids={
            "strategy_decision_id": str(decision_id),
            "target_version_id": str(target_id),
            "batch_id": str(batch_id),
        },
        occurred_at=now,
        created_at=now + timedelta(minutes=2),
    )
    result = service._activation_result(
        event=cast(Any, exact),
        decision=cast(Any, decision),
        target=cast(Any, target),
        batch=cast(Any, batch),
    )
    assert result is not None
    assert result.result_code == "PLAN_CREATED"
    assert result.batch_id == batch_id
    assert result.as_of == exact.created_at

    missing_batch = SimpleNamespace(
        result_code="PAPER_BATCH_EVALUATED",
        fact_ids={
            "strategy_decision_id": str(decision_id),
            "target_version_id": str(target_id),
        },
        occurred_at=now,
    )
    with pytest.raises(NexusError):
        service._activation_result(
            event=cast(Any, missing_batch),
            decision=cast(Any, decision),
            target=cast(Any, target),
            batch=cast(Any, batch),
        )


def test_no_order_activation_reports_recorded_time_after_proposal() -> None:
    decision_time = datetime(2026, 9, 22, 16, 30, tzinfo=UTC)
    recorded_at = decision_time + timedelta(minutes=3)
    decision_id = uuid4()
    target_id = uuid4()
    event = SimpleNamespace(
        result_code="NO_ORDER_REQUIRED",
        fact_ids={
            "strategy_decision_id": str(decision_id),
            "target_version_id": str(target_id),
        },
        occurred_at=decision_time,
        created_at=recorded_at,
    )
    service = PaperReadService(cast(AsyncSession, None), recorded_at)

    result = service._activation_result(
        event=cast(Any, event),
        decision=cast(Any, SimpleNamespace(id=decision_id)),
        target=cast(Any, SimpleNamespace(target_version_id=target_id)),
        batch=None,
        strict_identity=True,
    )

    assert result is not None
    assert result.result_code == "NO_ORDER_REQUIRED"
    assert result.as_of == recorded_at


def test_blocked_activation_never_accepts_target_or_plan_facts() -> None:
    now = datetime(2026, 9, 19, tzinfo=UTC)
    service = PaperReadService(cast(AsyncSession, None), now)
    blocked = SimpleNamespace(
        result_code="TARGET_ACTIVATION_BLOCKED",
        fact_ids={"strategy_decision_id": str(uuid4())},
        occurred_at=now,
    )
    with pytest.raises(NexusError):
        service._activation_result(
            event=cast(Any, blocked),
            decision=cast(Any, SimpleNamespace(id=uuid4())),
            target=cast(Any, SimpleNamespace(target_version_id=uuid4())),
            batch=None,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("missing", "multiple"))
async def test_ambiguous_occupation_never_becomes_a_planless_blocker(mode: str) -> None:
    first_order = uuid4()
    second_order = uuid4()
    first_batch = uuid4()
    second_batch = uuid4()
    rows = (
        [(first_order, first_batch)]
        if mode == "missing"
        else [(first_order, first_batch), (second_order, second_batch)]
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: rows)))
    projection = SimpleNamespace(
        open_orders=(
            SimpleNamespace(order_id=first_order),
            SimpleNamespace(order_id=second_order),
        ),
        reserved_cash=1,
        positions=(),
    )
    with pytest.raises(ValueError, match="^INITIAL_PROJECTION_OCCUPANCY_AUTHORITY_CONFLICT$"):
        await require_activation_projection_unoccupied(
            cast(AsyncSession, session),
            account_id=uuid4(),
            projection=cast(Any, projection),
        )


@pytest.mark.asyncio
async def test_one_exact_occupying_batch_is_carried_by_the_typed_blocker() -> None:
    order_id = uuid4()
    batch_id = uuid4()
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: [(order_id, batch_id)]))
    )
    projection = SimpleNamespace(
        open_orders=(SimpleNamespace(order_id=order_id),),
        reserved_cash=1,
        positions=(),
    )
    with pytest.raises(TargetActivationBlockedError) as caught:
        await require_activation_projection_unoccupied(
            cast(AsyncSession, session),
            account_id=uuid4(),
            projection=cast(Any, projection),
        )
    assert caught.value.blocking_plan_id == batch_id


@pytest.mark.asyncio
async def test_latest_recognized_activation_event_is_never_hidden_by_missing_facts() -> None:
    now = datetime(2026, 9, 19, tzinfo=UTC)
    cycle_id = uuid4()
    latest = SimpleNamespace(
        sequence=5,
        result_code="PAPER_BATCH_EVALUATED",
        fact_ids={"strategy_decision_id": str(uuid4())},
        occurred_at=now,
    )
    session = SimpleNamespace(scalar=AsyncMock(return_value=latest))
    service = PaperReadService(cast(AsyncSession, session), now)

    event = await service._cycle_activation_event(cycle_id)

    assert event is latest


def test_candidate_batch_activation_requires_every_exact_event_identity() -> None:
    now = datetime(2026, 9, 19, tzinfo=UTC)
    decision_id = uuid4()
    target_id = uuid4()
    service = PaperReadService(cast(AsyncSession, None), now)
    event = SimpleNamespace(
        result_code="NO_ORDER_REQUIRED",
        fact_ids={"target_version_id": str(target_id)},
        occurred_at=now,
    )

    with pytest.raises(NexusError):
        service._activation_result(
            event=cast(Any, event),
            decision=cast(Any, SimpleNamespace(id=decision_id)),
            target=cast(Any, SimpleNamespace(target_version_id=target_id)),
            batch=None,
            strict_identity=True,
        )


@pytest.mark.parametrize("missing", ("decision", "target", "batch"))
def test_candidate_batch_activation_rejects_missing_authority_rows(missing: str) -> None:
    now = datetime(2026, 9, 19, tzinfo=UTC)
    decision_id = uuid4()
    target_id = uuid4()
    batch_id = uuid4()
    service = PaperReadService(cast(AsyncSession, None), now)
    event = SimpleNamespace(
        result_code="PAPER_BATCH_EVALUATED",
        fact_ids={
            "strategy_decision_id": str(decision_id),
            "target_version_id": str(target_id),
            "batch_id": str(batch_id),
        },
        occurred_at=now,
    )

    with pytest.raises(NexusError) as caught:
        service._activation_result(
            event=cast(Any, event),
            decision=(
                None if missing == "decision" else cast(Any, SimpleNamespace(id=decision_id))
            ),
            target=(
                None
                if missing == "target"
                else cast(Any, SimpleNamespace(target_version_id=target_id))
            ),
            batch=(None if missing == "batch" else cast(Any, SimpleNamespace(id=batch_id))),
            strict_identity=True,
        )

    assert caught.value.code.value == "DATA_PARTIAL"
