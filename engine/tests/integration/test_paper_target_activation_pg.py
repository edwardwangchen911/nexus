import asyncio
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from integration.test_portfolio_proposal_pg import proposal_source
from integration.test_portfolio_synthesis_persistence_pg import synthesis_intent
from nexus.paper.agent_decision_store import (
    PersistedPortfolioProposal,
    persist_portfolio_proposal,
)
from nexus.paper.portfolio_proposal import PortfolioBaselineV1
from nexus.paper.service import PaperBatchError, create_paper_order_batch
from nexus.paper.target_service import activate_portfolio_target
from nexus.persistence.models.paper import (
    PaperOrderBatchRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
)
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError


def _include_first(run):
    from nexus.agents.portfolio_synthesis_contracts import PortfolioSynthesisIntentV1

    document = synthesis_intent(run).model_dump(mode="json")
    document["choices"][0].update(action="include", role="core", rank=1)
    return PortfolioSynthesisIntentV1.model_validate(document)


@pytest.mark.asyncio
async def test_first_activation_appends_one_target_and_no_plan_for_all_cash(
    migrated_database_url,
):
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            proposal = await persist_portfolio_proposal(session, **args)
            result = await activate_portfolio_target(
                session,
                account_id=proposal.account_id,
                strategy_decision_id=proposal.decision_id,
                request_time=args["now"] + timedelta(seconds=1),
                idempotency_key="activate-first",
            )
            assert result.result_code == "NO_ORDER_REQUIRED"
            assert result.target_created is True
            assert result.batch_id is None
            target = await session.get(PaperTargetVersionRecord, result.target_version_id)
            assert target.version_seq == 1
            assert target.previous_target_version_id is None
            assert target.strategy_decision_id == proposal.decision_id
            assert (
                await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord)) == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_activation_replay_and_new_key_reuse_same_target(migrated_database_url):
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            proposal = await persist_portfolio_proposal(session, **args)
            values = dict(
                account_id=proposal.account_id,
                strategy_decision_id=proposal.decision_id,
                request_time=args["now"] + timedelta(seconds=1),
            )
            first = await activate_portfolio_target(
                session, **values, idempotency_key="activate-first"
            )
            replay = await activate_portfolio_target(
                session, **values, idempotency_key="activate-first"
            )
            reused = await activate_portfolio_target(
                session,
                **(values | {"request_time": values["request_time"] + timedelta(hours=1)}),
                idempotency_key="activate-second",
            )
            assert first == replay
            assert reused.target_version_id == first.target_version_id
            assert reused.target_created is True
            assert reused.activated_at == first.activated_at
            assert (
                await session.scalar(select(func.count()).select_from(PaperTargetVersionRecord))
                == 1
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_direct_batch_rejects_superseded_target_version(migrated_database_url):
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            proposal = await persist_portfolio_proposal(session, **args)
            first = await activate_portfolio_target(
                session,
                account_id=proposal.account_id,
                strategy_decision_id=proposal.decision_id,
                request_time=args["now"] + timedelta(seconds=1),
                idempotency_key="activate-first",
            )
            target = await session.get(PaperTargetVersionRecord, first.target_version_id)
            other_decision = await session.scalar(
                select(StrategyDecisionRecord).where(
                    StrategyDecisionRecord.account_id == proposal.account_id,
                    StrategyDecisionRecord.id != proposal.decision_id,
                )
            )
            assert other_decision is not None
            superseding = PaperTargetVersionRecord(
                account_id=target.account_id,
                portfolio_id=target.portfolio_id,
                version_seq=2,
                strategy_decision_id=other_decision.id,
                previous_target_version_id=target.id,
                target_content_hash="b" * 64,
                activated_at=args["now"] + timedelta(seconds=2),
                activation_hash="c" * 64,
                created_at=args["now"] + timedelta(seconds=2),
            )
            session.add(superseding)
            await session.flush()
            with pytest.raises(PaperBatchError, match="PORTFOLIO_ACTIVATION_REQUIRED"):
                await create_paper_order_batch(
                    session,
                    account_id=proposal.account_id,
                    strategy_decision_id=proposal.decision_id,
                    request_time=args["now"] + timedelta(seconds=3),
                    idempotency_key="superseded-direct-batch",
                    target_version_id=target.id,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_public_batch_rejects_portfolio_decision_without_target_id(
    migrated_database_url,
):
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            proposal = await persist_portfolio_proposal(session, **args)
            with pytest.raises(PaperBatchError, match="PORTFOLIO_ACTIVATION_REQUIRED"):
                await create_paper_order_batch(
                    session,
                    account_id=proposal.account_id,
                    strategy_decision_id=proposal.decision_id,
                    request_time=args["now"] + timedelta(seconds=1),
                    idempotency_key="portfolio-direct-batch",
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_fresh_same_hash_decision_reuses_target_and_replay_never_creates_plan(
    migrated_database_url, monkeypatch
):
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            first_proposal = await persist_portfolio_proposal(session, **args)
            first = await activate_portfolio_target(
                session,
                account_id=first_proposal.account_id,
                strategy_decision_id=first_proposal.decision_id,
                request_time=args["now"] + timedelta(seconds=1),
                idempotency_key="activate-first",
            )
            decision_id = uuid4()
            target = await session.get(PaperTargetVersionRecord, first.target_version_id)
            baseline = PortfolioBaselineV1(
                weights=first_proposal.proposal.weights,
                cash_weight=first_proposal.proposal.cash_weight,
                roles=first_proposal.proposal.roles,
                target_version_id=target.id,
                content_hash=target.target_content_hash,
            )
            fresh = PersistedPortfolioProposal(
                decision_id=decision_id,
                account_id=first_proposal.account_id,
                cycle_id=first_proposal.cycle_id,
                proposal=first_proposal.proposal.model_copy(update={"baseline": baseline}),
            )

        async def load_fresh(session, *, account_id, decision_id):
            assert account_id == fresh.account_id
            return fresh if decision_id == fresh.decision_id else first_proposal

        monkeypatch.setattr("nexus.paper.agent_decision_store.load_portfolio_proposal", load_fresh)
        async with sessions.begin() as session:
            activated = await activate_portfolio_target(
                session,
                account_id=fresh.account_id,
                strategy_decision_id=fresh.decision_id,
                request_time=args["now"] + timedelta(seconds=2),
                idempotency_key="same-target-fresh-decision",
            )
            replay = await activate_portfolio_target(
                session,
                account_id=fresh.account_id,
                strategy_decision_id=fresh.decision_id,
                request_time=args["now"] + timedelta(minutes=1),
                idempotency_key="same-target-new-key",
            )
            assert activated.target_version_id == first.target_version_id
            assert activated.target_created is False
            assert activated.batch_id is None
            assert replay == activated
            assert (
                await session.scalar(select(func.count()).select_from(PaperTargetVersionRecord))
                == 1
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("position_quantity", "expects_plan"),
    [
        (Decimal("5"), False),
        (Decimal("8"), True),
    ],
)
async def test_same_target_raw_drift_delegates_remainder_to_private_batch(
    migrated_database_url,
    monkeypatch,
    position_quantity,
    expects_plan,
):
    from types import SimpleNamespace

    from nexus.paper import service, target_service
    from nexus.paper.projection import load_paper_projection
    from nexus.risk.canonical import canonical_paper_projection_hash
    from nexus.risk.models import PaperPositionBalance

    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            first_proposal = await persist_portfolio_proposal(session, **args)
            first = await activate_portfolio_target(
                session,
                account_id=first_proposal.account_id,
                strategy_decision_id=first_proposal.decision_id,
                request_time=args["now"] + timedelta(seconds=1),
                idempotency_key="activate-first",
            )
            target = await session.get(PaperTargetVersionRecord, first.target_version_id)
            base = await load_paper_projection(
                session,
                account_id=first_proposal.account_id,
                as_of=args["now"] + timedelta(seconds=2),
            )
        instrument_id = next(iter(first_proposal.proposal.weights))
        fresh = PersistedPortfolioProposal(
            decision_id=uuid4(),
            account_id=first_proposal.account_id,
            cycle_id=first_proposal.cycle_id,
            proposal=first_proposal.proposal.model_copy(
                update={
                    "baseline": PortfolioBaselineV1(
                        weights=first_proposal.proposal.weights,
                        cash_weight=first_proposal.proposal.cash_weight,
                        roles=first_proposal.proposal.roles,
                        target_version_id=target.id,
                        content_hash=target.target_content_hash,
                    )
                }
            ),
        )
        projection = base.model_copy(
            update={
                "positions": (
                    PaperPositionBalance(
                        instrument_id=instrument_id,
                        quantity=position_quantity,
                        reserved_quantity=Decimal(0),
                        cost_basis=position_quantity * Decimal(100),
                        reference_close=Decimal(100),
                    ),
                )
            }
        )
        projection = projection.model_copy(
            update={"projection_hash": canonical_paper_projection_hash(projection)}
        )

        async def load_fresh(*_args, **_kwargs):
            return fresh

        async def load_projection(*_args, **_kwargs):
            return projection

        calls = []

        async def create_batch(*_args, **_kwargs):
            calls.append(_kwargs)
            return SimpleNamespace(batch_id=uuid4()) if expects_plan else None

        monkeypatch.setattr(
            "nexus.paper.agent_decision_store.load_portfolio_proposal", load_fresh
        )
        monkeypatch.setattr(target_service, "load_paper_projection", load_projection)
        monkeypatch.setattr(service, "_create_target_aware_paper_order_batch", create_batch)
        async with sessions.begin() as session:
            activated = await activate_portfolio_target(
                session,
                account_id=fresh.account_id,
                strategy_decision_id=fresh.decision_id,
                request_time=args["now"] + timedelta(seconds=3),
                idempotency_key=f"risk-remainder-{position_quantity}",
            )
            assert (activated.batch_id is not None) is expects_plan
            assert bool(calls) is True
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_expired_proposal_does_not_activate(migrated_database_url):
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            proposal = await persist_portfolio_proposal(session, **args)
            with pytest.raises(ValueError, match="^PORTFOLIO_PROPOSAL_EXPIRED$"):
                await activate_portfolio_target(
                    session,
                    account_id=proposal.account_id,
                    strategy_decision_id=proposal.decision_id,
                    request_time=proposal.proposal.expires_at,
                    idempotency_key="activate-expired",
                )
            assert (
                await session.scalar(select(func.count()).select_from(PaperTargetVersionRecord))
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_nonempty_first_target_creates_one_target_aware_plan(
    migrated_database_url, monkeypatch
):
    import integration.test_portfolio_synthesis_persistence_pg as seed_module

    original_intent = seed_module._safe_no_trade_intent

    def complete_buy(**kwargs):
        source = original_intent(**kwargs)
        assessment = source.synthesis_assessment.model_copy(
            update={
                "stance": "buy_candidate",
                "review_conclusion": "sufficient",
                "reviews": tuple(
                    review.model_copy(
                        update={
                            "resolved": True,
                            "status": "accepted",
                            "missing_evidence": (),
                            "prevented_conclusion": None,
                        }
                    )
                    for review in source.synthesis_assessment.reviews
                ),
            }
        )
        return source.model_copy(
            update={
                "quality_status": "complete",
                "stance": "buy_candidate",
                "exposure_action": "eligible_for_increase",
                "evidence_audit": "complete",
                "synthesis_assessment": assessment,
                "role_votes": tuple(
                    vote.model_copy(update={"vote": "support"}) for vote in source.role_votes
                ),
            }
        )

    monkeypatch.setattr(seed_module, "_safe_no_trade_intent", complete_buy)
    from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis

    seeded = await _seed_normal_synthesis(migrated_database_url, monkeypatch)
    engine, sessions, args = await proposal_source(
        migrated_database_url, seeded=seeded, intent_factory=_include_first
    )
    try:
        async with sessions.begin() as session:
            proposal = await persist_portfolio_proposal(session, **args)
            result = await activate_portfolio_target(
                session,
                account_id=proposal.account_id,
                strategy_decision_id=proposal.decision_id,
                request_time=args["now"] + timedelta(seconds=1),
                idempotency_key="activate-and-plan",
            )
            assert result.result_code == "PLAN_CREATED"
            assert result.batch_id is not None
            batch = await session.get(PaperOrderBatchRecord, result.batch_id)
            assert batch.target_version_id == result.target_version_id
            assert batch.strategy_decision_id == proposal.decision_id
            assert (
                await session.scalar(select(func.count()).select_from(PaperTargetVersionRecord))
                == 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord)) == 1
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("occupation", ["cash", "position", "unknown_cancel"])
async def test_occupied_projection_blocks_activation_without_deleting_proposal(
    migrated_database_url, monkeypatch, occupation
):
    from nexus.paper import target_service
    from nexus.paper.projection import load_paper_projection
    from nexus.risk.models import PaperOpenOrder, PaperPositionBalance

    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            proposal = await persist_portfolio_proposal(session, **args)
            base = await load_paper_projection(
                session,
                account_id=proposal.account_id,
                as_of=args["now"] + timedelta(seconds=1),
            )
        instrument_id = next(iter(proposal.proposal.weights))
        update = {}
        if occupation == "cash":
            update = {
                "available_cash": base.available_cash - Decimal(1),
                "reserved_cash": Decimal(1),
            }
        elif occupation == "position":
            update = {
                "positions": (
                    PaperPositionBalance(
                        instrument_id=instrument_id,
                        quantity=Decimal(1),
                        reserved_quantity=Decimal(1),
                        cost_basis=Decimal(100),
                        reference_close=Decimal(100),
                    ),
                )
            }
        else:
            update = {
                "available_cash": base.available_cash - Decimal(100),
                "reserved_cash": Decimal(100),
                "open_orders": (
                    PaperOpenOrder(
                        order_id=uuid4(),
                        instrument_id=instrument_id,
                        side="buy",
                        quantity=Decimal(1),
                        worst_price=Decimal(100),
                        reserved_cash=Decimal(100),
                        reserved_quantity=Decimal(0),
                        status="reconciling",
                    ),
                ),
            }
        occupied = base.model_copy(update=update)

        async def load_occupied(*args, **kwargs):
            return occupied

        monkeypatch.setattr(target_service, "load_paper_projection", load_occupied)
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="^INITIAL_PROJECTION_OCCUPIED_OR_UNKNOWN$"):
                await activate_portfolio_target(
                    session,
                    account_id=proposal.account_id,
                    strategy_decision_id=proposal.decision_id,
                    request_time=args["now"] + timedelta(seconds=1),
                    idempotency_key=f"occupied-{occupation}",
                )
        async with sessions.begin() as session:
            assert await session.get(StrategyDecisionRecord, proposal.decision_id)
            assert (
                await session.scalar(select(func.count()).select_from(PaperTargetVersionRecord))
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_activation_creates_one_target(migrated_database_url):
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            proposal = await persist_portfolio_proposal(session, **args)

        async def activate(key):
            async with sessions.begin() as session:
                return await activate_portfolio_target(
                    session,
                    account_id=proposal.account_id,
                    strategy_decision_id=proposal.decision_id,
                    request_time=args["now"] + timedelta(seconds=1),
                    idempotency_key=key,
                )

        left, right = await asyncio.gather(activate("concurrent-a"), activate("concurrent-b"))
        assert left.target_version_id == right.target_version_id
        async with sessions.begin() as session:
            assert (
                await session.scalar(select(func.count()).select_from(PaperTargetVersionRecord))
                == 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord)) == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_batch_failure_rolls_back_target_insert(migrated_database_url, monkeypatch):
    import integration.test_portfolio_synthesis_persistence_pg as seed_module
    from nexus.paper import service

    original_intent = seed_module._safe_no_trade_intent

    def complete_buy(**kwargs):
        source = original_intent(**kwargs)
        assessment = source.synthesis_assessment.model_copy(
            update={
                "stance": "buy_candidate",
                "review_conclusion": "sufficient",
                "reviews": tuple(
                    review.model_copy(
                        update={
                            "resolved": True,
                            "status": "accepted",
                            "missing_evidence": (),
                            "prevented_conclusion": None,
                        }
                    )
                    for review in source.synthesis_assessment.reviews
                ),
            }
        )
        return source.model_copy(
            update={
                "quality_status": "complete",
                "stance": "buy_candidate",
                "exposure_action": "eligible_for_increase",
                "evidence_audit": "complete",
                "synthesis_assessment": assessment,
                "role_votes": tuple(
                    vote.model_copy(update={"vote": "support"}) for vote in source.role_votes
                ),
            }
        )

    monkeypatch.setattr(seed_module, "_safe_no_trade_intent", complete_buy)
    from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis

    seeded = await _seed_normal_synthesis(migrated_database_url, monkeypatch)
    engine, sessions, args = await proposal_source(
        migrated_database_url, seeded=seeded, intent_factory=_include_first
    )
    try:
        async with sessions.begin() as session:
            proposal = await persist_portfolio_proposal(session, **args)

        async def fail_batch(*args, **kwargs):
            raise RuntimeError("injected batch failure")

        monkeypatch.setattr(service, "_create_target_aware_paper_order_batch", fail_batch)
        with pytest.raises(RuntimeError, match="injected batch failure"):
            async with sessions.begin() as session:
                await activate_portfolio_target(
                    session,
                    account_id=proposal.account_id,
                    strategy_decision_id=proposal.decision_id,
                    request_time=args["now"] + timedelta(seconds=1),
                    idempotency_key="rollback-after-target",
                )
        async with sessions.begin() as session:
            assert (
                await session.scalar(select(func.count()).select_from(PaperTargetVersionRecord))
                == 0
            )
            assert (
                await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord)) == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_candidate_cycle_activates_target_and_completes_without_orders(
    migrated_database_url,
):
    from nexus.paper.agent_cycle import _events, _resume_research
    from nexus.persistence.models.paper import AgentPaperCycleRecord

    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            result = await _resume_research(
                session,
                cycle=cycle,
                authority=None,
                events=await _events(session, cycle.id),
                now=args["now"],
            )
            assert result.status == "completed"
            assert result.result_code == "NO_ORDER_REQUIRED"
            assert result.strategy_decision_id is not None
            assert (
                await session.scalar(select(func.count()).select_from(PaperTargetVersionRecord))
                == 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord)) == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_portfolio_cycle_cannot_use_unallowlisted_direct_completion_code(
    migrated_database_url,
):
    from nexus.paper.agent_cycle import _append_event, _events
    from nexus.persistence.models.paper import AgentPaperCycleRecord

    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            proposal = await persist_portfolio_proposal(session, **args)
            await _append_event(
                session,
                cycle,
                "researching",
                "decision_recorded",
                "TARGET_ACTIVATION_PENDING",
                {"strategy_decision_id": str(proposal.decision_id)},
            )
            events = await _events(session, cycle.id)
            assert events[-1].to_status == "decision_recorded"
            with pytest.raises(DBAPIError):
                await _append_event(
                    session,
                    cycle,
                    "decision_recorded",
                    "completed",
                    "DECISION_COMPLETED",
                    {"strategy_decision_id": str(proposal.decision_id)},
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_cycle_cannot_use_portfolio_direct_completion_exception(
    migrated_database_url,
):
    from nexus.paper.agent_cycle import _append_event
    from nexus.paper.agent_decision import AGENT_PAPER_SIZING_POLICY_HASH
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from sqlalchemy import text

    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            proposal = await persist_portfolio_proposal(session, **args)
            await session.execute(text("ALTER TABLE agent_paper_cycles DISABLE TRIGGER USER"))
            await session.execute(
                text(
                    "UPDATE agent_paper_cycles SET candidate_batch_id=NULL, "
                    "candidate_manifest='[]'::jsonb, candidate_hash=:candidate_hash, "
                    "sizing_policy_version='agent_paper_sizing_v1', "
                    "sizing_policy_hash=:sizing_hash WHERE id=:cycle_id"
                ),
                {
                    "candidate_hash": "a" * 64,
                    "sizing_hash": AGENT_PAPER_SIZING_POLICY_HASH,
                    "cycle_id": cycle.id,
                },
            )
            await session.execute(text("ALTER TABLE agent_paper_cycles ENABLE TRIGGER USER"))
            await _append_event(
                session,
                cycle,
                "researching",
                "decision_recorded",
                "TARGET_ACTIVATION_PENDING",
                {"strategy_decision_id": str(proposal.decision_id)},
            )
            with pytest.raises(DBAPIError):
                await _append_event(
                    session,
                    cycle,
                    "decision_recorded",
                    "completed",
                    "NO_ORDER_REQUIRED",
                    {"strategy_decision_id": str(proposal.decision_id)},
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_blocked_candidate_cycle_never_activates_same_proposal_after_release(
    migrated_database_url, monkeypatch
):
    from nexus.paper import target_service
    from nexus.paper.agent_cycle import (
        _append_event,
        _events,
        _resume_batch,
    )
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.paper import AgentPaperCycleRecord

    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            proposal = await persist_portfolio_proposal(session, **args)
            await _append_event(
                session,
                cycle,
                "researching",
                "decision_recorded",
                "TARGET_ACTIVATION_PENDING",
                {"strategy_decision_id": str(proposal.decision_id)},
            )
            base = await load_paper_projection(
                session, account_id=cycle.account_id, as_of=args["now"]
            )
        occupied = base.model_copy(
            update={
                "available_cash": base.available_cash - Decimal(1),
                "reserved_cash": Decimal(1),
            }
        )

        async def load_occupied(*args, **kwargs):
            return occupied

        monkeypatch.setattr(target_service, "load_paper_projection", load_occupied)
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            blocked = await _resume_batch(
                session,
                cycle=cycle,
                events=await _events(session, cycle.id),
                now=args["now"],
            )
            assert blocked.status == "completed"
            assert blocked.result_code == "TARGET_ACTIVATION_BLOCKED"
        monkeypatch.setattr(target_service, "load_paper_projection", load_paper_projection)
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            replay = await _resume_batch(
                session,
                cycle=cycle,
                events=await _events(session, cycle.id),
                now=args["now"] + timedelta(minutes=1),
            )
            assert replay.result_code == "TARGET_ACTIVATION_BLOCKED"
            assert (
                await session.scalar(select(func.count()).select_from(PaperTargetVersionRecord))
                == 0
            )
    finally:
        await engine.dispose()
