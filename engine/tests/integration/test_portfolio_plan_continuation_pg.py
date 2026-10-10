"""Continuation uses the original target, actual terminal execution, and native risk."""

import pytest
from nexus.paper import target_service
from nexus.paper.agent_decision_store import persist_portfolio_proposal
from nexus.persistence.models.paper import PaperOrderBatchRecord, PaperOrderRecord
from sqlalchemy import func, select

from engine.tests.integration.test_paper_target_activation_pg import tradeable_proposal_source


@pytest.mark.asyncio
async def test_continuation_cannot_spend_pending_orders(migrated_database_url, monkeypatch):
    engine, sessions, args = await tradeable_proposal_source(migrated_database_url, monkeypatch)
    try:
        async with sessions.begin() as session:
            proposal = await persist_portfolio_proposal(session, **args)
            root = await target_service.activate_portfolio_target(
                session,
                account_id=proposal.account_id,
                strategy_decision_id=proposal.decision_id,
                request_time=args["now"],
                idempotency_key="continuation-root",
            )
        assert hasattr(target_service, "continue_portfolio_plan"), (
            "normal continuation entry missing"
        )
        async with sessions.begin() as session:
            before = await session.scalar(select(func.count()).select_from(PaperOrderRecord))
            with pytest.raises(ValueError, match="PORTFOLIO_PLAN_NOT_READY"):
                await target_service.continue_portfolio_plan(
                    session,
                    account_id=proposal.account_id,
                    previous_batch_id=root.batch_id,
                    candidate_stage_capacity=False,
                    formal_stage_capacity=False,
                )
            assert (
                await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord)) == 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == before
            )
            from uuid import uuid4

            from sqlalchemy.exc import DBAPIError

            parent = await session.get(PaperOrderBatchRecord, root.batch_id)
            values = {
                c.name: getattr(parent, c.name) for c in PaperOrderBatchRecord.__table__.columns
            }
            values.update(id=uuid4(), previous_batch_id=parent.id)
            with pytest.raises(DBAPIError, match="PORTFOLIO_PLAN_NOT_READY"):
                async with session.begin_nested():
                    session.add(PaperOrderBatchRecord(**values))
                    await session.flush()
    finally:
        await engine.dispose()


async def verify_sale_continuation(
    sessions,
    url,
    monkeypatch,
    clock,
    identity,
    binding,
    actual,
    tmp_path,
    *,
    rejected_buy=False,
    execute_plan=False,
    partial_plan=False,
    late_plan=False,
    boundary_case="",
    source_before_slot=False,
):
    from datetime import timedelta
    from decimal import Decimal

    from helpers.paper_batch import INSTRUMENTS
    from nexus.ops import economic_cycle as cli
    from nexus.paper.economic_collection import collect_economic_window
    from nexus.paper.economic_cycle import start_economic_pair
    from nexus.paper.economic_window import freeze_common_window
    from nexus.paper.service import load_paper_order_batch_result
    from nexus.persistence.models.economic import EconomicPairSlotRecord
    from nexus.persistence.models.paper import PaperFillRecord, StrategyDecisionRecord
    from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage

    from engine.tests.integration.test_economic_cycle_pg import (
        append_second_day_sources,
        complete_controlled_pair,
    )

    async with sessions.begin() as session:
        pair = await session.scalar(
            select(EconomicPairSlotRecord).where(
                EconomicPairSlotRecord.evaluation_id == identity.id,
                EconomicPairSlotRecord.ordinal == 2,
            )
        )
    prices = {
        INSTRUMENTS[0][0]: Decimal("20000" if rejected_buy else "10000"),
        INSTRUMENTS[1][0]: INSTRUMENTS[1][1],
    }
    # Mark actual first-day holdings at that day's lawful close, before Monday.
    from integration.test_agent_paper_cycle_pg import _ObjectStore
    from nexus.market.ingestion import ingest_price_page
    from nexus.paper.performance import publish_paper_performance_snapshot
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
    from nexus.persistence.models.paper import PAPER_DECISION_SOURCE_POLICY_ID

    async with sessions.begin() as session:
        close = await session.scalar(
            select(ExchangeCalendarSessionRecord.market_close_at).where(
                ExchangeCalendarSessionRecord.session == pair.decision_at.date() - timedelta(days=3)
            )
        )
        clock[0] = close + timedelta(seconds=1)
        for instrument_id, price in prices.items():
            page = ProviderPage(
                items=(
                    PriceBar(
                        instrument_id=instrument_id,
                        session=close.date(),
                        open=price,
                        high=price,
                        low=price,
                        close=price,
                        volume=Decimal("1000000"),
                        observed_at=close,
                        available_at=close,
                        received_at=close,
                    ),
                ),
                next_cursor=None,
                raw_payload=f"synthetic-close:{instrument_id}:{close}".encode(),
                source_request_id=None,
                as_of=close,
                provenance=MarketDataProvenance(
                    source="alpaca",
                    feed="sip",
                    adjustment="all",
                    interval="1Day",
                    mode="delayed",
                    delivery_mode="delayed",
                ),
            )
            await ingest_price_page(
                session,
                _ObjectStore(),
                page,
                commit=False,
                source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
            )
        for account_id in (identity.incumbent_account_id, identity.challenger_account_id):
            await publish_paper_performance_snapshot(
                session, account_id=account_id, as_of=close, enqueue_session_summary=False
            )
    clock[0] = pair.decision_at + timedelta(seconds=-1 if source_before_slot else 1)
    await append_second_day_sources(sessions, pair.decision_at, prices=prices)
    clock[0] = pair.decision_at + timedelta(seconds=1)
    async with sessions.begin() as session:
        started = await start_economic_pair(session, pair_slot_id=pair.id, expected_database=actual)
    await complete_controlled_pair(
        sessions,
        url,
        monkeypatch,
        clock,
        started.cycle_ids,
        binding,
        "rotate",
        pair.id,
        actual,
        decision_time=pair.decision_at,
        synthesis_variant=boundary_case,
    )
    async with sessions.begin() as session:
        parents = tuple(
            await session.scalars(
                select(PaperOrderBatchRecord)
                .join(
                    StrategyDecisionRecord,
                    StrategyDecisionRecord.id == PaperOrderBatchRecord.strategy_decision_id,
                )
                .where(StrategyDecisionRecord.agent_paper_cycle_id.in_(started.cycle_ids))
            )
        )
        assert len(parents) == 2
        old_hashes = {p.id: p.result_hash for p in parents}
        root_orders = tuple(
            await session.scalars(
                select(PaperOrderRecord).where(PaperOrderRecord.batch_id.in_(old_hashes))
            )
        )
        assert {o.side for o in root_orders} == (
            {"sell"} if boundary_case == "both_not_required" else {"sell", "buy"}
        )
        assert boundary_case == "both_not_required" or any(
            o.side == "buy" and o.risk_outcome == ("rejected" if rejected_buy else "reduced")
            for o in root_orders
        ), [(o.side, str(o.quantity), o.risk_outcome) for o in root_orders]
        if rejected_buy:
            from nexus.paper import economic_window as windows

            original_root_orders = windows._root_orders

            async def only_rejected(current_session, decision_id):
                return tuple(
                    order
                    for order in await original_root_orders(current_session, decision_id)
                    if order.risk_outcome == "rejected"
                )

            # Fault injection returns actual immutable rejected orders only;
            # this must never be classified as evidence-based no-trade.
            with monkeypatch.context() as scoped:
                scoped.setattr(windows, "_root_orders", only_rejected)
                with pytest.raises(ValueError, match="ECONOMIC_WINDOW_NOT_READY"):
                    await freeze_common_window(session, pair_slot_id=pair.id, window_seq=1)
        window = await freeze_common_window(session, pair_slot_id=pair.id, window_seq=1)
    clock[0] = window.window_end + timedelta(seconds=46)

    root_window_end = window.window_end

    class Provider:
        async def fetch_bars(self, request, cursor=None):
            price = prices[request.instrument_id]
            received = window.window_end + timedelta(seconds=45)
            return ProviderPage(
                items=tuple(
                    PriceBar(
                        instrument_id=request.instrument_id,
                        session=window.window_start.date(),
                        open=price,
                        high=price + 1,
                        low=price - 1,
                        close=price,
                        volume=Decimal(
                            "20"
                            if partial_plan and window.window_start > root_window_end
                            else "1000000"
                        ),
                        observed_at=window.window_start + timedelta(minutes=i),
                        available_at=received,
                        received_at=received,
                    )
                    for i in range(5)
                ),
                next_cursor=None,
                raw_payload=f"{request.instrument_id}:{window.window_end.isoformat()}".encode(),
                source_request_id=None,
                as_of=received,
                provenance=MarketDataProvenance(
                    source="schwab",
                    feed="price_history",
                    interval="1Min",
                    adjustment="raw",
                    mode="read_only",
                    delivery_mode="unknown",
                ),
            )

    class Store:
        def __init__(self):
            self.raw = {}

        async def archive_raw(self, key, payload):
            self.raw[key] = payload
            return "memory://" + key

        async def read_raw(self, key, *, uri):
            assert uri == "memory://" + key
            return self.raw[key]

        async def put_normalized(self, key, payload):
            return "memory://" + key

    collected = await collect_economic_window(
        sessions,
        pair_slot_id=pair.id,
        window_seq=1,
        provider=Provider(),
        object_store=Store(),
    )
    assert collected.status == "archives_ready"
    config = tmp_path / "plan-continuation-url"
    config.write_text(url)
    config.chmod(0o600)
    kwargs = dict(database_url_file=config, expected_database=actual, pair_slot_id=pair.id)
    completed = await cli.execute_advance(**kwargs)
    assert completed["status"] == "completed"
    if rejected_buy:
        from nexus.persistence.models.economic import EconomicWindowOrderRecord
        from nexus.persistence.models.paper import PaperOrderEventRecord

        async with sessions.begin() as session:
            selected = set(
                await session.scalars(
                    select(EconomicWindowOrderRecord.order_id).where(
                        EconomicWindowOrderRecord.window_id == window.window_id
                    )
                )
            )
            assert selected == {o.id for o in root_orders if o.side == "sell"}
            for order in root_orders:
                latest = await session.scalar(
                    select(PaperOrderEventRecord.to_status)
                    .where(PaperOrderEventRecord.order_id == order.id)
                    .order_by(PaperOrderEventRecord.event_seq.desc())
                    .limit(1)
                )
                assert latest == ("filled" if order.side == "sell" else "risk_rejected")
            assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 6
            replay = await freeze_common_window(session, pair_slot_id=pair.id, window_seq=1)
            assert replay.replayed and replay.window_id == window.window_id
            from copy import deepcopy
            from types import SimpleNamespace

            from nexus.paper.economic_window import _hash, _read_window
            from nexus.paper.projection import load_paper_projection
            from nexus.persistence.models.economic import EconomicCommonWindowRecord

            persisted = await session.get(EconomicCommonWindowRecord, window.window_id)
            assert all(
                len(lane["rejected_orders"]) == 1 for lane in persisted.qualification["lanes"]
            )
            for mode in ("omit", "wrong_event", "wrong_hash"):
                document = deepcopy(persisted.qualification)
                lane = document["lanes"][0]
                if mode == "omit":
                    del lane["rejected_orders"]
                elif mode == "wrong_event":
                    lane["rejected_orders"][0]["event_seq"] = 1
                else:
                    lane["rejected_orders"][0]["event_hash"] = "0" * 64
                altered = SimpleNamespace(
                    **{
                        col.name: getattr(persisted, col.name)
                        for col in EconomicCommonWindowRecord.__table__.columns
                    }
                )
                altered.qualification = document
                altered.content_hash = _hash(document)
                with pytest.raises(ValueError, match="ECONOMIC_WINDOW_INVALID"):
                    await _read_window(session, altered)
            for parent in parents:
                projection = await load_paper_projection(
                    session, account_id=parent.account_id, as_of=clock[0]
                )
                assert not projection.open_orders
        assert (await cli.execute_advance(**kwargs)) == {**completed, "replayed": True}
        return
    from nexus.paper.projection import load_paper_projection

    post_sale = {}
    async with sessions.begin() as session:
        for parent in parents:
            post_sale[parent.account_id] = await load_paper_projection(
                session, account_id=parent.account_id, as_of=clock[0]
            )
            assert not post_sale[parent.account_id].open_orders
    if boundary_case:
        from engine.tests.integration.test_economic_stage_boundary_pg import verify_stage_boundary

        async def collect_child_stage(current):
            nonlocal window
            window = current
            return await collect_economic_window(
                sessions,
                pair_slot_id=pair.id,
                window_seq=2,
                provider=Provider(),
                object_store=Store(),
            )

        await verify_stage_boundary(
            sessions,
            clock,
            pair.id,
            kwargs,
            parents,
            post_sale,
            collect_child_stage,
            boundary_case,
            completed,
        )
        return
    from nexus.paper.agent_decision_store import load_portfolio_proposal

    async with sessions.begin() as session:
        proposal = await load_portfolio_proposal(
            session, account_id=parents[0].account_id, decision_id=parents[0].strategy_decision_id
        )
    available_time = clock[0]
    clock[0] = proposal.proposal.expires_at
    with pytest.raises(ValueError, match="PORTFOLIO_PROPOSAL_EXPIRED"):
        await cli.execute_continue_plans(**kwargs)
    clock[0] = available_time
    from nexus.paper import economic_plan

    original_continue = economic_plan.continue_portfolio_plan
    attempts = [0]

    async def fail_peer(*args, **options):
        attempts[0] += 1
        if attempts[0] == 2:
            raise ValueError("TEST_PLAN_PEER_FAILURE")
        return await original_continue(*args, **options)

    with monkeypatch.context() as scoped:
        scoped.setattr(economic_plan, "continue_portfolio_plan", fail_peer)
        with pytest.raises(ValueError, match="TEST_PLAN_PEER_FAILURE"):
            await cli.execute_continue_plans(**kwargs)
    async with sessions.begin() as session:
        assert await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord)) == 4
        assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 8
    result = await cli.execute_continue_plans(**kwargs)
    assert result["execution_locked"] is True
    assert {item["status"] for item in result["lanes"]} == {"created"}
    async with sessions.begin() as session:
        children = tuple(
            await session.scalars(
                select(PaperOrderBatchRecord).where(
                    PaperOrderBatchRecord.previous_batch_id.in_(old_hashes)
                )
            )
        )
        assert len(children) == 2
        assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 8
        for child in children:
            parent = next(p for p in parents if p.id == child.previous_batch_id)
            assert child.account_id == parent.account_id
            assert child.projection_hash == post_sale[child.account_id].projection_hash
            assert child.target_version_id == parent.target_version_id
            assert child.strategy_decision_id == parent.strategy_decision_id
            loaded = await load_paper_order_batch_result(
                session,
                account_id=child.account_id,
                strategy_decision_id=child.strategy_decision_id,
                batch_id=child.id,
            )
            assert loaded.batch_id == child.id
            original = await load_paper_order_batch_result(
                session,
                account_id=parent.account_id,
                strategy_decision_id=parent.strategy_decision_id,
            )
            assert original.batch_id == parent.id
            assert (await session.get(PaperOrderBatchRecord, parent.id)).result_hash == old_hashes[
                parent.id
            ]
        orders = tuple(
            await session.scalars(
                select(PaperOrderRecord).where(
                    PaperOrderRecord.batch_id.in_([c.id for c in children])
                )
            )
        )
        assert len(orders) == 2 and all(
            o.side == "buy" and o.risk_outcome == "approved" for o in orders
        )
    # A fresh child cannot borrow the original order's consumed native window.
    from nexus.paper.execution_service import advance_internal_paper_order

    async with sessions.begin() as session:
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW"):
            await advance_internal_paper_order(
                session,
                order_id=orders[0].id,
                idempotency_key="child-cannot-use-root-window",
                evaluated_at=clock[0],
            )
    if execute_plan and not late_plan:
        from nexus.persistence.models.agents import AgentModelConfigRecord

        async with sessions() as session:
            model = await session.get(AgentModelConfigRecord, binding.payload["shared_model_id"])
            expires = model.pricing_expires_at
        current_time = clock[0]
        clock[0] = expires
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_SOURCE_EXPIRED"):
            await cli.execute_freeze_next(**kwargs)
        clock[0] = current_time
    if late_plan:
        clock[0] += timedelta(minutes=2)
        missed = await cli.execute_freeze_next(**kwargs)
        assert missed["status"] == "missed"
        clock[0] += timedelta(minutes=10)
        assert (await cli.execute_freeze_next(**kwargs)) == {**missed, "replayed": True}
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_MISSED"):
            await collect_economic_window(
                sessions,
                pair_slot_id=pair.id,
                window_seq=2,
                provider=Provider(),
                object_store=Store(),
            )
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_MISSED"):
            await cli.execute_advance(**kwargs)
        assert (await cli.execute_continue_plans(**kwargs)) == result
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 8
        return
    frozen_child = await cli.execute_freeze_next(**kwargs)
    assert frozen_child["status"] == "frozen"
    if execute_plan:
        from nexus.persistence.models.economic import (
            EconomicCommonWindowRecord,
            EconomicWindowOrderRecord,
        )

        async with sessions.begin() as session:
            row = await session.scalar(
                select(EconomicCommonWindowRecord).where(
                    EconomicCommonWindowRecord.pair_slot_id == pair.id,
                    EconomicCommonWindowRecord.window_seq == 2,
                )
            )
            window = await freeze_common_window(session, pair_slot_id=pair.id, window_seq=2)
            assert window.replayed
            linked = set(
                await session.scalars(
                    select(EconomicWindowOrderRecord.order_id).where(
                        EconomicWindowOrderRecord.window_id == row.id
                    )
                )
            )
            assert linked == {order.id for order in orders}
            from engine.tests.integration.test_economic_plan_window_guards_pg import (
                verify_plan_window_guards,
            )

            await verify_plan_window_guards(session, row, root_orders)
            assert (
                row.window_start > row.eligible_at >= max(child.evaluated_at for child in children)
            )
        clock[0] = window.window_end + timedelta(seconds=46)
        assert (
            await collect_economic_window(
                sessions,
                pair_slot_id=pair.id,
                window_seq=2,
                provider=Provider(),
                object_store=Store(),
            )
        ).status == "archives_ready"
        first_child_execution = await cli.execute_advance(**kwargs)
        assert first_child_execution["status"] == ("partial" if partial_plan else "completed")
        if partial_plan:
            next_child = await cli.execute_freeze_next(**kwargs)
            assert next_child["status"] == "frozen"
            async with sessions.begin() as session:
                window = await freeze_common_window(session, pair_slot_id=pair.id, window_seq=3)
            partial_plan = False
            clock[0] = window.window_end + timedelta(seconds=46)
            assert (
                await collect_economic_window(
                    sessions,
                    pair_slot_id=pair.id,
                    window_seq=3,
                    provider=Provider(),
                    object_store=Store(),
                )
            ).status == "archives_ready"
            assert (await cli.execute_advance(**kwargs))["status"] == "completed"
        assert (await cli.execute_freeze_next(**kwargs))["status"] == "not_required"
        async with sessions.begin() as session:
            historical = await freeze_common_window(session, pair_slot_id=pair.id, window_seq=2)
            assert historical.replayed
            for parent in parents:
                projection = await load_paper_projection(
                    session, account_id=parent.account_id, as_of=clock[0]
                )
                assert not projection.open_orders
            assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) >= 10
    from nexus.broker_simulation.intents import BrokerIntentError, load_agent_paper_intent_source
    from nexus.paper.agent_challenger import (
        AgentChallengerError,
        collect_portfolio_challenger_cycle_sample,
    )

    async with sessions.begin() as session:
        with pytest.raises(BrokerIntentError, match="AGENT_PAPER_SOURCE_NOT_READY"):
            await load_agent_paper_intent_source(
                session, cycle_id=started.cycle_ids[0], now=clock[0]
            )
        with pytest.raises(AgentChallengerError, match="PORTFOLIO_CHALLENGER_SAMPLE_INCOMPLETE"):
            await collect_portfolio_challenger_cycle_sample(
                session, cycle_id=started.cycle_ids[0], as_of=clock[0]
            )
    replay = await cli.execute_continue_plans(**kwargs)
    assert replay == result
    from nexus.persistence.models.runtime import Job

    async with sessions.begin() as session:
        session.add(
            Job(
                job_key="foreign-after-child",
                input_hash="a" * 64,
                status="queued",
                schema_version="test_v1",
                payload={},
                job_type="test.unrelated",
                queue="general",
                scope_kind="global",
                scope_id="global",
                effective_at=clock[0],
                priority=1,
                retry_policy="never",
                max_attempts=1,
                available_at=clock[0],
            )
        )
    assert await cli.execute_continue_plans(**kwargs) == result
    # Actual console transport reads this exact persisted child pair, without
    # exposing a caller clock or retrying research/provider/consumption.
    import asyncio
    import json
    import subprocess
    import sys
    from pathlib import Path

    completed_cli = await asyncio.to_thread(
        subprocess.run,
        [
            str(Path(sys.executable).with_name("nexus-engine-economic-cycle")),
            "continue-plans",
            "--database-url-file",
            str(config),
            "--expected-database",
            actual,
            "--pair-slot-id",
            str(pair.id),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed_cli.returncode == 0 and not completed_cli.stderr
    assert json.loads(completed_cli.stdout) == result
    # Nonempty continuation facts prohibit destructive schema rollback.
    from alembic import command
    from alembic.config import Config
    from sqlalchemy.exc import DBAPIError

    monkeypatch.setenv("DATABASE_URL", url)
    migration_config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    with pytest.raises(DBAPIError, match="ECONOMIC_WINDOW_DOWNGRADE_FORBIDDEN"):
        await asyncio.to_thread(command.downgrade, migration_config, "0072_economic_later_windows")
    async with sessions.begin() as session:
        assert await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord)) == 6
        assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == (
            10
            if execute_plan and not first_child_execution["status"] == "partial"
            else 12
            if execute_plan
            else 8
        )
