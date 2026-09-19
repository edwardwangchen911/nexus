from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from helpers.paper_api import app, headers
from helpers.paper_batch import CALENDAR_ID
from integration.test_agent_paper_cycle_pg import (
    _IexProvider,
    _ObjectStore,
    _SchwabPriceHistoryProvider,
)
from integration.test_portfolio_proposal_pg import proposal_source
from integration.test_portfolio_synthesis_persistence_pg import synthesis_intent
from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis
from nexus.market.ingestion import ingest_price_page
from nexus.ops import agent_paper_loop
from nexus.ops import agent_paper_once as agent_paper_loop_once
from nexus.paper.agent_cycle import _events, _resume_portfolio_synthesis
from nexus.paper.agent_decision_store import load_portfolio_proposal
from nexus.paper.agent_market import collect_agent_paper_window, resolve_agent_paper_market_scope
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.paper.performance import publish_paper_performance_snapshot
from nexus.persistence.models.agents import AgentPortfolioSynthesisOutputRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.broker_simulation import (
    BrokerBatchApprovalRecord,
    BrokerCommandAttemptRecord,
    BrokerCommandRecord,
)
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    AgentPaperMarketWindowRecord,
    PaperAccountRecord,
    PaperExecutionWindowAuthorityRecord,
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperReservationRecord,
    PaperTargetVersionRecord,
)
from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
from sqlalchemy import func, select


async def _business_facts(session):
    """Exact persisted facts; HTTP authentication nonces are deliberately excluded."""
    facts = {}
    for model in (
        PaperOrderRecord,
        PaperOrderEventRecord,
        PaperFillRecord,
        PaperLedgerTransactionRecord,
        PaperLedgerEntryRecord,
        PaperReservationRecord,
        PaperTargetVersionRecord,
        PaperOrderBatchRecord,
        AgentPaperMarketWindowRecord,
        PaperExecutionWindowAuthorityRecord,
        BrokerBatchApprovalRecord,
        BrokerCommandRecord,
        BrokerCommandAttemptRecord,
    ):
        table = model.__table__
        facts[table.name] = tuple(
            (await session.execute(select(table).order_by(*table.primary_key.columns))).all()
        )
    return facts


def _include_two(run):
    from nexus.agents.portfolio_synthesis_contracts import PortfolioSynthesisIntentV1

    document = synthesis_intent(run).model_dump(mode="json")
    for rank, choice in enumerate(document["choices"][:2], start=1):
        choice.update(
            action="include",
            # Two tactical 5% targets fit the frozen 10% turnover cap and
            # therefore exercise two real orders in one activated plan.
            role="tactical",
            rank=rank,
        )
    return PortfolioSynthesisIntentV1.model_validate(document)


class _PartialSchwabProvider(_IexProvider):
    async def fetch_bars(self, request, cursor=None):  # type: ignore[no-untyped-def]
        page = await super().fetch_bars(request, cursor)
        return page.model_copy(
            update={
                "raw_payload": (
                    f'{{"candles":"portfolio-partial-{request.instrument_id}"}}'.encode()
                ),
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


async def _publish_portfolio_close(
    sessions,
    *,
    cycle: AgentPaperCycleRecord,
    orders: tuple[PaperOrderRecord, ...],
) -> object:
    """Freeze complete adjusted daily marks before reading the actual projection."""
    from nexus.persistence.models.paper import PAPER_DECISION_SOURCE_POLICY_ID

    async with sessions.begin() as session:
        market_close = await session.scalar(
            select(ExchangeCalendarSessionRecord.market_close_at).where(
                ExchangeCalendarSessionRecord.calendar_authority_id == CALENDAR_ID,
                ExchangeCalendarSessionRecord.session == cycle.execution_session,
            )
        )
        assert market_close is not None
        for index, order in enumerate(orders, start=1):
            await ingest_price_page(
                session,
                _ObjectStore(),
                ProviderPage(
                    items=(
                        PriceBar(
                            instrument_id=order.instrument_id,
                            session=cycle.execution_session,
                            open=Decimal("100"),
                            high=Decimal("100"),
                            low=Decimal("100"),
                            close=Decimal("100"),
                            volume=Decimal("1000000"),
                            observed_at=market_close,
                            available_at=market_close,
                            received_at=market_close,
                            revision=f"task13-close-{index}",
                            quality_flags=(),
                        ),
                    ),
                    next_cursor=None,
                    raw_payload=f'{{"task13_close":{index}}}'.encode(),
                    source_request_id=None,
                    as_of=market_close,
                    provenance=MarketDataProvenance(
                        source="alpaca",
                        feed="sip",
                        adjustment="all",
                        interval="1Day",
                        mode="delayed",
                        delivery_mode="delayed",
                    ),
                ),
                commit=False,
                source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
            )
        await publish_paper_performance_snapshot(
            session,
            account_id=cycle.account_id,
            as_of=market_close,
        )
    return market_close


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "first_partial",
    [True, False],
    ids=["partial-then-second", "two-full-fills"],
)
async def test_normal_host_advances_second_schwab_order_after_first_partial_window(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    first_partial: bool,
) -> None:
    """A consumed partial Schwab order must not starve the next submitted order."""
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
    seeded = await _seed_normal_synthesis(migrated_database_url, monkeypatch)
    engine, sessions, args = await proposal_source(
        migrated_database_url,
        seeded=seeded,
        intent_factory=_include_two,
    )
    try:
        async with sessions.begin() as session:
            output = await session.get(
                AgentPortfolioSynthesisOutputRecord,
                args["synthesis_output_id"],
            )
            assert output is not None and output.status == "completed", output
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            assert cycle is not None
            result = await _resume_portfolio_synthesis(
                session,
                cycle=cycle,
                events=await _events(session, cycle.id),
                run_ids=(),
                now=args["now"] + timedelta(seconds=1),
                daily_limit_usd=None,
            )
            assert result.status == "awaiting_market", result
            assert result.strategy_decision_id is not None
            proposal = await load_portfolio_proposal(
                session,
                account_id=cycle.account_id,
                decision_id=result.strategy_decision_id,
            )
            assert proposal is not None
            batch_id = await session.scalar(
                select(PaperOrderBatchRecord.id).where(
                    PaperOrderBatchRecord.strategy_decision_id == result.strategy_decision_id
                )
            )
            assert batch_id is not None
            orders = tuple(
                await session.scalars(
                    select(PaperOrderRecord)
                    .where(PaperOrderRecord.batch_id == batch_id)
                    .order_by(PaperOrderRecord.id)
                )
            )
            assert len(orders) == 2

        first_order, second_order = orders
        async with sessions() as session:
            first_scope = await resolve_agent_paper_market_scope(
                session,
                cycle_id=cycle.id,
                instrument_id=first_order.instrument_id,
            )
        evaluated_at = first_scope.window_end + timedelta(seconds=2)
        await collect_agent_paper_window(
            sessions,
            cycle_id=cycle.id,
            instrument_id=first_order.instrument_id,
            provider=_PartialSchwabProvider() if first_partial else _SchwabPriceHistoryProvider(),
            object_store=_ObjectStore(),
            evaluated_at=evaluated_at,
        )
        async with sessions.begin() as session:
            partial = await advance_internal_paper_order(
                session,
                order_id=first_order.id,
                evaluated_at=evaluated_at,
                idempotency_key="task13-first-partial",
            )
            assert partial.action == ("partially_filled" if first_partial else "filled")
            assert partial.filled_quantity > Decimal(0)

        async def fixed_calendar(_session, *, now, expected_calendar_authority_id):
            return agent_paper_loop.ServiceCalendarAuthority(
                calendar_authority_id=expected_calendar_authority_id,
                as_of=now,
                local_date=now.date(),
                session=now.date(),
                decision_window_start_at=now - timedelta(hours=1),
                market_open_at=now - timedelta(hours=1),
                market_close_at=now + timedelta(hours=1),
                next_session=now.date() + timedelta(days=1),
                next_decision_window_start_at=now + timedelta(days=1),
                next_market_close_at=now + timedelta(days=1, hours=1),
            )

        monkeypatch.setattr(
            agent_paper_loop,
            "load_agent_paper_service_calendar_authority",
            fixed_calendar,
        )
        snapshot = await agent_paper_loop.inspect_agent_paper_loop(
            agent_paper_loop.EngineSettings(database_url=migrated_database_url),
            account_id=cycle.account_id,
            expected_calendar_authority_id=CALENDAR_ID,
        )

        assert snapshot.order_id == second_order.id
        assert snapshot.instrument_id == second_order.instrument_id
        assert snapshot.cycle_status == "awaiting_market"
        assert (
            agent_paper_loop.choose_loop_action(snapshot)
            == agent_paper_loop.AgentPaperLoopAction.COLLECT_WINDOW
        )

        async def collect_second(_settings, request, **_kwargs):
            assert request.cycle_id == cycle.id
            assert request.instrument_id == second_order.instrument_id
            async with sessions() as session:
                scope = await resolve_agent_paper_market_scope(
                    session,
                    cycle_id=request.cycle_id,
                    instrument_id=request.instrument_id,
                )
            return await collect_agent_paper_window(
                sessions,
                cycle_id=request.cycle_id,
                instrument_id=request.instrument_id,
                provider=_SchwabPriceHistoryProvider(),
                object_store=_ObjectStore(),
                evaluated_at=scope.window_end + timedelta(seconds=2),
            )

        monkeypatch.setattr(
            agent_paper_loop,
            "load_agent_paper_service_environment",
            lambda *_args, **_kwargs: {"DATABASE_URL": migrated_database_url},
        )
        monkeypatch.setattr(agent_paper_loop, "collect_window_once", collect_second)
        assert (
            await agent_paper_loop.execute_agent_paper_loop_action(
                agent_paper_loop.EngineSettings(database_url=migrated_database_url),
                account_id=cycle.account_id,
                repo_root=Path.cwd(),
                action=agent_paper_loop.AgentPaperLoopAction.COLLECT_WINDOW,
                snapshot=snapshot,
            )
            is None
        )

        ready = await agent_paper_loop.inspect_agent_paper_loop(
            agent_paper_loop.EngineSettings(database_url=migrated_database_url),
            account_id=cycle.account_id,
            expected_calendar_authority_id=CALENDAR_ID,
        )
        assert ready.order_id == second_order.id
        assert ready.instrument_id == second_order.instrument_id
        assert ready.market_window_ready is True
        assert (
            agent_paper_loop.choose_loop_action(ready)
            == agent_paper_loop.AgentPaperLoopAction.ADVANCE_ORDER
        )
        execution_ready = replace(ready, now=evaluated_at)
        assert (
            await agent_paper_loop.execute_agent_paper_loop_action(
                agent_paper_loop.EngineSettings(database_url=migrated_database_url),
                account_id=cycle.account_id,
                repo_root=Path.cwd(),
                action=agent_paper_loop.AgentPaperLoopAction.ADVANCE_ORDER,
                snapshot=execution_ready,
            )
            is None
        )

        async with sessions() as session:
            counts = (
                await session.scalar(select(func.count(PaperFillRecord.id))),
                await session.scalar(select(func.count(PaperExecutionWindowAuthorityRecord.id))),
                await session.scalar(select(func.count(PaperLedgerTransactionRecord.id))),
            )
            first_market_windows = await session.scalar(
                select(func.count(AgentPaperMarketWindowRecord.id)).where(
                    AgentPaperMarketWindowRecord.order_id == first_order.id
                )
            )
            second_fills = await session.scalar(
                select(func.count(PaperFillRecord.id)).where(
                    PaperFillRecord.order_id == second_order.id
                )
            )
        assert counts[:2] == (2, 2)
        assert first_market_windows == 1
        assert second_fills == 1

        assert (
            await agent_paper_loop.execute_agent_paper_loop_action(
                agent_paper_loop.EngineSettings(database_url=migrated_database_url),
                account_id=cycle.account_id,
                repo_root=Path.cwd(),
                action=agent_paper_loop.AgentPaperLoopAction.ADVANCE_ORDER,
                snapshot=execution_ready,
            )
            is None
        )
        async with sessions() as session:
            assert counts == (
                await session.scalar(select(func.count(PaperFillRecord.id))),
                await session.scalar(select(func.count(PaperExecutionWindowAuthorityRecord.id))),
                await session.scalar(select(func.count(PaperLedgerTransactionRecord.id))),
            )

            before_reads = await _business_facts(session)
            assert before_reads[BrokerBatchApprovalRecord.__tablename__] == ()
            assert before_reads[BrokerCommandRecord.__tablename__] == ()
            assert before_reads[BrokerCommandAttemptRecord.__tablename__] == ()
            imbalances = (
                await session.execute(
                    select(PaperLedgerEntryRecord.transaction_id)
                    .group_by(PaperLedgerEntryRecord.transaction_id)
                    .having(
                        func.sum(PaperLedgerEntryRecord.debit_amount)
                        != func.sum(PaperLedgerEntryRecord.credit_amount)
                    )
                )
            ).all()
            assert imbalances == []

        market_close = await _publish_portfolio_close(
            sessions,
            cycle=cycle,
            orders=orders,
        )
        api_now = market_close + timedelta(seconds=1)
        actual = await agent_paper_loop_once.inspect_projection_once(
            agent_paper_loop_once.EngineSettings(database_url=migrated_database_url),
            agent_paper_loop_once.AgentPaperInspectProjectionRequest(
                action="inspect-projection",
                account_id=cycle.account_id,
                as_of=api_now,
            ),
        )
        assert len(actual.positions) == 2
        async with sessions() as session:
            account = await session.get(PaperAccountRecord, cycle.account_id)
            fills = tuple(
                (
                    await session.scalars(
                        select(PaperFillRecord).where(
                            PaperFillRecord.account_id == cycle.account_id
                        )
                    )
                ).all()
            )
        assert account is not None
        filled_by_instrument = {
            instrument_id: sum(
                (fill.quantity for fill in fills if fill.instrument_id == instrument_id),
                start=Decimal(0),
            )
            for instrument_id in {fill.instrument_id for fill in fills}
        }
        assert {position.instrument_id: position.quantity for position in actual.positions} == (
            filled_by_instrument
        )
        assert actual.available_cash + actual.reserved_cash == account.initial_capital - sum(
            (fill.notional + fill.fee for fill in fills),
            start=Decimal(0),
        )
        assert actual.reserved_cash == sum(
            (order.reserved_cash for order in actual.open_orders),
            start=Decimal(0),
        )
        plan_path = f"/internal/v1/paper/plans/{batch_id}"
        cycle_path = f"/internal/v1/paper/management/cycles/{cycle.id}"
        summary_path = "/internal/v1/paper/management/portfolio"
        cycles_path = "/internal/v1/paper/cycles?limit=20"
        envelopes = {}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=api_now)),
            base_url="http://testserver",
        ) as client:
            for path in (plan_path, cycle_path, summary_path, cycles_path):
                for _ in range(2):
                    response = await client.get(path, headers=headers("GET", path))
                    assert response.status_code == 200, response.text
                    if path in envelopes:
                        assert envelopes[path]["data"] == response.json()["data"]
                    envelopes[path] = response.json()
        plan = envelopes[plan_path]["data"]
        detail = envelopes[cycle_path]["data"]
        summary = envelopes[summary_path]["data"]
        cycle_page = envelopes[cycles_path]["data"]
        assert plan["batch_id"] == str(batch_id)
        target_id = plan["target"]["target_version_id"]
        assert detail["active_target"]["target_version_id"] == target_id
        assert detail["cycle"]["decision"]["sizing_policy_version"] == (
            "portfolio_allocation_v1"
        )
        assert summary["current_target"]["target_version_id"] == target_id
        assert detail["activation_result"]["batch_id"] == str(batch_id)
        assert {item["order_id"] for item in plan["plan"]["items"]} == {
            str(order.id) for order in orders
        }
        assert plan["plan"]["occupation_state"] == ("occupied" if first_partial else "released")
        assert all(item["confirmed_fills"] for item in plan["plan"]["items"])
        assert detail["cycle"]["execution_locked"] is True
        assert cycle_page["version_history"] == []
        assert cycle_page["summary"]["next_eligible_reason"] != "NEXT_SLOT"
        async with sessions() as session:
            assert before_reads == await _business_facts(session)

    finally:
        await engine.dispose()
