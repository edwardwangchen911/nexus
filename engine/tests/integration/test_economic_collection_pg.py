from datetime import timedelta
from decimal import Decimal

import pytest
from nexus.persistence.models.economic import EconomicCommonWindowRecord
from nexus.persistence.models.paper import AgentPaperMarketWindowRecord, PaperFillRecord
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
from sqlalchemy import func, select, text

from engine.tests.integration.test_economic_cycle_pg import (
    test_normal_pair_producer_binds_four_research_jobs_and_exact_budget as seed,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "success",
        "consume",
        "pair_advance",
        "pair_plan_sale",
        "pair_plan_execute",
        "pair_plan_partial",
        "pair_plan_missed",
        "pair_plan_rejected_buy",
        "pair_partial",
        "pair_followup",
        "pair_followup_missed",
        "pair_followup_expired",
        "pair_followup_selective",
        "pair_followup_close",
        "pair_pending",
        "pair_asymmetric",
        "pair_lost_receipt",
        "pair_safe_lost_receipt",
        "pair_history",
        "pair_mixed",
        "consume_partial",
        "consume_close",
        "consume_close_backdated",
        "consume_quote_drift",
        "consume_cancel_peer",
        "consume_cancel_race",
        "second_failure",
        "raw_read_failure",
        "pending_claim",
        "normal_cli",
        "cross_instrument_raw",
        "history_contamination",
        "mixed",
        "new_contamination",
    ],
)
async def test_shared_collection_once_outside_transaction(
    migrated_database_url, monkeypatch, tmp_path, case
):
    await seed(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        "mixed" if case in {"mixed", "pair_mixed"} else "trade",
        True,
        window_test="mixed" if case in {"mixed", "pair_mixed"} else "frozen",
        collection_test=case,
    )


async def verify_shared_collection(sessions, clock, pair_id, case, monkeypatch, tmp_path):
    from nexus.paper.economic_collection import collect_economic_window

    async with sessions.begin() as session:
        window = await session.scalar(
            select(EconomicCommonWindowRecord).where(
                EconomicCommonWindowRecord.pair_slot_id == pair_id
            )
        )
        start, end = window.window_start, window.window_end
    clock[0] = end + timedelta(seconds=46)
    requests, objects = [], {}

    async def check_released():
        # Inspect other connections: this observer owns no economic/audit lock.
        async with sessions.begin() as observer:
            assert (
                await observer.scalar(
                    text(
                        "SELECT count(*) FROM pg_locks "
                        "WHERE locktype='advisory' AND pid <> pg_backend_pid() "
                        "AND database=(SELECT oid FROM pg_database "
                        "WHERE datname=current_database())"
                    )
                )
                == 0
            )
            assert (
                await observer.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE datname=current_database() "
                        "AND pid <> pg_backend_pid() AND state='idle in transaction'"
                    )
                )
                == 0
            )
            assert (
                await observer.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "economic.window.collection_claimed")
                )
                == kwargs["window_seq"]
            )

    class Provider:
        async def fetch_bars(self, request, cursor=None):
            await check_released()
            requests.append(request)
            if case == "pending_claim" and len(requests) == 1:
                with pytest.raises(ValueError, match="ECONOMIC_WINDOW_COLLECTION_UNKNOWN"):
                    await collect_economic_window(sessions, **kwargs)
            assert cursor is None and request.start == start and request.end == end
            if case == "second_failure" and len(requests) == 2:
                raise RuntimeError("controlled transport failure")
            received = end + timedelta(seconds=45)
            return ProviderPage(
                items=tuple(
                    PriceBar(
                        instrument_id=request.instrument_id,
                        session=start.date(),
                        open=Decimal("100"),
                        high=Decimal("101"),
                        low=Decimal("99"),
                        close=Decimal("100.5"),
                        volume=Decimal("20")
                        if (
                            case in {"consume_partial", "pair_partial"}
                            or case.startswith("pair_followup")
                        )
                        and (kwargs["window_seq"] == 1 or case == "pair_followup_close")
                        and (case != "pair_followup_selective" or len(requests) % 2 == 1)
                        else Decimal("1000"),
                        observed_at=start + timedelta(minutes=i),
                        available_at=received,
                        received_at=received,
                    )
                    for i in range(5)
                ),
                next_cursor=None,
                raw_payload=b"same raw for distinct assets"
                if case == "cross_instrument_raw"
                else (str(request.instrument_id) + end.isoformat()).encode(),
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
        async def archive_raw(self, key, payload):
            await check_released()
            objects[key] = payload
            return "memory://" + key

        async def read_raw(self, key, *, uri):
            await check_released()
            assert uri == "memory://" + key
            return b"corrupt" if case == "raw_read_failure" else objects[key]

        async def put_normalized(self, key, payload):
            await check_released()
            return "memory://" + key

    kwargs = dict(pair_slot_id=pair_id, window_seq=1, provider=Provider(), object_store=Store())

    async def collect_next(second):
        nonlocal start, end
        start, end = second.window_start, second.window_end
        kwargs["window_seq"] = 2
        clock[0] = end + timedelta(seconds=46)
        from nexus.paper.agent_market import collect_agent_paper_window
        from nexus.persistence.models.economic import EconomicWindowOrderRecord
        from nexus.persistence.models.paper import PaperOrderRecord, StrategyDecisionRecord

        async with sessions.begin() as session:
            link = await session.scalar(
                select(EconomicWindowOrderRecord)
                .where(EconomicWindowOrderRecord.window_id == second.window_id)
                .order_by(EconomicWindowOrderRecord.order_id)
                .limit(1)
            )
            order = await session.get(PaperOrderRecord, link.order_id)
            decision = await session.get(StrategyDecisionRecord, order.strategy_decision_id)
        native = await collect_agent_paper_window(
            sessions,
            cycle_id=decision.agent_paper_cycle_id,
            instrument_id=link.instrument_id,
            provider=kwargs["provider"],
            object_store=kwargs["object_store"],
        )
        assert native.window_start == start and native.window_end == end and not native.replayed
        current = await collect_economic_window(sessions, **kwargs)
        assert current.replayed
        prior = await collect_economic_window(
            sessions,
            pair_slot_id=pair_id,
            window_seq=1,
            provider=kwargs["provider"],
            object_store=kwargs["object_store"],
        )
        assert prior.replayed
        assert len(requests) == (3 if case == "pair_followup_selective" else 4)
        return current

    async def foreign_job():
        async with sessions.begin() as session:
            session.add(
                Job(
                    job_key="foreign-archive-context",
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
                    started_at=None,
                    finished_at=None,
                )
            )

    if case == "new_contamination":
        await foreign_job()
        with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN"):
            await collect_economic_window(sessions, **kwargs)
        assert not requests and not objects
        async with sessions.begin() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "economic.window.collection_claimed")
                )
                == 0
            )
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperMarketWindowRecord))
                == 0
            )
        return
    if case == "success":
        clock[0] = start + timedelta(seconds=1)
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_COLLECTION_TOO_EARLY"):
            await collect_economic_window(sessions, **kwargs)
        assert not requests and not objects
        clock[0] = end + timedelta(seconds=46)
    if case in {"second_failure", "raw_read_failure", "cross_instrument_raw"}:
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_COLLECTION_FAILED"):
            await collect_economic_window(sessions, **kwargs)
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_COLLECTION_UNKNOWN"):
            await collect_economic_window(sessions, **kwargs)
    elif case == "normal_cli":
        import json
        from contextlib import asynccontextmanager

        import httpx
        from nexus.ops import agent_paper_once as cli
        from nexus.persistence.models.market import InstrumentAlias
        from nexus.persistence.models.paper import PaperOrderRecord, StrategyDecisionRecord
        from nexus.providers.base import BarsRequest
        from nexus.providers.market import schwab_oauth
        from nexus.providers.market.schwab_price_history import SchwabPriceHistoryProvider

        async with sessions.begin() as session:
            cycle, instrument_id = (
                await session.execute(
                    select(
                        StrategyDecisionRecord.agent_paper_cycle_id, PaperOrderRecord.instrument_id
                    )
                    .join(
                        PaperOrderRecord,
                        PaperOrderRecord.strategy_decision_id == StrategyDecisionRecord.id,
                    )
                    .limit(1)
                )
            ).one()
            symbols = dict(
                (symbol, iid)
                for iid, symbol in (
                    await session.execute(
                        select(InstrumentAlias.instrument_id, InstrumentAlias.symbol).where(
                            InstrumentAlias.valid_to.is_(None)
                        )
                    )
                ).all()
            )
        received = end + timedelta(seconds=45)
        schwab_oauth.save_schwab_token(
            tmp_path,
            schwab_oauth.SchwabOAuthToken(
                access_token="access-canary",
                refresh_token="refresh-canary",
                obtained_at=received - timedelta(minutes=10),
                expires_at=received + timedelta(minutes=10),
            ),
        )

        async def respond(request):
            await check_released()
            assert request.url.host == "api.schwabapi.com"
            assert request.url.path == "/marketdata/v1/pricehistory"
            symbol = request.url.params["symbol"]
            requests.append(
                BarsRequest(
                    instrument_id=symbols[symbol],
                    start=start,
                    end=end,
                    interval="1Min",
                    adjustment="raw",
                )
            )
            assert request.url.params["startDate"] == str(int(start.timestamp() * 1000))
            assert request.url.params["endDate"] == str(int(end.timestamp() * 1000))
            payload = {
                "symbol": symbol,
                "empty": False,
                "candles": [
                    {
                        "open": 100,
                        "high": 101,
                        "low": 99,
                        "close": 100.5,
                        "volume": 1000,
                        "datetime": int((start + timedelta(minutes=i)).timestamp() * 1000),
                    }
                    for i in range(5)
                ],
            }
            return httpx.Response(200, content=json.dumps(payload).encode())

        @asynccontextmanager
        async def http_client(**kwargs):
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(respond), follow_redirects=False
            ) as client:
                yield client

        def make_provider(repo_root, http, *, symbol_resolver, process_environment):
            return SchwabPriceHistoryProvider(
                repo_root,
                http,
                symbol_resolver=symbol_resolver,
                process_environment={},
                now=lambda: received,
            )

        monkeypatch.setattr(cli, "SchwabPriceHistoryProvider", make_provider)
        monkeypatch.setattr(cli, "schwab_http_client", http_client)
        monkeypatch.setattr(cli, "S3ProviderEvidenceStore", lambda *a, **kw: Store())
        settings = cli.AgentPaperMarketSettings(
            _env_file=None,
            database_url=str(sessions.kw["bind"].url.render_as_string(hide_password=False)),
            object_store_endpoint="http://127.0.0.1:19000",
        )
        request = cli.AgentPaperCollectWindowRequest(
            action="collect-window", cycle_id=cycle, instrument_id=instrument_id
        )
        result = await cli.collect_window_once(settings, request, repo_root=tmp_path)
        assert not result.replayed
        again = await cli.collect_window_once(settings, request, repo_root=tmp_path)
        assert again.replayed and again.authority_id == result.authority_id
    else:
        result = await collect_economic_window(sessions, **kwargs)
        assert result.status == "archives_ready" and not result.replayed
        again = await collect_economic_window(sessions, **kwargs)
        assert again.replayed and again.window_id == result.window_id
        if case == "history_contamination":
            await foreign_job()
            historical = await collect_economic_window(sessions, **kwargs)
            assert historical.replayed and historical.window_id == result.window_id
    expected_calls = 1 if case == "raw_read_failure" else 2
    assert (
        len(requests) == expected_calls
        and len({r.instrument_id for r in requests}) == expected_calls
    )
    async with sessions.begin() as session:
        rows = tuple(await session.scalars(select(AgentPaperMarketWindowRecord)))
        assert len(rows) == (
            0
            if case in {"second_failure", "raw_read_failure", "cross_instrument_raw"}
            else 2
            if case in {"mixed", "pair_mixed"}
            else 4
        )
        assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 0
        if rows:
            assert len({r.raw_artifact_id for r in rows}) == 2
            sources = {}
            for row in rows:
                sources.setdefault(row.instrument_id, set()).add(
                    (row.raw_artifact_id, row.source_content_hash, row.price_series_manifest_id)
                )
                assert row.window_start_at == start and row.window_end_at == end
            assert len(sources) == 2 and all(len(items) == 1 for items in sources.values())
            assert len(set.union(*sources.values())) == 2
            from nexus.paper.execution_service import advance_internal_paper_order

            if case.startswith("consume"):
                await verify_consumption(sessions, clock, rows, case)
                return
            if not case.startswith("pair_"):
                with pytest.raises(ValueError, match="ECONOMIC_WINDOW_BINDING_REQUIRED"):
                    await advance_internal_paper_order(
                        session,
                        order_id=rows[0].order_id,
                        evaluated_at=clock[0],
                        idempotency_key="economic-collected-still-no-consumption",
                        expected_agent_market_window_id=None,
                    )
                assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 0
    if case.startswith("pair_"):
        await verify_pair_execution(
            sessions, clock, pair_id, rows, case, monkeypatch, tmp_path, collect_next
        )


async def verify_consumption(sessions, clock, rows, case):
    from nexus.ops.paper_once import PaperAdvanceRequest, PaperOnceError, run_once
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.paper import (
        PaperExecutionWindowAuthorityRecord,
        PaperOrderRecord,
    )
    from nexus.settings import EngineSettings

    settings = EngineSettings(
        _env_file=None,
        database_url=sessions.kw["bind"].url.render_as_string(hide_password=False),
    )
    if case in {"consume_close", "consume_close_backdated"}:
        from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
        from nexus.persistence.models.paper import StrategyDecisionRecord

        async with sessions.begin() as session:
            order = await session.get(PaperOrderRecord, rows[0].order_id)
            decision = await session.get(StrategyDecisionRecord, order.strategy_decision_id)
            calendar = await session.get(
                ExchangeCalendarSessionRecord,
                (decision.calendar_authority_id, decision.execution_session),
            )
            clock[0] = calendar.market_close_at
    requests = []
    for row in rows:
        async with sessions.begin() as session:
            order = await session.get(PaperOrderRecord, row.order_id)
            account_id = order.account_id
        requests.append(
            PaperAdvanceRequest(
                action="advance",
                account_id=account_id,
                order_id=row.order_id,
                evaluated_at=row.evaluated_at if case == "consume_close_backdated" else clock[0],
                agent_cycle_id=row.cycle_id,
                agent_market_window_id=row.id,
                idempotency_key=f"agent-paper-loop-advance-v2:{row.order_id}:{row.id}",
            )
        )
    if case == "consume_cancel_race":
        await verify_cancel_race(sessions, clock, settings, requests, rows)
        return
    if case in {"consume_close", "consume_close_backdated"}:
        from nexus.paper.errors import PaperExecutionError
        from nexus.paper.execution_service import advance_internal_paper_order

        # Direct native callers cannot backdate a new fill through actual close.
        if case == "consume_close_backdated":
            async with sessions.begin() as session:
                with pytest.raises(
                    PaperExecutionError, match="ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE"
                ):
                    await advance_internal_paper_order(
                        session,
                        order_id=requests[0].order_id,
                        evaluated_at=rows[0].evaluated_at,
                        idempotency_key="backdated-native",
                        expected_agent_market_window_id=rows[0].id,
                    )
        # The normal product entry uses actual DB time and can lawfully close
        # the existing reservation. It must never create a backdated fill.
        assert (await run_once(settings, requests[0]))["action"] == "cancelled"
        async with sessions.begin() as session:
            assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 0
        return
    if case == "consume_cancel_peer":
        from nexus.paper.execution_service import cancel_internal_paper_order
        from nexus.persistence.models.paper import PaperOrderEventRecord

        async with sessions.begin() as session:
            await cancel_internal_paper_order(
                session,
                order_id=requests[-1].order_id,
                requested_at=clock[0],
                expected_event_seq=await session.scalar(
                    select(func.max(PaperOrderEventRecord.event_seq)).where(
                        PaperOrderEventRecord.order_id == requests[-1].order_id
                    )
                ),
                idempotency_key="cancel-peer",
            )
        with pytest.raises(PaperOnceError, match="ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE"):
            await run_once(settings, requests[0])
        async with sessions.begin() as session:
            assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 0
        # Terminal readback must not require market evidence or active peers.
        result = await run_once(
            settings, requests[-1].model_copy(update={"idempotency_key": "terminal-read"})
        )
        assert result["action"] == "terminal"
        return
    for index, request in enumerate(requests):
        result = await run_once(settings, request)
        assert result["action"] == ("partially_filled" if case == "consume_partial" else "filled")
        if case == "consume_quote_drift" and index == 0:
            from nexus.persistence.models.market import LatestQuote

            async with sessions.begin() as session:
                first = await session.get(PaperOrderRecord, request.order_id)
                peer = next(
                    r
                    for r in rows
                    if r.instrument_id == first.instrument_id and r.order_id != first.id
                )
                quote = await session.scalar(
                    select(LatestQuote).where(
                        LatestQuote.instrument_id == first.instrument_id,
                        LatestQuote.source == "schwab",
                    )
                )
                session.add(
                    LatestQuote(
                        **{
                            **{
                                column.name: getattr(quote, column.name)
                                for column in LatestQuote.__table__.columns
                                if column.name not in {"id", "created_at"}
                            },
                            "observed_at": rows[0].window_start_at + timedelta(minutes=1),
                            "available_at": clock[0],
                            "received_at": clock[0],
                            "bid": Decimal("100"),
                            "ask": Decimal("101"),
                            "revision": "controlled-quote-drift",
                            "feed": "price_history",
                            "content_hash": "e" * 64,
                        }
                    )
                )
            wanted = next(r for r in requests if r.order_id == peer.order_id)
            with pytest.raises(PaperOnceError, match="ECONOMIC_WINDOW_ARCHIVE_CONFLICT"):
                await run_once(settings, wanted)
            assert (await run_once(settings, request))["action"] == "filled"
            async with sessions.begin() as session:
                assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 1
            return
        # Each normal CLI call committed an independent native fill and ledger.
        async with sessions.begin() as session:
            assert (
                await session.scalar(select(func.count()).select_from(PaperFillRecord)) == index + 1
            )
    for request in requests:
        assert (await run_once(settings, request))["action"] == (
            "partially_filled" if case == "consume_partial" else "filled"
        )
    async with sessions.begin() as session:
        fills = tuple(await session.scalars(select(PaperFillRecord)))
        windows = tuple(await session.scalars(select(PaperExecutionWindowAuthorityRecord)))
        assert len(fills) == len(windows) == 4
        assert len({f.account_id for f in fills}) == 2
        assert all(f.quantity > 0 for f in fills)
        assert all(
            w.capacity == (1 if case == "consume_partial" else 50)
            and w.volume == (100 if case == "consume_partial" else 5000)
            for w in windows
        )
        assert len({w.source_authority_id for w in windows}) == 2
        for account_id in {f.account_id for f in fills}:
            projection = await load_paper_projection(session, account_id=account_id, as_of=clock[0])
            assert len(projection.positions) == 2
            assert projection.available_cash >= 0 and projection.reserved_cash >= 0
            if case == "consume_partial":
                assert projection.reserved_cash > 0 and len(projection.open_orders) == 2
                assert all(o.quantity > 0 and o.reserved_cash > 0 for o in projection.open_orders)
            for position in projection.positions:
                assert position.quantity == sum(
                    (
                        f.quantity
                        for f in fills
                        if f.account_id == account_id and f.instrument_id == position.instrument_id
                    ),
                    Decimal(0),
                )
    # Exact window receipts remain authoritative under a different caller key.
    replacement = requests[0].model_copy(update={"idempotency_key": "replacement-consume"})
    assert (await run_once(settings, replacement))["action"] == (
        "partially_filled" if case == "consume_partial" else "filled"
    )
    if case == "consume_partial":
        async with sessions.begin() as session:
            from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
            from nexus.persistence.models.paper import StrategyDecisionRecord

            order = await session.get(PaperOrderRecord, requests[0].order_id)
            decision = await session.get(StrategyDecisionRecord, order.strategy_decision_id)
            calendar = await session.get(
                ExchangeCalendarSessionRecord,
                (decision.calendar_authority_id, decision.execution_session),
            )
            clock[0] = calendar.market_close_at
        for request in requests:
            closing = request.model_copy(
                update={
                    "agent_cycle_id": None,
                    "agent_market_window_id": None,
                    "evaluated_at": clock[0],
                    "idempotency_key": f"closing:{request.order_id}",
                }
            )
            assert (await run_once(settings, closing))["action"] == "cancelled"
        async with sessions.begin() as session:
            for account_id in {r.account_id for r in requests}:
                projection = await load_paper_projection(
                    session, account_id=account_id, as_of=clock[0]
                )
                assert projection.reserved_cash == 0 and not projection.open_orders
                assert len(projection.positions) == 2
        # Exact committed receipts still resolve after intent expiry and closure.
        assert (await run_once(settings, requests[0]))["action"] == "partially_filled"
    async with sessions.begin() as session:
        assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 4


async def verify_cancel_race(sessions, clock, settings, requests, rows):
    import asyncio
    from unittest.mock import patch

    import nexus.paper.execution_service as execution
    from nexus.ops.paper_once import run_once
    from nexus.persistence.models.paper import PaperOrderEventRecord

    first = requests[0]
    peer = next(r for r in requests if r.account_id != first.account_id)
    ready, release = asyncio.Event(), asyncio.Event()
    original = execution._insert_window

    async def held_window(*args, **kwargs):
        row = await original(*args, **kwargs)
        ready.set()
        await release.wait()
        return row

    async def cancel():
        async with sessions.begin() as session:
            seq = await session.scalar(
                select(func.max(PaperOrderEventRecord.event_seq)).where(
                    PaperOrderEventRecord.order_id == peer.order_id
                )
            )
            return await execution.cancel_internal_paper_order(
                session,
                order_id=peer.order_id,
                requested_at=clock[0],
                expected_event_seq=seq,
                idempotency_key="racing-cancel",
            )

    with patch.object(execution, "_insert_window", held_window):
        filling = asyncio.create_task(run_once(settings, first))
        cancelling = None
        try:
            await asyncio.wait_for(ready.wait(), 10)
            cancelling = asyncio.create_task(cancel())
            blocked = False
            for _ in range(100):
                async with sessions.begin() as session:
                    blocked = bool(
                        await session.scalar(
                            text(
                                "SELECT count(*) FROM pg_stat_activity "
                                "WHERE datname=current_database() "
                                "AND pid <> pg_backend_pid() AND wait_event_type='Lock'"
                            )
                        )
                    )
                if blocked or cancelling.done():
                    break
            assert blocked and not cancelling.done()
        finally:
            release.set()
            await asyncio.wait_for(filling, 10)
            if cancelling is not None:
                await asyncio.wait_for(cancelling, 10)
    async with sessions.begin() as session:
        assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 1


async def verify_pair_execution(
    sessions, clock, pair_id, rows, case, monkeypatch, tmp_path, collect_next
):
    import nexus.ops.economic_cycle as cli
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.paper import PaperOrderRecord

    assert hasattr(cli, "execute_advance"), "normal registered-pair execution entry is missing"
    path = tmp_path / "pair-execution-url"
    path.write_text(sessions.kw["bind"].url.render_as_string(hide_password=False))
    path.chmod(0o600)
    kwargs = dict(
        database_url_file=path,
        expected_database=sessions.kw["bind"].url.database,
        pair_slot_id=pair_id,
    )
    before_status = await cli.execute_status(**kwargs)
    assert before_status["status"] == "not_started"
    async with sessions.begin() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "economic.window.execution_claimed")
            )
            == 0
        )
    if case == "pair_pending":
        from nexus.audit.service import append_audit_event

        async with sessions.begin() as session:
            window = await session.scalar(
                select(EconomicCommonWindowRecord).where(
                    EconomicCommonWindowRecord.pair_slot_id == pair_id
                )
            )
            await append_audit_event(
                session,
                actor_id="economic-window",
                action="economic.window.execution_claimed",
                schema_version="economic_window_execution_v1",
                occurred_at=clock[0],
                payload={"window_id": str(window.id), "window_hash": window.content_hash},
            )
        for _ in range(2):
            result = await cli.execute_advance(**kwargs)
            assert result["status"] == "unknown" and result["replayed"]
            assert all(item["action"] == "unknown" for item in result["orders"])
            status = await cli.execute_status(**kwargs)
            assert status["status"] == "unknown"
        async with sessions.begin() as session:
            assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 0
            from nexus.paper.economic_window import freeze_common_window

            with pytest.raises(ValueError, match="ECONOMIC_WINDOW_NOT_READY"):
                await freeze_common_window(session, pair_slot_id=pair_id, window_seq=2)
        return
    if case in {"pair_asymmetric", "pair_lost_receipt", "pair_safe_lost_receipt"}:
        import nexus.paper.economic_execution as execution
        from nexus.paper.execution_service import cancel_internal_paper_order
        from nexus.persistence.models.paper import PaperOrderEventRecord

        original = execution._consume_order
        calls = []

        async def observe_commit(factory, request):
            result = await original(factory, request)
            calls.append(request.order_id)
            if len(calls) == 1:
                if case == "pair_safe_lost_receipt":
                    from nexus.ops.paper_once import PaperOnceError

                    raise PaperOnceError("PAPER_ONCE_RESULT_UNKNOWN")
                if case == "pair_lost_receipt":
                    raise RuntimeError("controlled lost response after native commit")
                async with sessions.begin() as session:
                    account = await session.scalar(
                        select(PaperOrderRecord.account_id).where(
                            PaperOrderRecord.id == request.order_id
                        )
                    )
                    peer = await session.scalar(
                        select(PaperOrderRecord)
                        .where(PaperOrderRecord.account_id != account)
                        .order_by(PaperOrderRecord.id)
                    )
                    sequence = await session.scalar(
                        select(func.max(PaperOrderEventRecord.event_seq)).where(
                            PaperOrderEventRecord.order_id == peer.id
                        )
                    )
                    await cancel_internal_paper_order(
                        session,
                        order_id=peer.id,
                        requested_at=clock[0],
                        expected_event_seq=sequence,
                        idempotency_key="controlled-peer-cancel-after-commit",
                    )
            return result

        monkeypatch.setattr(execution, "_consume_order", observe_commit)
        result = await cli.execute_advance(**kwargs)
        assert result["status"] == ("unavailable" if case == "pair_asymmetric" else "unknown")
        assert result["comparison_status"] == "incomparable"
        assert sum(o["action"] == "filled" for o in result["orders"]) == 1
        again = await cli.execute_advance(**kwargs)
        assert again["replayed"] and again["orders"] == result["orders"]
        assert len(calls) == 1
        status = await cli.execute_status(**kwargs)
        assert (
            status["orders"] == result["orders"] and status["comparison_status"] == "incomparable"
        )
        async with sessions.begin() as session:
            assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 1
        return
    result = await cli.execute_advance(**kwargs)
    assert result["status"] == (
        "partial" if (case == "pair_partial" or case.startswith("pair_followup")) else "completed"
    )
    assert result["comparison_status"] == (
        "incomplete"
        if (case == "pair_partial" or case.startswith("pair_followup"))
        else "not_evaluated"
    )
    assert not result["replayed"] and result["execution_locked"] is True
    assert len(result["orders"]) == (2 if case == "pair_mixed" else 4)
    if case == "pair_mixed":
        assert {lane["status"] for lane in result["lanes"]} == {"filled", "no_trade"}
    async with sessions.begin() as session:
        assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == (
            2 if case == "pair_mixed" else 4
        )
        orders = tuple(await session.scalars(select(PaperOrderRecord)))
        assert len({o.account_id for o in orders}) == (1 if case == "pair_mixed" else 2)
        for aid in {o.account_id for o in orders}:
            projection = await load_paper_projection(session, account_id=aid, as_of=clock[0])
            assert len(projection.positions) == 2
            assert projection.available_cash >= 0
    if case.startswith("pair_followup"):
        from nexus.paper.economic_window import freeze_common_window

        if case == "pair_followup_expired":
            async with sessions.begin() as session:
                with pytest.raises(ValueError, match="ECONOMIC_WINDOW_SOURCE_EXPIRED"):
                    await freeze_common_window(session, pair_slot_id=pair_id, window_seq=2)
                assert (
                    await session.scalar(
                        select(func.count()).select_from(EconomicCommonWindowRecord)
                    )
                    == 1
                )
                assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 4
            return
        if case == "pair_followup_missed":
            clock[0] += timedelta(minutes=1)
        if case == "pair_followup":
            import copy

            from nexus.paper.economic_window import _hash
            from sqlalchemy.exc import DBAPIError

            async with sessions.begin() as session:
                nested = await session.begin_nested()
                preview = await freeze_common_window(session, pair_slot_id=pair_id, window_seq=2)
                preview_row = await session.get(EconomicCommonWindowRecord, preview.window_id)
                values = {
                    column.name: copy.deepcopy(getattr(preview_row, column.name))
                    for column in EconomicCommonWindowRecord.__table__.columns
                    if column.name not in {"id", "created_at"}
                }
                await nested.rollback()
                doc = values["qualification"]
                doc["order_states"][-1] = copy.deepcopy(doc["order_states"][0])
                doc["orders"] = sorted(
                    state["order_id"]
                    for state in doc["order_states"]
                    if state["status"] in {"submitted", "partially_filled"}
                )
                values["content_hash"] = _hash(doc)
                with pytest.raises(DBAPIError, match="ECONOMIC_WINDOW_INVALID"):
                    async with session.begin_nested():
                        session.add(EconomicCommonWindowRecord(**values))
                        await session.flush()
        async with sessions.begin() as session:
            second = await freeze_common_window(session, pair_slot_id=pair_id, window_seq=2)
            if case == "pair_followup_missed":
                assert second.status == "missed"
                again = await freeze_common_window(session, pair_slot_id=pair_id, window_seq=2)
                assert again.replayed and again.window_start == second.window_start
                with pytest.raises(ValueError, match="ECONOMIC_WINDOW_MISSED"):
                    await freeze_common_window(session, pair_slot_id=pair_id, window_seq=3)
                assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 4
                return
            assert second.status == "frozen" and second.window_id is not None
        assert second.window_start > rows[0].window_end_at
        async with sessions.begin() as session:
            replay = await freeze_common_window(session, pair_slot_id=pair_id, window_seq=2)
            assert replay.replayed and replay.window_id == second.window_id
            with pytest.raises(ValueError):
                async with session.begin_nested():
                    await freeze_common_window(session, pair_slot_id=pair_id, window_seq=4)
        frozen_cli = await cli.execute_freeze_next(**kwargs)
        assert frozen_cli["replayed"] and frozen_cli["window_id"] == str(second.window_id)
        collected = await collect_next(second)
        assert collected.status == "archives_ready"
        import nexus.paper.economic_execution as execution
        from nexus.paper.economic_execution import (
            advance_economic_pair,
            read_economic_pair_execution,
        )

        dbname = kwargs["expected_database"]
        consume = execution._consume_order
        attempted = []

        async def observe_followup(factory, request):
            attempted.append(request.order_id)
            return await consume(factory, request)

        monkeypatch.setattr(execution, "_consume_order", observe_followup)
        final = await cli.execute_advance(**kwargs)
        assert len(attempted) == (2 if case == "pair_followup_selective" else 4)
        assert final["status"] == ("partial" if case == "pair_followup_close" else "completed")
        assert final["comparison_status"] == (
            "incomplete" if case == "pair_followup_close" else "not_evaluated"
        )
        repeated = await advance_economic_pair(
            sessions, pair_slot_id=pair_id, window_seq=2, expected_database=dbname
        )
        assert repeated["replayed"] and repeated["orders"] == final["orders"]
        readonly = await cli.execute_status(**kwargs)
        assert readonly["status"] == final["status"] and readonly["orders"] == final["orders"]
        assert len(attempted) == (2 if case == "pair_followup_selective" else 4)
        if case == "pair_followup_close":
            await verify_followup_close(sessions, clock, rows, kwargs, final)
            return
        async with sessions.begin() as session:
            old = await read_economic_pair_execution(
                session, pair_slot_id=pair_id, window_seq=1, expected_database=dbname
            )
            assert old["status"] == "partial" and old["orders"] == result["orders"]
            assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == (
                6 if case == "pair_followup_selective" else 8
            )
            for aid in {o.account_id for o in orders}:
                projection = await load_paper_projection(session, account_id=aid, as_of=clock[0])
                assert not projection.open_orders and projection.reserved_cash == 0
            third = await freeze_common_window(session, pair_slot_id=pair_id, window_seq=3)
            assert third.status == "not_required" and third.window_id is None
        return
    if case == "pair_history":
        async with sessions.begin() as session:
            session.add(
                Job(
                    job_key="foreign-after-pair",
                    input_hash="f" * 64,
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
    again = await cli.execute_advance(**kwargs)
    assert again["replayed"] and again["orders"] == result["orders"]
    async with sessions.begin() as session:
        assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == (
            2 if case == "pair_mixed" else 4
        )

    assert hasattr(cli, "execute_status"), "read-only pair execution status entry is missing"
    async with sessions.begin() as session:
        before = await session.scalar(select(func.count()).select_from(AuditEvent))
    status = await cli.execute_status(**kwargs)
    assert status["orders"] == result["orders"]
    async with sessions.begin() as session:
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == before
    if case == "pair_advance":
        import asyncio
        import json
        import subprocess

        result_process = await asyncio.to_thread(
            subprocess.run,
            [
                "uv",
                "run",
                "--project",
                "engine",
                "nexus-engine-economic-cycle",
                "advance-window",
                "--database-url-file",
                str(path),
                "--expected-database",
                kwargs["expected_database"],
                "--pair-slot-id",
                str(pair_id),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result_process.returncode == 0
        receipt = json.loads(result_process.stdout)
        assert receipt["replayed"] and receipt["orders"] == result["orders"]


async def verify_followup_close(sessions, clock, rows, kwargs, final):
    import nexus.ops.economic_cycle as cli
    from nexus.ops.paper_once import PaperAdvanceRequest, run_once
    from nexus.paper.execution_service import cancel_internal_paper_order
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
    from nexus.persistence.models.paper import (
        PaperOrderEventRecord,
        PaperOrderRecord,
        StrategyDecisionRecord,
    )
    from nexus.settings import EngineSettings

    async with sessions.begin() as session:
        original = tuple(await session.scalars(select(PaperFillRecord)))
        hashes = {(fill.id, fill.quantity, fill.execution_window_authority_id) for fill in original}
        first = await session.get(PaperOrderRecord, rows[0].order_id)
        seq = await session.scalar(
            select(func.max(PaperOrderEventRecord.event_seq)).where(
                PaperOrderEventRecord.order_id == first.id
            )
        )
        canceled = await cancel_internal_paper_order(
            session,
            order_id=first.id,
            requested_at=clock[0],
            expected_event_seq=seq,
            idempotency_key="second-window-explicit-cancel",
        )
        assert canceled.status == "cancelled"
        decision = await session.get(StrategyDecisionRecord, first.strategy_decision_id)
        calendar = await session.get(
            ExchangeCalendarSessionRecord,
            (decision.calendar_authority_id, decision.execution_session),
        )
        clock[0] = calendar.market_close_at
    settings = EngineSettings(_env_file=None, database_url=kwargs["database_url_file"].read_text())
    for binding in rows:
        async with sessions.begin() as session:
            order = await session.get(PaperOrderRecord, binding.order_id)
        request = PaperAdvanceRequest(
            action="advance",
            account_id=order.account_id,
            order_id=order.id,
            evaluated_at=clock[0],
            idempotency_key=f"second-window-eod:{order.id}",
        )
        result = await run_once(settings, request)
        assert result["action"] in {"cancelled", "terminal"}
    async with sessions.begin() as session:
        fills = tuple(await session.scalars(select(PaperFillRecord)))
        assert (
            len(fills) == 8
            and {(fill.id, fill.quantity, fill.execution_window_authority_id) for fill in fills}
            == hashes
        )
        for account_id in {fill.account_id for fill in fills}:
            projection = await load_paper_projection(session, account_id=account_id, as_of=clock[0])
            assert not projection.open_orders and projection.reserved_cash == 0
            assert len(projection.positions) == 2
    history = await cli.execute_status(**kwargs)
    assert history["status"] == "partial" and history["orders"] == final["orders"]
