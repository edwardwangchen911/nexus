"""Normal authenticated economic GET reads the same isolated native facts."""

import json
import os
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from helpers.paper_api import app, headers
from nexus.persistence.base import Base
from nexus.persistence.models.economic import EconomicPairSlotRecord
from sqlalchemy import event, func, select

from engine.tests.integration.test_economic_cycle_pg import (
    test_normal_pair_producer_binds_four_research_jobs_and_exact_budget as seed,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["no_trade", "pending", "failed", "filled", "asymmetric", "stage"])
async def test_economic_pair_get_is_authenticated_redacted_and_business_read_only(
    migrated_database_url, monkeypatch, tmp_path, case
):
    await seed(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        "trade"
        if case in {"filled", "asymmetric", "stage"}
        else "unknown"
        if case == "failed"
        else False
        if case == "pending"
        else True,
        True,
        window_test="frozen" if case in {"filled", "asymmetric", "stage"} else False,
        collection_test="pair_stage_one_not_required"
        if case == "stage"
        else "pair_advance"
        if case == "filled"
        else "pair_asymmetric"
        if case == "asymmetric"
        else False,
        api_read_test=case,
    )


async def verify_pair_api(sessions, clock, pair_id, url, case):
    if case == "stage":
        async with sessions() as session:
            pair_id = await session.scalar(
                select(EconomicPairSlotRecord.id).where(EconomicPairSlotRecord.ordinal == 2)
            )
    # Observe after every actually recorded cycle event became available.
    from nexus.persistence.models.paper import AgentPaperCycleEventRecord

    async with sessions() as session:
        latest = await session.scalar(select(func.max(AgentPaperCycleEventRecord.occurred_at)))
        if latest is not None:
            clock[0] = max(clock[0], latest) + timedelta(seconds=1)
    application = app(url, now=clock[0])
    engine = application.state.session_factory.kw["bind"]
    timing = None
    timing_dir = os.environ.get("NEXUS_TEST_ECONOMIC_READ_TIMING_DIR")
    if timing_dir:
        from helpers.economic_read_timing import EconomicReadTiming

        timing = EconomicReadTiming(application, timing_dir, case)
    statements = []
    event.listen(
        engine.sync_engine,
        "before_cursor_execute",
        lambda conn, cursor, statement, parameters, context, executemany: statements.append(
            statement
        ),
    )
    names = [
        "audit_events",
        "jobs",
        "economic_pair_slots",
        "economic_lane_cycles",
        "economic_common_windows",
        "paper_orders",
        "paper_order_batches",
        "paper_order_events",
        "paper_fills",
        "paper_reservations",
        "paper_ledger_transactions",
        "paper_ledger_entries",
        "paper_position_snapshots",
        "paper_target_versions",
        "agent_paper_cycle_events",
        "strategy_decisions",
    ]

    async def counts():
        async with sessions() as session:
            return [
                await session.scalar(select(func.count()).select_from(Base.metadata.tables[n]))
                for n in names
            ]

    before = await counts()
    path = f"/internal/v1/paper/economic-pairs/{pair_id}"
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://testserver"
        ) as client:
            assert (await client.get(path)).status_code == 401
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200, response.text
            data = response.json()["data"]
            assert data["pair_slot_id"] == str(pair_id)
            assert data["planned_slots"] == 24 and data["execution_locked"] is True
            assert data["economic_status"] == "not_evaluated"
            assert {lane["lane"] for lane in data["lanes"]} == {"incumbent", "challenger"}
            assert all(lane["cycle_id"] for lane in data["lanes"])
            assert "account_id" not in response.text and "input_hash" not in response.text
            if case == "no_trade":
                assert data["execution"]["status"] == "completed"
                assert data["execution"]["window_id"] is None
                assert {lane["status"] for lane in data["execution"]["lanes"]} == {"no_trade"}
            elif case in {"pending", "failed"}:
                assert data["execution"] is None and data["gap_code"] == "ECONOMIC_WINDOW_REQUIRED"
                if case == "pending":
                    assert all(lane["cycle_status"] == "researching" for lane in data["lanes"])
                if case == "failed":
                    assert all(lane["cycle_status"] == "unavailable" for lane in data["lanes"])
            else:
                assert data["execution"]["window_id"]
                assert data["execution"]["status"] == (
                    "completed" if case in {"filled", "stage"} else "unavailable"
                )
                assert data["execution"]["comparison_status"] == (
                    "not_evaluated" if case in {"filled", "stage"} else "incomparable"
                )
                assert data["execution"]["orders"]
                if case in {"filled", "stage"}:
                    assert all(order["action"] == "filled" for order in data["execution"]["orders"])
            async with sessions() as session:
                if case == "stage":
                    assert {
                        lane["lane"]: lane["status"] for lane in data["execution"]["lanes"]
                    } == {"incumbent": "filled", "challenger": "not_required"}
                    assert len(data["execution"]["orders"]) == 1
                planned = await session.scalar(
                    select(EconomicPairSlotRecord).where(
                        EconomicPairSlotRecord.ordinal == (3 if case == "stage" else 2)
                    )
                )
            planned_path = f"/internal/v1/paper/economic-pairs/{planned.id}"
            planned_response = await client.get(planned_path, headers=headers("GET", planned_path))
            assert planned_response.status_code == 200
            planned_data = planned_response.json()["data"]
            assert (
                planned_data["ordinal"] == (3 if case == "stage" else 2)
                and planned_data["planned_slots"] == 24
            )
            assert all(
                lane["cycle_id"] is None and lane["cycle_status"] is None
                for lane in planned_data["lanes"]
            )
            assert (
                planned_data["execution"] is None
                and planned_data["gap_code"] == "ECONOMIC_WINDOW_REQUIRED"
            )
            assert (await client.post(path, headers=headers("POST", path))).status_code == 405
            for lane in ("incumbent", "challenger"):
                lane_path = path + f"/lanes/{lane}"
                assert (await client.get(lane_path)).status_code == 401
                lane_response = await client.get(lane_path, headers=headers("GET", lane_path))
                if lane_response.status_code != 200:
                    import traceback

                    from nexus.paper.economic_read import read_economic_lane
                    from sqlalchemy import text

                    async with sessions.begin() as diagnostic_session:
                        await diagnostic_session.execute(
                            text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                        )
                        try:
                            await read_economic_lane(
                                diagnostic_session,
                                pair_slot_id=pair_id,
                                lane=lane,
                                expected_database=diagnostic_session.bind.url.database,
                                now=clock[0],
                            )
                        except Exception as error:
                            print(
                                "LANE_READ_DIAGNOSTIC",
                                type(error).__name__,
                                [
                                    (Path(frame.filename).name, frame.name, frame.lineno)
                                    for frame in traceback.extract_tb(error.__traceback__)
                                ],
                            )
                            if hasattr(error, "errors"):
                                print(
                                    "LANE_SCHEMA_LOCATIONS",
                                    [(item["loc"], item["type"]) for item in error.errors()],
                                )
                            elif str(error).replace("_", "").isalnum() and str(
                                error
                            ).upper() == str(error):
                                print("LANE_FIXED_CODE", str(error))
                assert lane_response.status_code == 200, lane_response.text
                if timing is not None and os.environ.get("NEXUS_TEST_ECONOMIC_READ_ONCE") == "1":
                    assert (
                        sum(phase["phase"] == "orders" for phase in timing.records[-1]["phases"])
                        <= 2
                    ), "same snapshot expanded selected cycle orders twice"
                lane_data = lane_response.json()["data"]
                export_dir = os.environ.get("NEXUS_TEST_ECONOMIC_LANE_EXPORT_DIR")
                if export_dir:
                    directory = Path(export_dir)
                    if not directory.is_absolute() or directory.stat().st_mode & 0o077:
                        raise AssertionError("private test export directory required")
                    output = directory / f"{case}-{lane}.json"
                    output.write_text(json.dumps(lane_response.json()))
                    output.chmod(0o600)
                assert lane_data["pair"] == data and lane_data["lane"] == lane
                ref = next(item for item in data["lanes"] if item["lane"] == lane)
                assert lane_data["detail"]["cycle"]["cycle_id"] == ref["cycle_id"]
                from nexus.paper.projection import load_paper_projection
                from nexus.persistence.models.economic import EconomicLaneCycleRecord

                async with sessions() as session:
                    link = await session.scalar(
                        select(EconomicLaneCycleRecord).where(
                            EconomicLaneCycleRecord.pair_slot_id == pair_id,
                            EconomicLaneCycleRecord.lane == lane,
                        )
                    )
                    projection = await load_paper_projection(
                        session, account_id=link.account_id, as_of=clock[0]
                    )
                assert lane_data["portfolio"]["portfolio_id"] == str(link.portfolio_id)
                assert lane_data["actual"]["data_state"] == "READY"
                from decimal import Decimal

                assert Decimal(lane_data["actual"]["available_cash"]) == projection.available_cash
                assert Decimal(lane_data["actual"]["nav"]) == projection.nav
                again = await client.get(lane_path, headers=headers("GET", lane_path))
                assert again.json()["data"] == lane_data
                queried_lane = lane_path + "?account_id=foreign"
                assert (
                    await client.get(queried_lane, headers=headers("GET", queried_lane))
                ).status_code == 422
                planned_lane_path = planned_path + f"/lanes/{lane}"
                planned_lane = await client.get(
                    planned_lane_path, headers=headers("GET", planned_lane_path)
                )
                assert planned_lane.status_code == 200
                assert planned_lane.json()["data"]["detail"] is None
            bad_lane = path + "/lanes/other"
            assert (await client.get(bad_lane, headers=headers("GET", bad_lane))).status_code == 422
            second = await client.get(path, headers=headers("GET", path))
            assert second.json()["data"] == data
            missing = f"/internal/v1/paper/economic-pairs/{uuid4()}"
            assert (await client.get(missing, headers=headers("GET", missing))).status_code == 404
            queried = path + "?account_id=foreign"
            assert (await client.get(queried, headers=headers("GET", queried))).status_code == 422
        if os.environ.get("NEXUS_TEST_LANE_BROWSER_VERIFY") == "1" and case in {"filled", "stage"}:
            from helpers.economic_lane_browser import verify_live_economic_pages

            await verify_live_economic_pages(application, case=case)
        assert await counts() == before
        assert any("REPEATABLE READ READ ONLY" in statement for statement in statements)
    finally:
        if timing is not None:
            timing.close()
        await engine.dispose()
