"""Installed calendar producer; other source authorities remain isolated synthetic fixtures."""

import asyncio
import json
import os
import sys
from datetime import UTC, date, datetime, timedelta
from uuid import UUID, uuid4

import exchange_calendars
import httpx
import pytest
from helpers.portfolio_api import app, headers
from nexus.backtest.authority_loader import _load_calendar
from nexus.backtest.calendar import calendar_definition, prepare_calendar
from nexus.backtest.cost_policy import BACKTEST_COST_POLICY_ID
from nexus.persistence.models.backtest import (
    BacktestAuthorityBundleRecord,
    BacktestAuthorityCollectionRecord,
    BacktestCorporateActionAuthorityMemberRecord,
    BacktestRunRecord,
    BacktestSectorAuthorityMemberRecord,
    BacktestUniverseAuthorityMemberRecord,
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from engine.tests.integration.test_backtest_bundle import cli as bundle_cli
from engine.tests.integration.test_backtest_writer import (
    COMPLETED_AT,
    UNIVERSE_ID,
    _dataset,
    _raw_inputs,
    _seed_upstream_authorities,
)


async def calendar_cli(url, start="2024-11-27", end="2024-11-29"):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "nexus.ops.backtest_once",
        "--prepare-calendar",
        "--start",
        start,
        "--end",
        end,
        env={**os.environ, "DATABASE_URL": url},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
    except TimeoutError:
        process.kill()
        await process.wait()
        raise
    return process.returncode, stdout, stderr


@pytest.mark.asyncio
async def test_calendar_cli_persists_exact_sessions_at_actual_time_and_only_prepares(
    migrated_database_url,
):
    before = datetime.now(UTC)
    code, stdout, stderr = await calendar_cli(migrated_database_url)
    assert code == 0, stderr.decode()
    output = json.loads(stdout)
    assert output["status"] == "BACKTEST_CALENDAR_PREPARED"
    assert output["execution_locked"] and not output["backtest_executed"]
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        root = await session.get(
            ExchangeCalendarAuthorityRecord, UUID(output["calendar_authority_id"])
        )
        assert before <= root.published_at <= datetime.now(UTC)
        assert before <= root.created_at <= datetime.now(UTC)
        loaded = await _load_calendar(session, root.id, datetime.now(UTC))
        assert loaded.content_hash == output["content_hash"]
        assert loaded.sessions == (date(2024, 11, 27), date(2024, 11, 29))
        rows = tuple(
            await session.scalars(
                select(ExchangeCalendarSessionRecord).order_by(
                    ExchangeCalendarSessionRecord.session
                )
            )
        )
        assert [row.market_open_at.isoformat() for row in rows] == [
            "2024-11-27T14:30:00+00:00",
            "2024-11-29T14:30:00+00:00",
        ]
        assert [row.market_close_at.isoformat() for row in rows] == [
            "2024-11-27T21:00:00+00:00",
            "2024-11-29T18:00:00+00:00",
        ]
        assert all(before <= row.created_at <= datetime.now(UTC) for row in rows)
        for model in (BacktestAuthorityBundleRecord, BacktestRunRecord):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
    replay_code, replay, replay_error = await calendar_cli(migrated_database_url)
    assert replay_code == 0, replay_error.decode()
    assert json.loads(replay) == output


async def other_sources(url, calendar_id):
    """Only upstream synthetic authorities, no calendar/bundle/snapshot/result insertion."""
    factory = session_factory(EngineSettings(database_url=url))
    _, bars, sessions, protocol = _raw_inputs()
    prices = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, prices)
    collections = (
        (
            data.authority.universe_collection,
            BacktestUniverseAuthorityMemberRecord,
            "universe_snapshot_id",
        ),
        (
            data.authority.sector_collection,
            BacktestSectorAuthorityMemberRecord,
            "sector_exposure_id",
        ),
        (
            data.authority.corporate_action_collection,
            BacktestCorporateActionAuthorityMemberRecord,
            "corporate_action_id",
        ),
    )
    async with factory.begin() as session:
        for collection, member_type, member_field in collections:
            session.add(
                BacktestAuthorityCollectionRecord(
                    id=collection.authority_id,
                    kind=collection.kind,
                    source=collection.source,
                    range_start=collection.range_start,
                    range_end=collection.range_end,
                    query_parameters=dict(collection.query_parameters),
                    completeness_status=collection.completeness_status,
                    content_hash=collection.content_hash,
                    completed_at=COMPLETED_AT,
                )
            )
            await session.flush()
            session.add_all(
                member_type(
                    **{
                        "collection_id": collection.authority_id,
                        member_field: member,
                    }
                )
                for member in collection.member_ids
            )
    return {
        "strategy_version": "baseline_v1",
        "calendar_authority_id": calendar_id,
        **{
            f"{collection.kind}_collection_id": str(collection.authority_id)
            for collection, _, _ in collections
        },
        "spy_instrument_alias_id": str(data.authority.spy_benchmark.alias_id),
        "equal_weight_universe_snapshot_id": str(UNIVERSE_ID),
        "source_policy_id": str(SOURCE_POLICY_ID),
        "cost_policy_id": str(BACKTEST_COST_POLICY_ID),
        "start": protocol.start.isoformat(),
        "end": protocol.end.isoformat(),
        "initial_nav": str(protocol.initial_nav),
        "idempotency_key": "installed-calendar-bundle",
    }


@pytest.mark.asyncio
async def test_installed_calendar_cli_to_bundle_cli_backtest_and_authenticated_get(
    migrated_database_url,
    tmp_path,
):
    _, _, sessions, _ = _raw_inputs()
    code, output, error = await calendar_cli(
        migrated_database_url,
        sessions[0].isoformat(),
        sessions[-1].isoformat(),
    )
    assert code == 0, error.decode()
    calendar = json.loads(output)
    request = await other_sources(migrated_database_url, calendar["calendar_authority_id"])
    path = tmp_path / "synthetic-sources.json"
    path.write_text(json.dumps({**request, "corporate_action_collection_id": str(uuid4())}))
    invalid_code, invalid_stdout, invalid_stderr = await bundle_cli(migrated_database_url, path)
    assert invalid_code == 1 and invalid_stdout == b""
    assert json.loads(invalid_stderr)["status"] == "failed"
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        for model in (BacktestAuthorityBundleRecord, BacktestRunRecord):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
    path.write_text(json.dumps(request))
    code, stdout, stderr = await bundle_cli(migrated_database_url, path)
    assert code == 0, stderr.decode()
    result = json.loads(stdout)
    assert result["status"] == "BACKTEST_COMPLETED"
    assert result["execution_locked"] and not result["promotable"]
    route = "/internal/v1/strategies"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(route, headers=headers("GET", route))
    assert response.status_code == 200
    summary = response.json()["data"][0]["latest_backtest"]
    assert summary["result_hash"] == result["result_hash"] and summary["data_mode"] == "fixture"
    # The normal replay remains valid after a result has sealed the calendar.
    replay_code, replay, replay_error = await calendar_cli(
        migrated_database_url,
        sessions[0].isoformat(),
        sessions[-1].isoformat(),
    )
    assert replay_code == 0, replay_error.decode()
    assert json.loads(replay) == calendar


@pytest.mark.asyncio
async def test_concurrent_calendar_preparation_has_one_natural_key(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async def prepare():
        async with factory.begin() as session:
            return await prepare_calendar(session, date(2024, 3, 8), date(2024, 3, 11))

    calendars = await asyncio.gather(*(prepare() for _ in range(4)))
    assert len({item.authority_id for item in calendars}) == 1
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(ExchangeCalendarAuthorityRecord))
            == 1
        )
        assert (
            await session.scalar(select(func.count()).select_from(ExchangeCalendarSessionRecord))
            == 2
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "problem",
    [
        "hash",
        "source",
        "query",
        "missing",
        "extra",
        "open",
        "decision",
        "late_child",
        "late_root",
    ],
)
async def test_bad_existing_calendar_is_rejected_without_repair(problem, migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    definition = calendar_definition(date(2024, 11, 27), date(2024, 11, 29))
    official = exchange_calendars.get_calendar("XNYS")
    root_values = definition.model_dump(exclude={"authority_id", "sessions", "decision_times"})
    root_values.update(id=definition.authority_id, published_at=datetime.now(UTC))
    if problem == "hash":
        root_values["content_hash"] = "a" * 64
    elif problem == "source":
        root_values["source"] = "not-installed-library"
    elif problem == "query":
        root_values["query_parameters"] = {"exchange": "XNYS"}
    elif problem == "late_root":
        root_values["created_at"] = datetime.now(UTC) + timedelta(days=1)
    dates = list(definition.sessions)
    if problem == "missing":
        dates.pop()
    elif problem == "extra":
        dates.append(date(2024, 12, 2))
    async with factory.begin() as session:
        session.add(ExchangeCalendarAuthorityRecord(**root_values))
        await session.flush()
        for day in dates:
            values = {
                "calendar_authority_id": definition.authority_id,
                "session": day,
                "market_open_at": official.session_open(day).to_pydatetime(),
                "market_close_at": official.session_close(day).to_pydatetime(),
                "decision_at": official.session_close(day).to_pydatetime() + timedelta(minutes=30),
            }
            if problem == "open":
                values["market_open_at"] += timedelta(minutes=1)
            elif problem == "decision":
                values["decision_at"] += timedelta(minutes=1)
            elif problem == "late_child":
                values["created_at"] = datetime.now(UTC) + timedelta(days=1)
            session.add(ExchangeCalendarSessionRecord(**values))
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="BACKTEST_CALENDAR_AUTHORITY_INVALID"):
            await prepare_calendar(session, definition.range_start, definition.range_end)
        assert (
            await session.scalar(select(func.count()).select_from(ExchangeCalendarAuthorityRecord))
            == 1
        )
        assert await session.scalar(
            select(func.count()).select_from(ExchangeCalendarSessionRecord)
        ) == len(dates)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["last_insert", "readback"])
async def test_calendar_savepoint_rolls_back_complete_root_on_failure(
    failure, migrated_database_url
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory.begin() as session:
        body = (
            "IF NEW.session = '2024-11-29'::date THEN "
            "RAISE EXCEPTION 'synthetic insert failure'; END IF;"
            if failure == "last_insert"
            else "NEW.decision_at := NEW.decision_at + interval '1 minute';"
        )
        await session.execute(
            text(
                "CREATE FUNCTION synthetic_calendar_failure() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ "
                f"BEGIN {body} RETURN NEW; END; $$"
            )
        )
        await session.execute(
            text(
                "CREATE TRIGGER synthetic_calendar_failure "
                "BEFORE INSERT ON exchange_calendar_sessions "
                "FOR EACH ROW EXECUTE FUNCTION synthetic_calendar_failure()"
            )
        )
    async with factory.begin() as session:
        with pytest.raises(DBAPIError if failure == "last_insert" else ValueError):
            await prepare_calendar(session, date(2024, 11, 27), date(2024, 11, 29))
        for model in (ExchangeCalendarAuthorityRecord, ExchangeCalendarSessionRecord):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


@pytest.mark.asyncio
async def test_fosun_display_calendar_is_not_relabelled_or_reused(migrated_database_url):
    from nexus.ops.fosun_local_refresh import _insert_calendar

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    official = exchange_calendars.get_calendar("XNYS")
    dates = (date(2024, 11, 27), date(2024, 11, 29))
    async with factory.begin() as session:
        old = await _insert_calendar(
            session,
            rows=tuple(
                (
                    day,
                    official.session_open(day).to_pydatetime(),
                    official.session_close(day).to_pydatetime(),
                )
                for day in dates
            ),
            range_start=dates[0],
            range_end=dates[-1],
            published_at=datetime.now(UTC),
        )
        original = (old.id, old.version, old.content_hash)
    async with factory.begin() as session:
        new = await prepare_calendar(session, dates[0], dates[-1])
        old = await session.get(ExchangeCalendarAuthorityRecord, original[0])
        assert (old.id, old.version, old.content_hash) == original
        assert new.authority_id != old.id
        old_rows = tuple(
            await session.scalars(
                select(ExchangeCalendarSessionRecord).where(
                    ExchangeCalendarSessionRecord.calendar_authority_id == old.id,
                )
            )
        )
        assert all(row.decision_at == row.market_close_at for row in old_rows)
