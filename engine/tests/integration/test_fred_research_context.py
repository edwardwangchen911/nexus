"""Normal FRED import augments exact research, never its historical or standalone base."""

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

import exchange_calendars
import pytest
from helpers.research_snapshots import INSTRUMENT, seed_filing
from nexus.agents import snapshots as snapshot_module
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run, select_authoritative_snapshot
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops import agent_paper_loop as loop
from nexus.ops.daily_once import DailyOnceSettings
from nexus.ops.openai_local import prepare_database
from nexus.paper.candidate_batch import CandidateInstrumentIdentity
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataSnapshot, MacroObservation
from nexus.persistence.session import session_factory
from nexus.research.fred_observations import (
    FredCapture,
    FredVintageCollection,
    FredVintageRequest,
    parse_collected_vintage,
)
from nexus.research.fred_storage import import_fred_vintage, read_fred_observations
from pydantic import SecretStr
from sqlalchemy import func, select
from unit.market.test_object_store import FakeS3Client
from unit.research.test_fred_observations import parse


def _macro_collection(
    series: str, *, vintage: date, received_at: datetime
) -> FredVintageCollection:
    request = FredVintageRequest(
        series=series,
        observation_start=vintage,
        observation_end=vintage,
        vintage=vintage,
    )
    metadata = FredCapture(
        kind="series",
        offset=None,
        received_at=received_at,
        raw_payload=json.dumps(
            {
                "realtime_start": vintage.isoformat(),
                "realtime_end": vintage.isoformat(),
                "seriess": [
                    {
                        "id": series,
                        "realtime_start": vintage.isoformat(),
                        "realtime_end": vintage.isoformat(),
                        "units": "Percent",
                    }
                ],
            }
        ).encode(),
    )
    page = FredCapture(
        kind="observations",
        offset=0,
        received_at=received_at,
        raw_payload=json.dumps(
            {
                "realtime_start": vintage.isoformat(),
                "realtime_end": vintage.isoformat(),
                "units": "lin",
                "output_type": 1,
                "count": 1,
                "offset": 0,
                "limit": 16,
                "observation_start": vintage.isoformat(),
                "observation_end": vintage.isoformat(),
                "order_by": "observation_date",
                "sort_order": "asc",
                "observations": [
                    {
                        "date": vintage.isoformat(),
                        "realtime_start": vintage.isoformat(),
                        "realtime_end": vintage.isoformat(),
                        "value": "1",
                    }
                ],
            }
        ).encode(),
    )
    return parse_collected_vintage(request, metadata, (page,))


@pytest.mark.asyncio
@pytest.mark.parametrize("has_research_base", [True, False])
async def test_normal_fred_import_only_augments_new_frozen_research(
    migrated_database_url, has_research_base
):
    from nexus.ops.fred_local import import_collection
    from nexus.research.fred_storage import read_fred_observations

    settings = DailyOnceSettings.model_validate(
        {
            "DATABASE_URL": migrated_database_url,
            "OBJECT_STORE_ENDPOINT": "http://127.0.0.1:9000",
            "OBJECT_STORE_REGION": "us-east-1",
            "OBJECT_STORE_BUCKET": "synthetic-fred",
            "OBJECT_STORE_PREFIX": "synthetic-fred",
            "OBJECT_STORE_ACCESS_KEY_ID": "synthetic-access",
            "OBJECT_STORE_SECRET_ACCESS_KEY": "synthetic-secret",
        }
    )
    await prepare_database(settings)
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        if has_research_base:
            await seed_filing(session)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC))
        old_id = None if old is None else old.id
        old_hash = None if old is None else old.revision
        old_ids = [] if old is None else list(old.payload["evidence_ids"])

    result = await import_collection(
        settings,
        collection=parse(),
        store=S3ProviderEvidenceStore(settings, client=FakeS3Client()),
    )
    assert result["status"] == "imported" and result["execution_locked"] is True
    async with factory() as session, session.begin():
        cutoff = datetime.now(UTC)
        observations = await read_fred_observations(session, as_of=cutoff)
        assert len(observations) == 2
        latest = max(observations.values(), key=lambda row: row.observed_on)
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=cutoff)
        if not has_research_base:
            assert old is None and frozen is None
            return
        assert frozen is not None
        assert str(latest.evidence_ref_id) in frozen.payload["evidence_ids"]
        assert str(latest.calendar_evidence_ref_id) in frozen.payload["evidence_ids"]
        older_refs = {str(ref) for ref in observations if ref != latest.evidence_ref_id}
        assert not older_refs.intersection(frozen.payload["evidence_ids"])
        old = await session.get(DataSnapshot, old_id)
        assert old.revision == old_hash and old.payload["evidence_ids"] == old_ids
        for snapshot, expected_rows in ((old, 0), (frozen, 1)):
            decision = datetime.fromisoformat(snapshot.payload["as_of"])
            admitted = await admit_research_run(
                session,
                actor_id="test",
                route="/internal/v1/research-runs",
                idempotency_key=f"fred-frozen-{expected_rows}",
                request_hash=str(expected_rows) * 64,
                request=ResearchRequest(
                    instrument_id=INSTRUMENT,
                    data_snapshot_id=snapshot.id,
                    decision_time=decision,
                ),
                prompt_version_id=None,
                model_config_id=None,
                daily_limit_usd=Decimal("3"),
                now=decision,
            )
            tools = await database_read_only_tools(
                session, admitted.run, allow_fixture_evidence=False
            )
            result = await tools.execute(
                "get_macro_snapshot",
                {
                    "data_snapshot_id": str(snapshot.id),
                    "as_of": decision.isoformat(),
                    "series": ["DGS10"],
                },
            )
            assert len(result["items"]) == expected_rows
            if expected_rows:
                row = result["items"][0]
                assert Decimal(row["value"]) == 0
                assert row["unit"] == "Percent"
                assert row["vintage"] == "2026-08-28"
                assert row["calendar_evidence_ref_id"] == str(latest.calendar_evidence_ref_id)
                assert row["execution_locked"] is True

        # A bad row can reuse a legitimate ref without owning its canonical content.
        original = await session.get(MacroObservation, latest.observation_id)
        for index in range(2):
            session.add(
                MacroObservation(
                    source="fred",
                    series=("DGS2", "DFF")[index],
                    observed_at=original.observed_at,
                    available_at=original.available_at,
                    revision=original.revision,
                    value=Decimal("99"),
                    unit="Percent",
                    provider_object_id=original.provider_object_id,
                    authority_version="research_macro_v1",
                    real_time_start=original.real_time_start,
                    real_time_end=original.real_time_end,
                    evidence_ref_id=original.evidence_ref_id,
                    calendar_evidence_ref_id=original.calendar_evidence_ref_id,
                    calendar_source=original.calendar_source,
                    calendar_provider_object_id=original.calendar_provider_object_id,
                    calendar_revision=original.calendar_revision,
                    display_sanity_only=False,
                )
            )
        await session.flush()
        assert len(list(await session.scalars(select(MacroObservation)))) == 4
        result = await tools.execute(
            "get_macro_snapshot",
            {
                "data_snapshot_id": str(frozen.id),
                "as_of": cutoff.isoformat(),
                "series": ["DGS10", "DGS2", "DFF"],
            },
        )
        assert [row["series"] for row in result["items"]] == ["DGS10"]


@pytest.mark.asyncio
async def test_agent_paper_binds_complete_macro_evidence_before_continuing(
    migrated_database_url, monkeypatch: pytest.MonkeyPatch, tmp_path
):
    settings = DailyOnceSettings.model_validate(
        {
            "DATABASE_URL": migrated_database_url,
            "OBJECT_STORE_ENDPOINT": "http://127.0.0.1:9000",
            "OBJECT_STORE_REGION": "us-east-1",
            "OBJECT_STORE_BUCKET": "synthetic-agent-paper-macro",
            "OBJECT_STORE_PREFIX": "synthetic-agent-paper-macro",
            "OBJECT_STORE_ACCESS_KEY_ID": "synthetic-access",
            "OBJECT_STORE_SECRET_ACCESS_KEY": "synthetic-secret",
        }
    )
    await prepare_database(settings)
    factory = session_factory(settings)
    async with factory() as session:
        database_now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(database_now, datetime)
    fixed_now = database_now
    local_date = fixed_now.astimezone(ZoneInfo("America/New_York")).date()
    calendar_authority_id = UUID("0199c8a1-0000-7000-8000-000000000001")
    async with factory() as session, session.begin():
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=calendar_authority_id,
                exchange="XNYS",
                version="fred-research-context-v1",
                source="fixture",
                range_start=local_date - timedelta(days=7),
                range_end=local_date + timedelta(days=7),
                query_parameters={},
                completeness_status="complete",
                content_hash="f" * 64,
                published_at=fixed_now - timedelta(days=1),
                created_at=fixed_now - timedelta(days=1),
            )
        )
        official_calendar = exchange_calendars.get_calendar("XNYS")
        for label in official_calendar.sessions_in_range(
            local_date - timedelta(days=7),
            local_date + timedelta(days=7),
        ):
            session_date = label.date()
            market_close_at = official_calendar.session_close(label).to_pydatetime()
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=calendar_authority_id,
                    session=session_date,
                    market_open_at=official_calendar.session_open(label).to_pydatetime(),
                    market_close_at=market_close_at,
                    decision_at=market_close_at + timedelta(minutes=1),
                    created_at=fixed_now - timedelta(days=1),
                )
            )
    initial_as_of = fixed_now
    async with factory() as session:
        vintage = await loop.latest_visible_macro_vintage(
            session,
            now=fixed_now,
            expected_calendar_authority_id=calendar_authority_id,
        )

    async with factory() as session, session.begin():
        await seed_filing(session)
        original = await prepare_research_snapshot(
            session,
            INSTRUMENT,
            as_of=initial_as_of,
        )
        assert original is not None
        original_payload = dict(original.payload)

    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    received_at = fixed_now - timedelta(minutes=1)
    async with factory() as session, session.begin():
        for series in loop._PAPER_MACRO_SERIES:
            await import_fred_vintage(
                session,
                collection=_macro_collection(
                    series,
                    vintage=vintage,
                    received_at=received_at,
                ),
                store=store,
            )
    async with factory() as session:
        decision_now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(decision_now, datetime)
        scoped = await read_fred_observations(
            session,
            as_of=decision_now,
            series=loop._PAPER_MACRO_SERIES,
            vintage=vintage,
        )
    assert {row.series for row in scoped.values()} == loop._PAPER_MACRO_SERIES

    async def prepare_scoped_snapshot(session, instrument_id, *, as_of, **kwargs):
        assert kwargs["macro_series"] == loop._PAPER_MACRO_SERIES
        assert kwargs["macro_vintage"] == vintage
        return await prepare_research_snapshot(
            session,
            instrument_id,
            as_of=as_of,
            **kwargs,
        )

    async def inspect_prior(
        _settings, *, account_id, now, expected_calendar_authority_id, candidate
    ):
        assert expected_calendar_authority_id == calendar_authority_id
        return loop.PriorDailyScope(
            instrument_id=INSTRUMENT,
            prior_session=vintage,
            authority_ready=True,
            decision_session=vintage + timedelta(days=1),
        )

    async def unexpected_provider(*_args, **_kwargs):
        raise AssertionError("complete persisted macro evidence must not call FRED")

    monkeypatch.setattr(loop, "_inspect_prior_daily_scope", inspect_prior)
    monkeypatch.setattr(loop, "collect_fred", unexpected_provider)
    monkeypatch.setattr(loop, "prepare_research_snapshot", prepare_scoped_snapshot)

    result = await loop.prepare_macro_once(
        settings,
        candidate=CandidateInstrumentIdentity(
            instrument_id=INSTRUMENT,
            symbol="AAPL",
            asset_type="common_stock",
            exchange="XNYS",
            currency="USD",
        ),
        fred_key=SecretStr("unused-test-key"),
        account_id=UUID(int=1),
        repo_root=tmp_path,
        now=decision_now,
        expected_calendar_authority_id=calendar_authority_id,
    )

    assert result == "prepared"
    assert not (tmp_path / ".secrets").exists()
    scope = await loop._inspect_macro_scope(
        settings,
        candidate=CandidateInstrumentIdentity(
            instrument_id=INSTRUMENT,
            symbol="AAPL",
            asset_type="common_stock",
            exchange="XNYS",
            currency="USD",
        ),
        account_id=UUID(int=1),
        now=decision_now,
        expected_calendar_authority_id=calendar_authority_id,
    )
    assert scope.ready_series == loop._PAPER_MACRO_SERIES
    assert scope.snapshot_ready is True
    async with factory() as session:
        read_now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(read_now, datetime)
        current = await select_authoritative_snapshot(
            session,
            instrument_id=INSTRUMENT,
            as_of=read_now,
        )
        persisted_original = await session.get(DataSnapshot, original.id)
        assert current is not None and current.id != original.id
        assert persisted_original is not None
        assert persisted_original.payload == original_payload
        admitted = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="agent-paper-complete-macro",
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT,
                data_snapshot_id=current.id,
                decision_time=read_now,
            ),
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=Decimal("3"),
            now=read_now,
        )
        tools = await database_read_only_tools(
            session,
            admitted.run,
            allow_fixture_evidence=False,
        )
        result = await tools.execute(
            "get_macro_snapshot",
            {
                "data_snapshot_id": str(current.id),
                "as_of": read_now.isoformat(),
                "series": sorted(loop._PAPER_MACRO_SERIES),
            },
        )
        assert {row["series"] for row in result["items"]} == loop._PAPER_MACRO_SERIES
        assert {row["vintage"] for row in result["items"]} == {vintage.isoformat()}


@pytest.mark.asyncio
async def test_current_trends_snapshot_does_not_bind_partial_macro_vintage(
    migrated_database_url, monkeypatch: pytest.MonkeyPatch
):
    settings = DailyOnceSettings.model_validate(
        {
            "DATABASE_URL": migrated_database_url,
            "OBJECT_STORE_ENDPOINT": "http://127.0.0.1:9000",
            "OBJECT_STORE_REGION": "us-east-1",
            "OBJECT_STORE_BUCKET": "synthetic-agent-paper-partial-macro",
            "OBJECT_STORE_PREFIX": "synthetic-agent-paper-partial-macro",
            "OBJECT_STORE_ACCESS_KEY_ID": "synthetic-access",
            "OBJECT_STORE_SECRET_ACCESS_KEY": "synthetic-secret",
        }
    )
    await prepare_database(settings)
    factory = session_factory(settings)
    fixed_now = datetime(2026, 9, 16, 15, tzinfo=UTC)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now.replace(tzinfo=None) if tz is None else fixed_now.astimezone(tz)

    monkeypatch.setattr(snapshot_module, "datetime", FixedDateTime)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    old_vintage = date(2026, 9, 14)
    new_vintage = date(2026, 9, 15)
    async with factory() as session, session.begin():
        await seed_filing(session)
        for series in loop._PAPER_MACRO_SERIES:
            await import_fred_vintage(
                session,
                collection=_macro_collection(
                    series,
                    vintage=old_vintage,
                    received_at=datetime(2026, 9, 14, 14, tzinfo=UTC),
                ),
                store=store,
            )
        await import_fred_vintage(
            session,
            collection=_macro_collection(
                "DFF",
                vintage=new_vintage,
                received_at=datetime(2026, 9, 15, 14, tzinfo=UTC),
            ),
            store=store,
        )

    await loop._prepare_current_trends_research_snapshot(
        settings,
        instrument_id=INSTRUMENT,
        now=fixed_now,
    )

    async with factory() as session:
        snapshot = await select_authoritative_snapshot(
            session,
            instrument_id=INSTRUMENT,
            as_of=fixed_now,
        )
        assert snapshot is not None
        admitted = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="agent-paper-partial-macro",
            request_hash="b" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT,
                data_snapshot_id=snapshot.id,
                decision_time=fixed_now,
            ),
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=Decimal("3"),
            now=fixed_now,
        )
        tools = await database_read_only_tools(
            session,
            admitted.run,
            allow_fixture_evidence=False,
        )
        result = await tools.execute(
            "get_macro_snapshot",
            {
                "data_snapshot_id": str(snapshot.id),
                "as_of": fixed_now.isoformat(),
                "series": sorted(loop._PAPER_MACRO_SERIES),
            },
        )
        assert result["items"] == []
