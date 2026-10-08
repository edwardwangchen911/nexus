"""Normal host identity preparation with synthetic SEC/S3 and isolated PostgreSQL."""

import json
from datetime import UTC, datetime, time, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx
import pytest
from helpers.current_identity import archives
from helpers.research_snapshots import seed_filing
from integration.test_agent_paper_candidate_batch_pg import (
    AAPL_ID,
    ACCOUNT_ID,
    NVDA_ID,
    UNIVERSE_ID,
    _seed_scope,
)
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops import agent_paper_identity as identity
from nexus.ops import research_identity_local as collector
from nexus.ops.instrument_identity_local import revise_listing
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.paper import AgentPaperCycleRecord, PaperFillRecord, PaperOrderRecord
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.research import sec_exhibit_storage
from nexus.research.current_identity_storage import SOURCE
from nexus.settings import EngineSettings
from sqlalchemy import event, func, literal, select
from unit.market.test_object_store import FakeS3Client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "ready",
        "partially_registered",
        "source_failure",
        "import_mismatch",
        "closed_window",
        "closed_during_collect",
        "closed_during_import",
        "existing_incomplete",
        "unsafe_parent",
    ],
)
async def test_formal_batch_preparation_once_without_cycle_or_order(
    migrated_database_url, tmp_path, monkeypatch, case
):
    # Explicit synthetic database/receipt clock in the current NY day, before
    # real wall time; this test does not manufacture any business evidence.
    day = datetime.now(UTC).astimezone(ZoneInfo("America/New_York")).date()
    now = datetime.combine(day, time(9, 35), ZoneInfo("America/New_York")).astimezone(UTC)
    clock = [now]
    monkeypatch.setattr(func, "clock_timestamp", lambda: literal(clock[0]))

    class ReceiptClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].replace(tzinfo=None) if tz is None else clock[0].astimezone(tz)

    monkeypatch.setattr(collector, "datetime", ReceiptClock)

    def published_master(_mapper, _connection, instrument):
        if instrument.id in (AAPL_ID, NVDA_ID):
            instrument.risk_bucket = "EQUITY"

    # Set synthetic published master values before INSERT; immutable database
    # guards stay enabled throughout this test.
    event.listen(Instrument, "before_insert", published_master)
    try:
        await _seed_scope(migrated_database_url)
    finally:
        event.remove(Instrument, "before_insert", published_master)
    settings = EngineSettings(database_url=migrated_database_url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-daily-identity",
            "object_store_prefix": "synthetic-daily-identity",
        }
    )
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    monkeypatch.setattr(sec_exhibit_storage, "S3ProviderEvidenceStore", lambda actual: store)
    calendar_id = uuid4()
    try:
        for symbol in ("AAPL", "NVDA"):
            fm, sc = archives(
                tmp_path / symbol, symbol=symbol, received=(now - timedelta(minutes=2)).isoformat()
            )
            await revise_listing(
                settings, symbol=symbol, fmp_archive=fm, sec_archive=sc, store=store
            )
        async with factory() as session, session.begin():
            await seed_filing(session, instrument_id=AAPL_ID, seed_instrument=False)
            evidence = ResearchEvidenceRef(
                source="sec",
                provider_object_id="filing:NVDA",
                revision="synthetic-v1",
                available_at=now - timedelta(days=1),
                created_at=now - timedelta(days=1),
                content_hash="b" * 64,
                location="memory://sha256/" + "b" * 64,
                byte_length=10,
            )
            session.add(evidence)
            await session.flush()
            session.add(
                Filing(
                    instrument_id=NVDA_ID,
                    evidence_ref_id=evidence.id,
                    source="sec",
                    provider_object_id=evidence.provider_object_id,
                    revision=evidence.revision,
                    accession_number="0000000001-26-000002",
                    form="10-Q",
                    filed_at=now - timedelta(days=1),
                    accepted_at=now - timedelta(days=1),
                    created_at=now - timedelta(days=1),
                )
            )
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=calendar_id,
                    source="exchange_calendars",
                    version="synthetic-identity-preparation",
                    exchange="XNYS",
                    range_start=day - timedelta(days=1),
                    range_end=day + timedelta(days=2),
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="c" * 64,
                    published_at=now - timedelta(days=2),
                    created_at=now - timedelta(days=2),
                )
            )
            for offset in (0, 1):
                session_day = day + timedelta(days=offset)
                session.add(
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=calendar_id,
                        session=session_day,
                        market_open_at=datetime.combine(
                            session_day, time(9, 30), ZoneInfo("America/New_York")
                        ).astimezone(UTC),
                        market_close_at=datetime.combine(
                            session_day, time(16), ZoneInfo("America/New_York")
                        ).astimezone(UTC),
                        decision_at=datetime.combine(
                            session_day, time(17), ZoneInfo("America/New_York")
                        ).astimezone(UTC),
                        created_at=now - timedelta(days=2),
                    )
                )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=ACCOUNT_ID,
                universe_snapshot_id=UNIVERSE_ID,
                instrument_ids=(AAPL_ID, NVDA_ID),
                idempotency_key="daily-identity",
                created_at=now,
            )
        calls = []
        original_client = httpx.AsyncClient

        def response(request):
            calls.append(request.url.path)
            if case == "source_failure":
                return httpx.Response(503)
            nvda_exchange = "Nasdaq" if case == "import_mismatch" else "NYSE"
            data = (
                {
                    "fields": ["cik", "name", "ticker", "exchange"],
                    "data": [
                        [123456, "Synthetic", "AAPL", "NYSE"],
                        [123456, "Synthetic", "NVDA", nvda_exchange],
                    ],
                }
                if request.url.path.endswith("company_tickers_exchange.json")
                else {
                    "cik": "123456",
                    "tickers": ["AAPL", "NVDA"],
                    "exchanges": ["NYSE", nvda_exchange],
                }
            )
            if case == "closed_during_collect" and len(calls) == 3:
                clock[0] = now + timedelta(minutes=26)
            return httpx.Response(200, content=json.dumps(data).encode())

        monkeypatch.setattr(
            collector.httpx,
            "AsyncClient",
            lambda **kw: original_client(**kw, transport=httpx.MockTransport(response)),
        )
        if case == "closed_window":
            clock[0] = now + timedelta(minutes=26)
        if case == "existing_incomplete":
            parent = identity._private_parent(tmp_path)
            (parent / day.strftime("%Y%m%d")).mkdir(mode=0o700)
        if case == "unsafe_parent":
            (tmp_path / ".secrets").mkdir(mode=0o755)
        if case == "closed_during_import":
            original_import = identity.import_identity_binding
            imports = []

            async def import_then_close(*args, **kwargs):
                result = await original_import(*args, **kwargs)
                imports.append(result)
                if len(imports) == 2:
                    clock[0] = now + timedelta(minutes=26)
                return result

            monkeypatch.setattr(identity, "import_identity_binding", import_then_close)
        args = dict(
            account_id=ACCOUNT_ID,
            batch_id=batch.batch_id,
            expected_calendar_authority_id=calendar_id,
            repo_root=tmp_path,
            contact="synthetic@example.invalid",
        )
        if case == "partially_registered":
            scope = await identity._read_scope(
                settings, account_id=ACCOUNT_ID, batch_id=batch.batch_id, calendar_id=calendar_id
            )
            assert scope is not None
            clock[0] = now - timedelta(minutes=1)
            prior_dir = identity._private_parent(tmp_path) / "prior-manual-collection"
            await collector.collect_identity_sources(
                symbols=("AAPL", "NVDA"), contact=args["contact"], output_dir=prior_dir
            )
            clock[0] = now
            async with factory() as session, session.begin():
                await identity.import_identity_binding(
                    session,
                    store,
                    identity_snapshot_id=scope.candidates[0][2],
                    collection_dir=prior_dir,
                )
            clock[0] = now
        if case == "unsafe_parent":
            with pytest.raises(ValueError, match="AGENT_PAPER_IDENTITY_DIRECTORY_INVALID"):
                await identity.prepare_agent_paper_identity_once(settings, **args)
            result = "unavailable"
        else:
            result = await identity.prepare_agent_paper_identity_once(settings, **args)
        success = case in ("ready", "partially_registered")
        assert result == ("prepared" if success else "unavailable")
        if case != "unsafe_parent":
            assert await identity.prepare_agent_paper_identity_once(settings, **args) == (
                "ready" if success else "unavailable"
            )
        assert len(calls) == (
            0
            if case in ("closed_window", "existing_incomplete", "unsafe_parent")
            else 1
            if case == "source_failure"
            else 6
            if case == "partially_registered"
            else 3
        )
        async with factory() as session:
            assert await session.scalar(
                select(func.count())
                .select_from(ResearchEvidenceRef)
                .where(ResearchEvidenceRef.source == SOURCE)
            ) == (2 if success else 0)
            for model in (AgentPaperCycleRecord, Job, PaperOrderRecord, PaperFillRecord):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
    finally:
        await factory.kw["bind"].dispose()
