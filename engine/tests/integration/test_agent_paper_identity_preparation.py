"""Normal host identity preparation with synthetic SEC/S3 and isolated PostgreSQL."""

import json
from datetime import UTC, datetime, time, timedelta
from types import SimpleNamespace
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
        "manual_after_slot",
        "scheduled_after_slot",
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
    migrated_database_url, tmp_path, monkeypatch, case, capsys
):
    # Use one coherent synthetic clock in a past NY day, including snapshot
    # compilation; this must also pass before today's market opens.
    day = (datetime.now(UTC) - timedelta(days=1)).astimezone(ZoneInfo("America/New_York")).date()
    now = datetime.combine(day, time(9, 35), ZoneInfo("America/New_York")).astimezone(UTC)
    clock = [now]
    monkeypatch.setattr(func, "clock_timestamp", lambda: literal(clock[0]))

    class ClockMeta(type):
        def __instancecheck__(cls, value):
            return isinstance(value, datetime)

    class ReceiptClock(datetime, metaclass=ClockMeta):
        @classmethod
        def now(cls, tz=None):
            return clock[0].replace(tzinfo=None) if tz is None else clock[0].astimezone(tz)

    monkeypatch.setattr(collector, "datetime", ReceiptClock)
    monkeypatch.setattr(identity, "datetime", ReceiptClock)
    from nexus.ops import agent_paper_loop

    monkeypatch.setattr(agent_paper_loop, "datetime", ReceiptClock)

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
            if case == "manual_after_slot":
                await _seed_funded_projection(session, at=now - timedelta(minutes=1))
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
        if case in {"manual_after_slot", "scheduled_after_slot"}:
            clock[0] = now + timedelta(minutes=26)
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
        if case in {"manual_after_slot", "scheduled_after_slot"}:
            from nexus.ops import formal_research_preparation as preparation
            from nexus.paper.agent_cycle_authority import (
                load_agent_paper_service_calendar_authority,
            )

            # This test isolates the source/archive/builder boundary. Native claim
            # ownership and request-hash validation have separate focused coverage.
            async def authorized_execution(session, context, *, now):
                assert context is args["execution"]
                return await load_agent_paper_service_calendar_authority(
                    session, now=now, expected_calendar_authority_id=calendar_id
                )

            monkeypatch.setattr(preparation, "require_preparation_scope", authorized_execution)
            args["execution"] = SimpleNamespace(
                account_id=ACCOUNT_ID,
                calendar_id=calendar_id,
                reservation=SimpleNamespace(
                    candidate_batch_id=batch.batch_id,
                    decision_session=day,
                    trigger_mode="manual" if case == "manual_after_slot" else "scheduled",
                    decision_slot_at=now + timedelta(minutes=25),
                ),
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
        success = case in ("ready", "partially_registered", "manual_after_slot")
        assert result == ("prepared" if success else "unavailable")
        diagnostics = [
            json.loads(line)
            for line in capsys.readouterr().err.splitlines()
            if line.startswith('{"event": "agent_paper_preparation_unavailable"')
        ]
        if not success and case != "unsafe_parent":
            assert len(diagnostics) == 1
            assert diagnostics[0]["failure_phase"] == "research_identity"
            assert (
                diagnostics[0]["failure_point"]
                == {
                    "source_failure": "collect",
                    "import_mismatch": "import",
                    "closed_window": "scope",
                    "scheduled_after_slot": "scope",
                    "closed_during_collect": "readback",
                    "closed_during_import": "import",
                    "existing_incomplete": "archive",
                }[case]
            )
        if case != "unsafe_parent":
            assert await identity.prepare_agent_paper_identity_once(settings, **args) == (
                "ready" if success else "unavailable"
            )
        assert len(calls) == (
            0
            if case
            in ("closed_window", "existing_incomplete", "unsafe_parent", "scheduled_after_slot")
            else 1
            if case == "source_failure"
            else 6
            if case == "partially_registered"
            else 3
        )
        if case == "manual_after_slot":
            # Freeze with the normal builder after all sources, then prove exact
            # selection and current-session identity on the persisted snapshot.
            context = args["execution"]
            scope = await preparation.read_preparation_scope(settings, context)
            clock[0] += timedelta(seconds=1)
            await preparation.freeze_prepared_snapshots(settings, context, scope)
            frozen_at = clock[0]
            clock[0] += timedelta(seconds=10)
            async with factory() as session, session.begin():
                for instrument_id in (AAPL_ID, NVDA_ID):
                    snapshot = await identity.select_authoritative_snapshot(
                        session, instrument_id=instrument_id, as_of=clock[0]
                    )
                    assert snapshot.observed_at == frozen_at
                    diagnosis = await identity.inspect_identity_binding_snapshot(
                        session,
                        instrument_id=instrument_id,
                        snapshot=snapshot,
                        as_of=clock[0],
                        decision_session=day,
                    )
                    assert diagnosis["snapshot_status"] == "ready"
                    bindings = await identity.read_identity_bindings(
                        session,
                        instrument_id=instrument_id,
                        as_of=clock[0],
                        decision_session=day,
                    )
                    assert bindings
                    assert all(item.decision_session == day for item in bindings.values())
                    await _assert_final_paper_identity(
                        session,
                        store,
                        snapshot,
                        instrument_id,
                        clock[0],
                        day,
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


async def _assert_final_paper_identity(session, store, base, instrument_id, decision_time, day):
    """Use the cycle's real final-context consumer with native price/quote rows."""
    from copy import deepcopy
    from decimal import Decimal
    from hashlib import sha256

    from nexus.agents.paper_context import (
        prepare_agent_paper_research_snapshot,
        read_agent_paper_research_context,
    )
    from nexus.market.ingestion import ingest_price_page
    from nexus.market.intraday_ingestion import canonical_quote_observation
    from nexus.paper.agent_cycle_models import CandidateManifestEntry
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
    from nexus.persistence.models.data import DataArtifact, DataSnapshot
    from nexus.persistence.models.market import LatestQuote
    from nexus.persistence.models.paper import PAPER_DECISION_SOURCE_POLICY_ID
    from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage

    original = (deepcopy(base.payload), base.revision, base.observed_at)
    before_ids = set(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == base.id,
                AgentDataSnapshotEvidenceRecord.source == SOURCE,
            )
        )
    )
    assert before_ids
    prior = day - timedelta(days=1)
    price = await ingest_price_page(
        session,
        store,
        ProviderPage(
            items=(
                PriceBar(
                    instrument_id=instrument_id,
                    session=prior,
                    open=Decimal("100"),
                    high=Decimal("100"),
                    low=Decimal("100"),
                    close=Decimal("100"),
                    volume=Decimal("1000"),
                    observed_at=decision_time - timedelta(days=1),
                    available_at=decision_time,
                    received_at=decision_time,
                    revision="final-context-prior",
                    quality_flags=(),
                ),
            ),
            next_cursor=None,
            raw_payload=b'{"synthetic":"final-context-prior"}',
            source_request_id=None,
            as_of=decision_time,
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
    assert price.price_authority is not None
    digest = sha256(str(instrument_id).encode()).hexdigest()
    artifact = DataArtifact(
        id=uuid4(),
        uri=f"memory://sha256/{digest}",
        sha256=digest,
        byte_size=100,
        mime_type="application/json",
        schema_version="schwab_level_one_stream_v1",
        source="schwab",
        observed_at=decision_time,
        available_at=decision_time,
        received_at=decision_time,
        created_at=decision_time,
    )
    values = dict(
        instrument_id=instrument_id,
        source="schwab",
        feed="l1",
        delivery_mode="real_time",
        event_kind="quote",
        bid=Decimal("99.9"),
        ask=Decimal("100.1"),
        last=None,
        observed_at=decision_time,
        available_at=decision_time,
        received_at=decision_time,
        revision=digest,
    )
    quote = LatestQuote(
        id=uuid4(),
        **values,
        quality_flags=[],
        evidence_status="verified",
        data_artifact_id=artifact.id,
        created_at=decision_time,
        content_hash=sha256(canonical_quote_observation(**values)).hexdigest(),
    )
    session.add_all((artifact, quote))
    await session.flush()
    candidate = CandidateManifestEntry(
        instrument_id=instrument_id,
        symbol="AAPL" if instrument_id == AAPL_ID else "NVDA",
        inclusion_reason="synthetic",
        research_snapshot_id=base.id,
        prior_daily_price_authority_id=price.price_authority.id,
        decision_quote_id=quote.id,
        decision_trade_id=None,
        decision_artifact_id=artifact.id,
        decision_evidence_hash=digest,
        risk_query_authority_ids=(uuid4(), uuid4(), uuid4()),
    )
    projection = await load_paper_projection(session, account_id=ACCOUNT_ID, as_of=decision_time)
    assert projection.available_cash == projection.nav == projection.peak_nav == Decimal("100000")
    assert projection.reserved_cash == 0
    assert projection.positions == projection.open_orders == ()
    assert projection.drawdown_state == "unlocked"
    final = await prepare_agent_paper_research_snapshot(
        session,
        cycle_id=uuid4(),
        decision_time=decision_time,
        candidate=candidate,
        projection=projection,
    )
    assert final.id != base.id and final.observed_at == decision_time > base.observed_at
    final_ids = set(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == final.id,
                AgentDataSnapshotEvidenceRecord.source == SOURCE,
            )
        )
    )
    assert final_ids == before_ids
    bindings = await identity.read_identity_bindings(
        session,
        instrument_id=instrument_id,
        as_of=decision_time,
        decision_session=day,
        evidence_ids=tuple(final_ids),
    )
    assert set(bindings) == final_ids
    assert all(item.decision_session == day for item in bindings.values())
    context = await read_agent_paper_research_context(
        session, snapshot_id=final.id, instrument_id=instrument_id, as_of=decision_time
    )
    assert context is not None and context[0].decision_time == decision_time
    unchanged = await session.get(DataSnapshot, base.id, populate_existing=True)
    assert (unchanged.payload, unchanged.revision, unchanged.observed_at) == original


async def _seed_funded_projection(session, *, at):
    """Seed the same balanced funding and drawdown genesis as native account setup."""
    from decimal import Decimal
    from hashlib import sha256

    from nexus.paper.ledger import _EXPECTED_NORMAL_SIDES
    from nexus.paper.models import LedgerEntry, LedgerTransaction
    from nexus.persistence.models.paper import (
        PaperAccountRecord,
        PaperDrawdownLockEventRecord,
        PaperLedgerAccountRecord,
        PaperLedgerEntryRecord,
        PaperLedgerTransactionRecord,
    )
    from nexus.risk.canonical import canonical_paper_ledger_transaction_hash

    account = await session.get(PaperAccountRecord, ACCOUNT_ID)
    assert account is not None
    capital = account.initial_capital
    catalog = {
        code: PaperLedgerAccountRecord(
            id=uuid4(),
            paper_account_id=ACCOUNT_ID,
            code=code,
            normal_side=side,
            currency="USD",
            created_at=at,
        )
        for code, side in _EXPECTED_NORMAL_SIDES.items()
    }
    session.add_all(catalog.values())
    await session.flush()
    draft = LedgerTransaction(
        transaction_id=uuid4(),
        account_id=ACCOUNT_ID,
        transaction_type="funding",
        reference_type="initial_funding",
        reference_hash=sha256(f"identity-test-funding:{ACCOUNT_ID}:{capital}".encode()).hexdigest(),
        occurred_at=at,
        entries=(
            LedgerEntry(
                entry_id=uuid4(),
                entry_seq=1,
                account_code="asset.cash.available.usd",
                currency="USD",
                debit_amount=capital,
                credit_amount=Decimal(0),
            ),
            LedgerEntry(
                entry_id=uuid4(),
                entry_seq=2,
                account_code="equity.contributed_capital.usd",
                currency="USD",
                debit_amount=Decimal(0),
                credit_amount=capital,
            ),
        ),
        transaction_hash="0" * 64,
    )
    session.add(
        PaperLedgerTransactionRecord(
            id=draft.transaction_id,
            account_id=ACCOUNT_ID,
            order_id=None,
            transaction_type=draft.transaction_type,
            reference_type=draft.reference_type,
            reference_hash=draft.reference_hash,
            occurred_at=at,
            created_at=at,
            transaction_hash=canonical_paper_ledger_transaction_hash(draft),
        )
    )
    await session.flush()
    session.add_all(
        PaperLedgerEntryRecord(
            id=entry.entry_id,
            transaction_id=draft.transaction_id,
            account_id=ACCOUNT_ID,
            entry_seq=entry.entry_seq,
            ledger_account_id=catalog[entry.account_code].id,
            account_code=entry.account_code,
            currency="USD",
            debit_amount=entry.debit_amount,
            credit_amount=entry.credit_amount,
            created_at=at,
        )
        for entry in draft.entries
    )
    session.add(
        PaperDrawdownLockEventRecord(
            id=uuid4(),
            account_id=ACCOUNT_ID,
            event_seq=1,
            from_state=None,
            to_state="unlocked",
            drawdown_ratio=Decimal(0),
            user_acknowledged=False,
            reason_code="PAPER_ACCOUNT_INITIALIZED",
            occurred_at=at,
            created_at=at,
            payload_hash=sha256(
                f"identity-test-drawdown:{ACCOUNT_ID}:{at.isoformat()}".encode()
            ).hexdigest(),
        )
    )
    await session.flush()
