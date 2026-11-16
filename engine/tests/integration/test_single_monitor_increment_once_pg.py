"""Normal v6 root and canonical market-input fixture; disposable PostgreSQL only."""

from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import httpx
import pytest
from integration import test_single_sol_request_pg as legacy
from nexus import worker
from nexus.agents.platform_native import credential_for_preparation
from nexus.monitoring.admission import admit_trigger, finish_increment
from nexus.monitoring.installation import install_plan_from_output
from nexus.monitoring.observations import read_latest_price
from nexus.monitoring.storage import record_check
from nexus.ops import manual_paper
from nexus.ops.manual_paper_host import dispatch_pure_research_request
from nexus.paper.manual_request import read_manual_request
from nexus.persistence.models.agents import AgentResearchOutputRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.monitoring import MonitorCheck, MonitorTrigger
from sqlalchemy import select, text
from unit.agents.test_monitoring_producer import proposal_wire


def _approved_monitor_response(body, payload):
    proposal = payload.get("monitoring_proposal")
    if payload["stage"] == "synthesis":
        assessment = body["synthesis_assessment"]
        for review in assessment["reviews"]:
            review.update(
                status="accepted",
                resolved=True,
                missing_evidence=[],
                prevented_conclusion=None,
                rationale="已核对冻结证据。",
            )
        assessment.update(
            review_conclusion="sufficient",
            uncertainties=[],
            claims=[{"text": "证据支持继续观察并复核价格。", "citations": [0]}],
        )
        body["monitoring_adoption_hash"] = proposal["content_hash"]
    else:
        for role in body["roles"]:
            role["vote"] = "support"
            if payload["stage"] == "research" and role["role"] == "market_state":
                value = proposal_wire(datetime.fromisoformat(payload["request"]["decision_time"]))
                value["condition"]["threshold"] = "100000"
                root_at = datetime.fromisoformat(payload["request"]["decision_time"])
                value["review_due_at"] = (root_at + timedelta(days=6)).isoformat()
                value["valid_until"] = (root_at + timedelta(days=7)).isoformat()
                role["monitoring_proposal"] = value
            if payload["stage"] == "critique":
                role["monitoring_review"] = dict(
                    proposal_hash=proposal["content_hash"],
                    decision="accepted",
                    rationale="核对冻结价格条件及其证据。",
                    citations=[0],
                )


async def _set_clock(factory, at):
    # Only this disposable database's existing fixture clock changes.
    async with factory.begin() as db:
        for name in ("now", "clock_timestamp"):
            await db.execute(
                text(
                    f"CREATE OR REPLACE FUNCTION fixture_clock.{name}() RETURNS timestamptz "
                    f"LANGUAGE sql {'STABLE' if name == 'now' else 'VOLATILE'} "
                    f"AS $$ SELECT '{at.isoformat()}'::timestamptz $$"
                )
            )


async def _canonical_market_scope(factory, settings, store, instrument_id):
    """Add the same real authority shapes used by normal formal priced tests.

    Everything belongs to this root's existing reviewed universe and instrument.
    No legacy fixture writer that disables triggers is called.
    """
    from integration.test_agent_paper_cycle_pg import _ObjectStore
    from nexus.agents.snapshots import prepare_research_snapshot
    from nexus.market.ingestion import ingest_price_page
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
    from nexus.persistence.models.backtest import (
        ExchangeCalendarAuthorityRecord,
        ExchangeCalendarSessionRecord,
    )
    from nexus.persistence.models.market import SectorExposure
    from nexus.persistence.models.paper import PAPER_DECISION_SOURCE_POLICY_ID
    from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage

    calendar = uuid4()
    async with factory.begin() as db:
        db.info["sec_exhibit_store"] = store
        at = await db.scalar(text("SELECT clock_timestamp()"))
        day = at.astimezone(ZoneInfo("America/New_York")).date()
        prior = day - timedelta(days=1)
        while prior.weekday() >= 5:
            prior -= timedelta(days=1)
        db.add(
            ExchangeCalendarAuthorityRecord(
                id=calendar,
                exchange="XNYS",
                version="v6-increment-canonical-fixture",
                source="fixture",
                range_start=prior,
                range_end=day + timedelta(days=8),
                query_parameters={},
                completeness_status="complete",
                content_hash=sha256(f"calendar:{prior}:{day}".encode()).hexdigest(),
                published_at=at,
                created_at=at,
            )
        )
        await db.flush()
        current = prior
        while current <= day + timedelta(days=8):
            if current.weekday() < 5:
                opening = datetime.combine(
                    current, time(9, 30), ZoneInfo("America/New_York")
                ).astimezone(UTC)
                close = opening + timedelta(hours=6, minutes=30)
                db.add(
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=calendar,
                        session=current,
                        market_open_at=opening,
                        market_close_at=close,
                        decision_at=close,
                        created_at=at,
                    )
                )
            current += timedelta(days=1)
        db.add(
            SectorExposure(
                instrument_id=instrument_id,
                sector_code="technology",
                weight=Decimal(1),
                as_of=prior,
                source_url="memory://single-monitor-canonical/sector",
                content_hash=sha256(f"sector:{instrument_id}:{prior}".encode()).hexdigest(),
                synthetic=True,
                reviewed=True,
                created_at=at,
            )
        )
        previous_close = datetime.combine(prior, time(16), ZoneInfo("America/New_York")).astimezone(
            UTC
        )
        result = await ingest_price_page(
            db,
            _ObjectStore(),
            ProviderPage(
                items=(
                    PriceBar(
                        instrument_id=instrument_id,
                        session=prior,
                        open=Decimal(100),
                        high=Decimal(100),
                        low=Decimal(100),
                        close=Decimal(100),
                        volume=Decimal(1000000),
                        observed_at=previous_close,
                        available_at=at,
                        received_at=at,
                        revision="single-monitor-canonical-prior",
                        quality_flags=(),
                    ),
                ),
                next_cursor=None,
                raw_payload=f'{{"synthetic_prior":"{instrument_id}:{prior}"}}'.encode(),
                source_request_id=None,
                as_of=at,
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
        assert result.price_authority is not None
        await _canonical_runtime_raw_price(db, instrument_id, prior, at)
        # The normal candidate reader needs its ordinary research snapshot,
        # distinct from the root's subsequent research-only source and context.
        snapshot = await prepare_research_snapshot(db, instrument_id, as_of=at)
        assert snapshot is not None and snapshot.source == "nexus_research"
        batch = await load_agent_paper_candidate_batch(
            db, batch_id=settings.agent_paper_candidate_batch_id
        )
        assert batch is not None and instrument_id in batch.instrument_ids
    await _canonical_market_tick(factory, settings, instrument_id, at, midpoint=Decimal(100))
    return calendar


async def _fresh_session_identity(factory, store, instrument_id, root, at):
    """Append new synthetic same-session SEC receipts through the normal importer.

    The old root's original identity, sources, references and hashes remain intact.
    Its completed child still reads those originals at its original decision time.
    """
    from integration.test_research_identity_context import source_archive
    from nexus.agents.snapshots import prepare_research_snapshot
    from nexus.persistence.models.data import DataSnapshot
    from nexus.research.current_identity_storage import (
        import_identity_binding,
        read_identity_bindings,
    )

    archive = root / "next-session-identity"
    source_archive(archive)
    async with factory.begin() as db:
        db.info["sec_exhibit_store"] = store
        prior = await db.scalar(
            select(DataSnapshot)
            .where(
                DataSnapshot.source == "research_current_identity",
                DataSnapshot.provider_object_id == str(instrument_id),
            )
            .order_by(DataSnapshot.created_at.desc(), DataSnapshot.id.desc())
            .limit(1)
        )
        assert prior is not None
        reference = await import_identity_binding(
            db,
            store,
            identity_snapshot_id=UUID(prior.payload["identity_snapshot_id"]),
            collection_dir=archive,
        )
        assert reference.id in await read_identity_bindings(
            db,
            instrument_id=instrument_id,
            as_of=at,
            store=store,
        )
        snapshot = await prepare_research_snapshot(db, instrument_id, as_of=at)
        assert snapshot is not None and snapshot.source == "nexus_research"


async def _append_closed_adjusted_day(factory, instrument_id, session_day, at):
    from integration.test_agent_paper_cycle_pg import _ObjectStore
    from nexus.market.ingestion import ingest_price_page
    from nexus.persistence.models.paper import PAPER_DECISION_SOURCE_POLICY_ID
    from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage

    close = datetime.combine(session_day, time(16), ZoneInfo("America/New_York")).astimezone(UTC)
    assert close < at
    async with factory.begin() as db:
        result = await ingest_price_page(
            db,
            _ObjectStore(),
            ProviderPage(
                items=(
                    PriceBar(
                        instrument_id=instrument_id,
                        session=session_day,
                        open=Decimal(100),
                        high=Decimal(100),
                        low=Decimal(100),
                        close=Decimal(100),
                        volume=Decimal(1000000),
                        observed_at=close,
                        available_at=at,
                        received_at=at,
                        revision=f"closed-prior-{session_day}",
                        quality_flags=(),
                    ),
                ),
                next_cursor=None,
                raw_payload=f'{{"closed_prior":"{instrument_id}:{session_day}"}}'.encode(),
                source_request_id=None,
                as_of=at,
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
        assert result.price_authority is not None


async def _canonical_runtime_raw_price(db, instrument_id, prior, at):
    """Normal v2 runtime price authority, separate from adjusted candidate bars.

    Same complete raw/calendar/provider binding as helpers.paper_batch. This
    fixture appends facts and uses the real price-authority finalizer.
    """
    from helpers.fosun_lineage import seed_fosun_daily_lineage
    from helpers.paper_batch import FOSUN_CAPABILITY_ID, FOSUN_CONTRACT_HASH, FOSUN_STATUS_ID
    from nexus.market.ingestion import serialize_price_bars
    from nexus.market.price_bar_authority import finalize_price_bar_set
    from nexus.persistence.models.data import DataArtifact, PriceBarRecord, PriceSeriesManifest
    from nexus.providers.base import PriceBar
    from nexus.quality.source_policy import SOURCE_POLICY_V2_ID

    manifest_id = uuid4()
    previous_close = datetime.combine(prior, time(16), ZoneInfo("America/New_York")).astimezone(UTC)
    lineage = await seed_fosun_daily_lineage(
        db,
        manifest_id=manifest_id,
        provider_authorized_at=at,
        sessions=(prior,),
        raw_payload=f'{{"synthetic_fosun_prior":"{instrument_id}:{prior}"}}'.encode(),
        received_at=at,
        session_closes={prior: previous_close},
    )
    bar = PriceBar(
        instrument_id=instrument_id,
        session=prior,
        open=Decimal(100),
        high=Decimal(100),
        low=Decimal(100),
        close=Decimal(100),
        volume=Decimal(1000000),
        observed_at=previous_close,
        available_at=at,
        received_at=at,
        revision=lineage.raw_digest,
        quality_flags=(),
    )
    serialized = serialize_price_bars((bar,))
    digest = sha256(serialized).hexdigest()
    artifact = DataArtifact(
        uri=f"memory://normalized/market/{digest}.parquet",
        sha256=digest,
        byte_size=len(serialized),
        mime_type="application/vnd.apache.parquet",
        schema_version="price_bars_v1",
        source="fosun",
        observed_at=previous_close,
        available_at=at,
        received_at=at,
        created_at=at,
    )
    db.add(artifact)
    await db.flush()
    manifest = PriceSeriesManifest(
        id=manifest_id,
        evidence_ref_id=None,
        data_artifact_id=artifact.id,
        instrument=str(instrument_id),
        source="fosun",
        feed="l1",
        delivery_mode="real_time",
        adjustment="raw",
        interval="1Day",
        start_at=previous_close,
        end_at=previous_close,
        row_count=1,
        provider_capability_id=FOSUN_CAPABILITY_ID,
        provider_status_id=FOSUN_STATUS_ID,
        provider_status_version=1,
        provider_contract_hash=FOSUN_CONTRACT_HASH,
        provider_authorized_at=at,
        gateway_transport_artifact_id=lineage.raw_artifact.id,
        daily_calendar_authority_id=lineage.calendar_authority_id,
        gateway_request_start_at=lineage.request_start_at,
        gateway_request_end_at=lineage.request_end_at,
        created_at=at,
    )
    db.add(manifest)
    await db.flush()
    db.add(
        PriceBarRecord(
            price_series_manifest_id=manifest.id,
            instrument_id=instrument_id,
            session=prior,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            observed_at=bar.observed_at,
            available_at=bar.available_at,
            received_at=bar.received_at,
            revision=bar.revision,
            quality_flags=[],
            created_at=at,
        )
    )
    await db.flush()
    finalized = await finalize_price_bar_set(db, manifest)
    assert finalized is not None and finalized[0].source_policy_id == SOURCE_POLICY_V2_ID


async def _canonical_market_tick(factory, settings, instrument_id, at, *, midpoint):
    """Append canonical Schwab quote/raw linkage and exact three-query scope."""
    from nexus.market.intraday_ingestion import canonical_quote_observation
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
    from nexus.persistence.models.data import DataArtifact
    from nexus.persistence.models.market import LatestQuote
    from nexus.persistence.models.paper import (
        PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
        RiskQueryAuthorityRecord,
    )
    from nexus.risk.authority import canonical_query_authority_hash

    async with factory.begin() as db:
        batch = await load_agent_paper_candidate_batch(
            db, batch_id=settings.agent_paper_candidate_batch_id
        )
        raw = f'{{"synthetic_quote":"{instrument_id}:{at.isoformat()}:{midpoint}"}}'.encode()
        digest = sha256(raw).hexdigest()
        artifact = DataArtifact(
            uri=f"memory://raw/market/schwab/stream/{digest}.json",
            sha256=digest,
            byte_size=len(raw),
            mime_type="application/json",
            schema_version="schwab_level_one_stream_v1",
            source="schwab",
            observed_at=at,
            available_at=at,
            received_at=at,
            created_at=at,
        )
        db.add(artifact)
        await db.flush()
        canonical = canonical_quote_observation(
            instrument_id=instrument_id,
            source="schwab",
            feed="l1",
            delivery_mode="real_time",
            event_kind="quote",
            bid=midpoint,
            ask=midpoint,
            last=None,
            observed_at=at,
            available_at=at,
            received_at=at,
            revision=digest,
        )
        db.add(
            LatestQuote(
                instrument_id=instrument_id,
                source="schwab",
                feed="l1",
                delivery_mode="real_time",
                event_kind="quote",
                bid=midpoint,
                ask=midpoint,
                last=None,
                observed_at=at,
                available_at=at,
                received_at=at,
                revision=digest,
                quality_flags=[],
                content_hash=sha256(canonical).hexdigest(),
                evidence_status="verified",
                data_artifact_id=artifact.id,
                created_at=at,
            )
        )
        day = at.astimezone(ZoneInfo("America/New_York")).date()
        for kind in ("corporate_action", "suspension", "delisting"):
            fields = dict(
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                universe_snapshot_id=batch.universe_snapshot_id,
                decision_session=day,
                kind=kind,
                required_instrument_ids=[instrument_id],
                blocked_instrument_ids=[],
                complete=True,
                source_watermark_hash=sha256(f"{kind}:{instrument_id}:{at}".encode()).hexdigest(),
                available_at=at,
            )
            db.add(
                RiskQueryAuthorityRecord(
                    **fields,
                    content_hash=canonical_query_authority_hash(**fields),
                    created_at=at,
                )
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown", [False, True], ids=["known", "unknown"])
async def test_v6_root_one_price_increment_is_permanent(
    migrated_database_url, tmp_path, monkeypatch, unknown
):
    factory, settings, store, ids, calendar, _ = await legacy.single_scope(
        migrated_database_url, tmp_path, monkeypatch
    )
    assert settings.agent_daily_budget_usd == Decimal("13")
    calendar = await _canonical_market_scope(factory, settings, store, ids[0])

    async def call(*args):
        return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

    try:
        offer = await call("inspect-single-monitor-sol-intent", "--instrument-id", str(ids[0]))
        receipt = await call(
            "start-single-monitor-sol",
            "--instrument-id",
            str(ids[0]),
            "--idempotency-key",
            "increment-once-root",
            "--expected-intent-hash",
            offer.intent_hash,
        )
        # Capture the actual root decision time from the normal request's stage
        # payload rather than inventing or modifying any persisted source.
        root_time = []

        def reviewed(body, payload):
            if payload["stage"] == "research":
                root_time.append(payload.get("decision_time"))
            _approved_monitor_response(body, payload)

        sent, _ = legacy.install_sol_http(monkeypatch, store, model_response=reviewed)
        assert await dispatch_pure_research_request(
            settings, request_id=receipt.request_id, calendar_authority_id=calendar
        )
        assert sent == ["research", "critique", "synthesis"]
        async with factory() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            view = await read_manual_request(db, row)
            assert view.status == "completed"
            parent = view.research.members[0]
            output = await db.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == parent.run_id,
                    AgentResearchOutputRecord.role.is_(None),
                    AgentResearchOutputRecord.status == "completed",
                )
            )
            at = await db.scalar(text("SELECT clock_timestamp()"))
        await _canonical_market_tick(factory, settings, ids[0], at, midpoint=Decimal("100"))
        async with factory.begin() as db:
            plan = await install_plan_from_output(
                db,
                account_id=settings.agent_paper_account_id,
                output_id=output.id,
                expected_predecessor_id=None,
                now=at,
            )
        opening = datetime.combine(at.date(), time(10, 0), ZoneInfo("America/New_York")).astimezone(
            UTC
        )
        clock = [max(opening, at + timedelta(seconds=1))]

        class FixtureTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return clock[0].astimezone(tz)

        import importlib

        for module in (
            "nexus.worker",
            "nexus.agents.snapshots",
            "nexus.research.current_identity_archive",
            "integration.test_research_identity_context",
            "nexus.ops.instrument_identity_local",
            "helpers.current_identity",
        ):
            monkeypatch.setattr(importlib.import_module(module), "datetime", FixtureTime)
        await _set_clock(factory, clock[0])
        await _canonical_market_tick(factory, settings, ids[0], clock[0], midpoint=Decimal("100"))
        async with factory.begin() as db:
            await record_check(
                db,
                plan_id=plan.id,
                scheduled_at=clock[0],
                checked_at=clock[0],
                observation=await read_latest_price(db, instrument_id=ids[0], now=clock[0]),
            )
            first_trigger = (
                await db.scalars(
                    select(MonitorTrigger).where(
                        MonitorTrigger.check_id.in_(
                            select(MonitorCheck.id).where(MonitorCheck.plan_id == plan.id)
                        )
                    )
                )
            ).one()
            first = await admit_trigger(
                db,
                account_id=settings.agent_paper_account_id,
                trigger_id=first_trigger.id,
                candidate_batch_id=settings.agent_paper_candidate_batch_id,
                calendar_authority_id=calendar,
                model_config_id=settings.agent_paper_formal_model_config_id,
                daily_limit_usd=settings.agent_daily_budget_usd,
                daily_sdk_call_limit=settings.agent_monitor_daily_sdk_call_limit,
                platform_credential=credential_for_preparation(settings),
                now=clock[0],
            )
        requests, _ = legacy.install_sol_http(
            monkeypatch, store, model_response=_approved_monitor_response
        )
        if unknown:
            from nexus.agents import platform_native

            def timeout(request):
                requests.append("research")
                raise httpx.ReadTimeout("isolated increment unknown", request=request)

            monkeypatch.setattr(
                platform_native,
                "native_http_client",
                lambda: httpx.AsyncClient(transport=httpx.MockTransport(timeout)),
            )
        assert await worker.run_agent_worker_once(
            settings, job_ids=(first.job_id,), formal_stage_capacity=True
        )
        clock[0] += timedelta(minutes=2)
        await _set_clock(factory, clock[0])
        async with factory.begin() as db:
            done = await finish_increment(
                db,
                account_id=settings.agent_paper_account_id,
                trigger_id=first_trigger.id,
                now=clock[0],
            )
            assert done.status == ("unknown" if unknown else "completed")
            claim = (
                await db.execute(
                    text("SELECT root_request_id,run_id FROM single_monitor_increment_claims")
                )
            ).one()
            assert claim == (receipt.request_id, first.run_id)
            if unknown:
                reservation = (
                    await db.execute(
                        text(
                            "SELECT state,reserved_usd,settled_usd "
                            "FROM agent_daily_budget_reservations WHERE run_id=:r"
                        ),
                        {"r": first.run_id},
                    )
                ).one()
                assert reservation.state == "reserved" and reservation.settled_usd is None
                assert reservation.reserved_usd > 0
                assert await db.scalar(
                    text("SELECT settled_cost_usd IS NULL FROM agent_research_runs WHERE id=:r"),
                    {"r": first.run_id},
                )

            # Recursive resolver also handles a real child; no source relabeling.
            assert (
                await db.scalar(
                    text("SELECT single_monitor_root_request(:run)"), {"run": first.run_id}
                )
                == receipt.request_id
            )
        if not unknown:
            # The same fully reviewed child output installs a genuine successor;
            # the resolver must retain the original v6 root through that plan.
            await _canonical_market_tick(
                factory, settings, ids[0], clock[0], midpoint=Decimal("100")
            )
            async with factory.begin() as db:
                child_output = await db.scalar(
                    select(AgentResearchOutputRecord).where(
                        AgentResearchOutputRecord.run_id == first.run_id,
                        AgentResearchOutputRecord.role.is_(None),
                        AgentResearchOutputRecord.status == "completed",
                    )
                )
                successor = await install_plan_from_output(
                    db,
                    account_id=settings.agent_paper_account_id,
                    output_id=child_output.id,
                    expected_predecessor_id=plan.id,
                    now=clock[0],
                )
                assert successor.id != plan.id
                assert successor.source_run_id == first.run_id
                assert (
                    await db.scalar(
                        text("SELECT single_monitor_root_request(:run)"),
                        {"run": successor.source_run_id},
                    )
                    == receipt.request_id
                )
            plan = successor
        # Move to the next real trading day, so a second attempt has its own
        # ordinary 13 USD budget instead of the first day's protected credit.
        previous_session = clock[0].astimezone(ZoneInfo("America/New_York")).date()
        next_session = previous_session + timedelta(days=1)
        while next_session.weekday() >= 5:
            next_session += timedelta(days=1)
        clock[0] = datetime.combine(
            next_session, time(10), ZoneInfo("America/New_York")
        ).astimezone(UTC)
        await _set_clock(factory, clock[0])
        await _append_closed_adjusted_day(factory, ids[0], previous_session, clock[0])
        await _fresh_session_identity(factory, store, ids[0], tmp_path, clock[0])
        # A new false->true episode creates a distinct, otherwise valid hit.
        for midpoint in (Decimal("200000"), Decimal("100")):
            clock[0] += timedelta(minutes=5)
            await _set_clock(factory, clock[0])
            await _canonical_market_tick(factory, settings, ids[0], clock[0], midpoint=midpoint)
            async with factory.begin() as db:
                await record_check(
                    db,
                    plan_id=plan.id,
                    scheduled_at=clock[0],
                    checked_at=clock[0],
                    observation=await read_latest_price(db, instrument_id=ids[0], now=clock[0]),
                )
        async with factory() as db:
            triggers = (
                await db.scalars(
                    select(MonitorTrigger)
                    .where(
                        MonitorTrigger.check_id.in_(
                            select(MonitorCheck.id).where(MonitorCheck.plan_id == plan.id)
                        )
                    )
                    .order_by(MonitorTrigger.created_at, MonitorTrigger.id)
                )
            ).all()
            assert len(triggers) == (2 if unknown else 1)
            second = next(t for t in triggers if t.id != first_trigger.id)
        with pytest.raises(
            ValueError,
            match=(
                "MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"
                if unknown
                else "SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED"
            ),
        ):
            async with factory.begin() as db:
                await admit_trigger(
                    db,
                    account_id=settings.agent_paper_account_id,
                    trigger_id=second.id,
                    candidate_batch_id=settings.agent_paper_candidate_batch_id,
                    calendar_authority_id=calendar,
                    model_config_id=settings.agent_paper_formal_model_config_id,
                    daily_limit_usd=settings.agent_daily_budget_usd,
                    daily_sdk_call_limit=settings.agent_monitor_daily_sdk_call_limit,
                    platform_credential=credential_for_preparation(settings),
                    now=clock[0],
                )
        if not unknown:
            # Exercise the authoritative database fence with the Python preview
            # deliberately bypassed. Every source, plan, market and budget guard
            # stays intact; a failure from any other layer fails this test.
            from nexus.monitoring import single_monitor_limit
            from sqlalchemy.exc import DBAPIError

            async def preview_bypassed(*args, **kwargs):
                return None

            async def counts():
                async with factory() as db:
                    return tuple(
                        [
                            await db.scalar(text(f"SELECT count(*) FROM {table}"))
                            for table in (
                                "agent_research_runs",
                                "jobs",
                                "agent_daily_budget_reservations",
                                "research_monitor_increments",
                                "single_monitor_increment_claims",
                            )
                        ]
                    )

            before = await counts()
            with monkeypatch.context() as bypass:
                bypass.setattr(
                    single_monitor_limit,
                    "require_single_monitor_increment_unused",
                    preview_bypassed,
                )
                with pytest.raises(DBAPIError, match="SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED"):
                    async with factory.begin() as db:
                        await admit_trigger(
                            db,
                            account_id=settings.agent_paper_account_id,
                            trigger_id=second.id,
                            candidate_batch_id=settings.agent_paper_candidate_batch_id,
                            calendar_authority_id=calendar,
                            model_config_id=settings.agent_paper_formal_model_config_id,
                            daily_limit_usd=settings.agent_daily_budget_usd,
                            daily_sdk_call_limit=settings.agent_monitor_daily_sdk_call_limit,
                            platform_credential=credential_for_preparation(settings),
                            now=clock[0],
                        )
            assert await counts() == before
        async with factory() as db:
            assert (
                await db.scalar(
                    text("SELECT single_monitor_root_request(:run)"), {"run": first.run_id}
                )
                == receipt.request_id
            )
            assert (
                await db.scalar(text("SELECT count(*) FROM single_monitor_increment_claims")) == 1
            )
        assert requests == (["research"] if unknown else ["research", "critique", "synthesis"])
    finally:
        await factory.kw["bind"].dispose()
