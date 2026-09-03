"""Synthetic NAV remains current issuer research, never market price authority."""

import hashlib
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


@pytest.mark.asyncio
@pytest.mark.parametrize("omitted_original", [None, "identity", "workbook"])
async def test_nav_reaches_only_frozen_tool_with_bounded_rows_and_original_times(
    migrated_database_url,
    omitted_original,
):
    from nexus.research.ssga_nav_history import IssuerNavHistory, NavObservation
    from nexus.research.ssga_nav_storage import persist_nav_history, read_nav_histories

    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    receipt, decision = NOW + timedelta(minutes=1), NOW + timedelta(minutes=2)
    html, workbook = b"synthetic identity original", b"synthetic nav original"
    document = IssuerNavHistory(
        instrument_id=INSTRUMENT,
        symbol="TEST",
        isin="US0000000001",
        observations=tuple(
            NavObservation(
                date=date(2026, 7, 1) + timedelta(days=n),
                nav=Decimal("10.125"),
                shares_outstanding=None,
                total_net_assets=None,
            )
            for n in range(45)
        ),
        html_url="https://www.ssga.com/us/en/individual/etfs/synthetic-test",
        workbook_url="https://www.ssga.com/library-content/products/fund-data/etfs/us/nav-test.xlsx",
        html_content_hash=hashlib.sha256(html).hexdigest(),
        workbook_content_hash=hashlib.sha256(workbook).hexdigest(),
        html_byte_length=len(html),
        workbook_byte_length=len(workbook),
        html_received_at=NOW,
        workbook_received_at=receipt,
        available_at=receipt,
    )
    async with session_factory(settings)() as session, session.begin():
        sec = await seed_filing(session)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        old_payload = dict(old.payload)
        for raw, received in ((html, NOW - timedelta(days=1)), (workbook, receipt)):
            if omitted_original == ("identity" if raw == html else "workbook"):
                continue
            digest = hashlib.sha256(raw).hexdigest()
            session.add(
                DataArtifact(
                    uri="memory://sha256/" + digest,
                    sha256=digest,
                    byte_size=len(raw),
                    mime_type="application/octet-stream",
                    schema_version="issuer_raw_v1",
                    source="ssga",
                    observed_at=received,
                    available_at=received,
                    received_at=received,
                )
            )
        await session.flush()
        ref = await persist_nav_history(
            session,
            document,
            location="memory://sha256/" + hashlib.sha256(document.canonical_bytes()).hexdigest(),
        )
        assert (await persist_nav_history(session, document, location=ref.location)).id == ref.id
        assert not await read_nav_histories(session, instrument_id=INSTRUMENT, as_of=NOW)
        assert not await read_nav_histories(
            session, instrument_id=INSTRUMENT, as_of=decision, evidence_ids=()
        )
        assert not await read_nav_histories(session, instrument_id=UUID(int=99), as_of=decision)
        if omitted_original is not None:
            from sqlalchemy.exc import IntegrityError

            with pytest.raises(IntegrityError):
                await prepare_research_snapshot(session, INSTRUMENT, as_of=decision)
            await session.rollback()
            return
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=decision)
        assert str(ref.id) in frozen.payload["evidence_ids"]
        assert str(sec.id) in frozen.payload["evidence_ids"]
        assert old.payload == old_payload and str(ref.id) not in old.payload["evidence_ids"]
        admitted = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="nav-history",
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT, data_snapshot_id=frozen.id, decision_time=decision
            ),
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=Decimal("3"),
            now=decision,
        )
        tools = await database_read_only_tools(session, admitted.run, allow_fixture_evidence=False)
        result = await tools.execute(
            "get_security_snapshot",
            {
                "instrument_id": str(INSTRUMENT),
                "data_snapshot_id": str(frozen.id),
                "as_of": decision.isoformat(),
            },
        )
        values = [
            item["issuer_nav_history"] for item in result["items"] if "issuer_nav_history" in item
        ]
        assert len(values) == 1
        value = values[0]
        assert value["total_observations"] == 45 and value["truncated"] is True
        assert len(value["observations"]) == 32
        assert value["coverage_start"] == "2026-07-01" and value["coverage_end"] == "2026-08-14"
        assert value["observations"][0]["date"] == "2026-07-14"
        assert value["observations"][-1]["nav"] == "10.125"
        assert value["observations"][-1]["shares_outstanding"] is None
        assert value["historical_backtest_eligible"] is False
        assert value["market_price_eligible"] is False and value["total_return_eligible"] is False
        assert value["source_published_at"] is None
        assert value["currency_basis"] == "issuer_page_base_currency"
        assert value["available_at"] == receipt.isoformat().replace("+00:00", "Z")
    import asyncio
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    with pytest.raises(RuntimeError, match="cannot remove ownership"):
        await asyncio.to_thread(command.downgrade, config, "0038_issuer_fund_observations")
