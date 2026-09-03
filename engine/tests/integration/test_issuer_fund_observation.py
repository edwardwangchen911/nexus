"""Synthetic official observations reach only their frozen research scope."""

import hashlib
from datetime import timedelta
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
from nexus.research.ssga_fund_observations import (
    parse_fund_observation,
    persist_fund_observation,
    read_fund_observations,
)
from nexus.settings import EngineSettings

from engine.tests.unit.research.test_ssga_fund_observations import originals


@pytest.mark.asyncio
@pytest.mark.parametrize("reused_identity", [False, True])
async def test_new_observation_reaches_frozen_tool_preserving_old_scope(
    migrated_database_url, reused_identity
):
    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    receipt = NOW + timedelta(minutes=1)
    decision = NOW + timedelta(minutes=2)
    html, workbook = originals()
    doc = parse_fund_observation(
        html=html,
        workbook=workbook,
        instrument_id=INSTRUMENT,
        symbol="TEST",
        html_url="https://www.ssga.com/us/en/individual/etfs/synthetic-test",
        workbook_url="https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-test.xlsx",
        html_received_at=receipt,
        workbook_received_at=receipt,
    )
    async with session_factory(settings)() as s, s.begin():
        sec = await seed_filing(s)
        old = await prepare_research_snapshot(s, INSTRUMENT, as_of=NOW)
        old_payload = dict(old.payload)
        for raw in (html, workbook):
            digest = hashlib.sha256(raw).hexdigest()
            s.add(
                DataArtifact(
                    uri="memory://sha256/" + digest,
                    sha256=digest,
                    byte_size=len(raw),
                    mime_type="application/octet-stream",
                    schema_version="issuer_raw_v1",
                    source="ssga",
                    observed_at=NOW if reused_identity and raw == html else receipt,
                    received_at=NOW if reused_identity and raw == html else receipt,
                    available_at=NOW if reused_identity and raw == html else receipt,
                )
            )
        await s.flush()
        if reused_identity:
            from nexus.research.ssga_distributions import parse_distribution, persist_distribution

            from engine.tests.unit.research.test_ssga_distributions import (
                workbook as distribution_workbook,
            )

            original = distribution_workbook()
            digest = hashlib.sha256(original).hexdigest()
            s.add(
                DataArtifact(
                    uri="memory://sha256/" + digest,
                    sha256=digest,
                    byte_size=len(original),
                    mime_type="application/octet-stream",
                    schema_version="issuer_raw_v1",
                    source="ssga",
                    observed_at=NOW,
                    received_at=NOW,
                    available_at=NOW,
                )
            )
            await s.flush()
            distribution = parse_distribution(
                raw=original,
                identity_html=html,
                instrument_id=INSTRUMENT,
                symbol="TEST",
                identity_url=doc.html_url,
                received_at=NOW,
            )
            await persist_distribution(
                s,
                distribution,
                location="memory://sha256/"
                + hashlib.sha256(distribution.canonical_bytes()).hexdigest(),
            )
        ref = await persist_fund_observation(
            s, doc, location="memory://sha256/" + hashlib.sha256(doc.canonical_bytes()).hexdigest()
        )
        replay = await persist_fund_observation(s, doc, location=ref.location)
        assert replay.id == ref.id
        assert not await read_fund_observations(s, instrument_id=INSTRUMENT, as_of=NOW)
        assert not await read_fund_observations(
            s, instrument_id=INSTRUMENT, as_of=decision, evidence_ids=()
        )
        assert not await read_fund_observations(s, instrument_id=UUID(int=99), as_of=decision)
        frozen = await prepare_research_snapshot(s, INSTRUMENT, as_of=decision)
        assert str(ref.id) in frozen.payload["evidence_ids"]
        assert str(sec.id) in frozen.payload["evidence_ids"]
        assert old.payload == old_payload and str(ref.id) not in old.payload["evidence_ids"]
        admitted = await admit_research_run(
            s,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="fund-observation",
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT, data_snapshot_id=frozen.id, decision_time=decision
            ),
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=Decimal("3"),
            now=decision,
        )
        tools = await database_read_only_tools(s, admitted.run, allow_fixture_evidence=False)
        result = await tools.execute(
            "get_security_snapshot",
            {
                "instrument_id": str(INSTRUMENT),
                "data_snapshot_id": str(frozen.id),
                "as_of": decision.isoformat(),
            },
        )
        observations = [
            x["issuer_fund_observation"] for x in result["items"] if "issuer_fund_observation" in x
        ]
        if reused_identity:
            assert any("issuer_distribution" in x for x in result["items"])
        assert len(observations) == 1
        assert observations[0]["holdings"][0]["weight_percent"] == "-0.125"
        assert observations[0]["denominator"] is None
        assert observations[0]["sector_risk_eligible"] is False

    import asyncio
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    with pytest.raises(RuntimeError, match="cannot remove ownership"):
        await asyncio.to_thread(command.downgrade, config, "0037_strategy_rank_checkpoints")


def test_empty_issuer_ownership_migration_roundtrip(migrated_database_url):
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD

    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    command.downgrade(config, "0037_strategy_rank_checkpoints")
    command.upgrade(config, CURRENT_MIGRATION_HEAD)
