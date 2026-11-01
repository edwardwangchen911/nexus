"""Real native claims/observations/terminal readers in isolated PG; no model calls."""

import hashlib
import json
from datetime import timedelta
from decimal import Decimal

import pytest
from integration.test_platform_funding_pg import funding_fixture
from integration.test_platform_producer_pg import setup
from nexus.agents.comparison_claim import claim_comparison_position
from nexus.agents.comparison_inputs import PreparedComparisonInput
from nexus.agents.comparison_producer import admit_comparison_position
from nexus.agents.comparison_registration import register_comparison
from nexus.agents.platform_schema import (
    CURRENT_PLATFORM_UNITS_VERSION,
    LEGACY_PLATFORM_UNITS_VERSION,
)
from nexus.agents.platform_usage import (
    MEDIUM_PLATFORM_PRICE_VERSION,
    PLATFORM_PRICE_VERSION,
    PLATFORM_PRICE_VERSIONS,
)
from nexus.agents.platform_wire import compile_platform_stage
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import StagedProtocol, stage_evidence
from nexus.agents.research_usage import research_reserved_occupancy
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from unit.agents.test_platform_usage import usage


async def native_platform(factory, monkeypatch, tmp_path, *, price_version=PLATFORM_PRICE_VERSION):
    source, prepared, manifest, model_id, old, protected = await setup(
        factory,
        monkeypatch,
        price_version=price_version,
        units_version=CURRENT_PLATFORM_UNITS_VERSION
        if price_version == MEDIUM_PLATFORM_PRICE_VERSION
        else LEGACY_PLATFORM_UNITS_VERSION,
    )
    manifest, funding = await funding_fixture(
        factory,
        manifest,
        tmp_path,
        monkeypatch,
        explicit_spend=manifest.purpose == "platform-qualification-v1",
    )
    async with factory.begin() as s:
        if manifest.purpose == "platform-qualification-v1":
            # Stable rounds require registration before admission; unlike the
            # historical purposes, admission must never create their identity.
            await register_comparison(s, manifest, allow_new_round=True)
        result = await admit_comparison_position(
            s,
            source=source,
            prepared=prepared,
            manifest=manifest,
            ordinal=1,
            model_config_id=model_id,
            daily_limit_usd=Decimal(20),
            platform_funding=funding,
        )
    with pytest.raises(ValueError, match="PLATFORM_FUNDING_REQUIRED"):
        await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="platform-fixture"
        )
    claim = await claim_comparison_position(
        factory,
        experiment_id=manifest.experiment_id,
        ordinal=1,
        worker_id="platform-fixture",
        platform_funding_loader=lambda: funding,
    )
    now = claim.expires_at - timedelta(seconds=330)
    protocol = StagedProtocol(factory)
    await protocol.install(
        scope_id=manifest.experiment_id,
        claims=(claim,),
        now=now,
        comparison=True,
        work_deadline=now + timedelta(seconds=300),
        cleanup_deadline=now + timedelta(seconds=307),
    )
    m = prepared["members"][0]
    member = PreparedComparisonInput(
        payload=m["payload"], proof=m["proof"], forbidden=tuple(m["forbidden"])
    )
    compiled = compile_platform_stage(
        member,
        allocation=manifest.allocations[0],
        expected_allocation_hash=manifest.allocations[0].allocation_hash,
        stage="research",
        previous=[],
        prior_usage=[],
    )
    return result, manifest, claim, protocol, now, compiled, old, protected


@pytest.mark.asyncio
@pytest.mark.parametrize("orphan", [False, True])
@pytest.mark.parametrize("classification", ["complete", "missing", "unverified"])
@pytest.mark.parametrize("price_version", PLATFORM_PRICE_VERSIONS)
async def test_one_native_cost_path_for_normal_orphan_proof_and_occupancy(
    migrated_database_url, monkeypatch, tmp_path, orphan, classification, price_version
):
    f = session_factory(EngineSettings(database_url=migrated_database_url))
    result, m, claim, p, now, compiled, old, protected = await native_platform(
        f, monkeypatch, tmp_path, price_version=price_version
    )
    try:
        with pytest.raises(ValueError, match="PLATFORM_AUTHORIZATION_BINDING_INVALID"):
            await p.authorize(claim, stage=1, now=now, wire=compiled.wire)
        ticket = await p.authorize(
            claim, stage=1, now=now, wire=compiled.wire, platform_wire=compiled
        )
        raw = {
            "model": "gpt-5.6-terra",
            "service_tier": "default",
            "status": "completed",
            "usage": usage(),
        }
        if classification == "missing":
            del raw["usage"]["input_tokens_details"]["cache_write_tokens"]
        payload = json.dumps(raw).encode()
        await p.observe(ticket, now=now, payload=payload, tokens=(12000, 1000))
        if classification != "unverified":
            assert await p.verify(
                ticket,
                now=now,
                wire_hash=hashlib.sha256(compiled.wire).hexdigest(),
                payload=payload,
                tokens=(12000, 1000),
            )
        await p.cleanup(ticket, now=now, confirmed=True)
        async with f() as s:
            rows = await stage_evidence(s, result.run_id)
            assert rows[0]["platform_response"] == payload.decode()
            assert rows[0]["platform_binding"]["price_version"] == price_version
            assert (
                rows[0]["platform_binding"]["allocation_hash"] == m.allocations[0].allocation_hash
            )
            held = await s.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == result.run_id
                )
            )
            assert await research_reserved_occupancy(s, held) == m.reservation_for(1)
        end = claim.expires_at + timedelta(seconds=1) if orphan else now + timedelta(seconds=1)
        assert await p.close(claim, now=end, orphan=orphan) is (classification == "complete")
        async with f() as s:
            proof = await read_terminal_proof(s, run_id=result.run_id, now=end)
            assert proof.known is (classification == "complete")
            assert proof.budget_settled is (classification == "complete")
            held = await s.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == result.run_id
                )
            )
            run = await s.get(AgentResearchRunRecord, result.run_id)
            assert held.settled_usd == (Decimal("0.0323") if classification == "complete" else None)
            assert run.settled_cost_usd == held.settled_usd
            assert (
                await s.scalar(
                    text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
                    {"id": old},
                )
                == protected
            )
            if classification != "complete":
                assert await research_reserved_occupancy(s, held) == m.reservation_for(1)
        async with f.begin() as s:
            with pytest.raises(DBAPIError):
                async with s.begin_nested():
                    await s.execute(
                        text(
                            "UPDATE research_stage_observation SET platform_response='{}' "
                            "WHERE run_id=:id"
                        ),
                        {"id": result.run_id},
                    )
    finally:
        await f.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_late_raw_evidence_cannot_change_frozen_unknown_cost_or_membership(
    migrated_database_url, monkeypatch, tmp_path
):
    f = session_factory(EngineSettings(database_url=migrated_database_url))
    result, m, claim, p, now, compiled, _, _ = await native_platform(f, monkeypatch, tmp_path)
    try:
        ticket = await p.authorize(
            claim, stage=1, now=now, wire=compiled.wire, platform_wire=compiled
        )
        await p.cleanup(ticket, now=now, confirmed=True)
        assert await p.close(claim, now=now + timedelta(seconds=1)) is False
        async with f() as s:
            original = await read_terminal_proof(
                s, run_id=result.run_id, now=now + timedelta(seconds=1)
            )
        payload = json.dumps(
            {
                "model": "gpt-5.6-terra",
                "service_tier": "default",
                "status": "completed",
                "usage": usage(),
            }
        ).encode()
        await p.observe(
            ticket, now=now + timedelta(seconds=2), payload=payload, tokens=(12000, 1000)
        )
        async with f() as s:
            assert (
                await read_terminal_proof(s, run_id=result.run_id, now=now + timedelta(seconds=3))
                == original
            )
            assert (
                await s.scalar(
                    text(
                        "SELECT platform_response FROM research_stage_observation WHERE run_id=:id"
                    ),
                    {"id": result.run_id},
                )
                == payload.decode()
            )
            held = await s.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == result.run_id
                )
            )
            assert held.state == "reserved" and held.settled_usd is None
            assert await research_reserved_occupancy(s, held) == m.reservation_for(1)
    finally:
        await f.kw["bind"].dispose()
