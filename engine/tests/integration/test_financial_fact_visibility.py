"""Late SEC fact inserts cannot change the facts visible to an older research run."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.agents.tools import ToolInputError
from nexus.ops.openai_local import prepare_database
from nexus.ops.sec_local import refresh_sec_research
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import FinancialFactRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select

from .test_sec_local import (
    INSTRUMENT,
    InMemoryRawEvidenceStore,
    sec_responder,
    seed_instrument,
)


@pytest.mark.asyncio
async def test_normal_sec_late_fact_is_excluded_from_old_exact_tools_and_http(
    migrated_database_url,
):
    # Only source transport and raw object storage are synthetic. The producer,
    # preparation, admission, frozen authority, tool and HTTP readers are real.
    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    await seed_instrument(settings)
    requests = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(sec_responder(requests))) as client:
        prepared = await refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=InMemoryRawEvidenceStore(),
        )
    assert prepared["status"] == "RESEARCH_INPUTS_PREPARED"
    assert len(requests) == 3
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        old = await session.get(DataSnapshot, UUID(prepared["data_snapshot_id"]))
        cutoff = datetime.fromisoformat(old.payload["as_of"])
        original_hash, original_payload = old.revision, dict(old.payload)
        baseline = await session.scalar(
            select(FinancialFactRecord).where(FinancialFactRecord.canonical_fact == "revenue")
        )
        assert baseline.created_at <= cutoff and baseline.accepted_at <= cutoff
        base_value = baseline.value
        old_run = await _admit(session, old, "old")
        old_run_id = old_run.id
        baseline_columns = {
            column.key: getattr(baseline, column.key)
            for column in FinancialFactRecord.__table__.columns
            if column.key not in {"id", "created_at"}
        }

    # Append records, never alter an old fact/ref/filing or backfill its clock.
    # Equality proves the cutoff is inclusive; the late row shares the exact
    # original filing/ref and accepted_at but was unavailable at the old cutoff.
    late_time = datetime.now(UTC)
    assert late_time > cutoff
    async with factory() as session, session.begin():
        for offset, value, created in (
            (1, Decimal("123.25"), cutoff),
            (2, Decimal("456.75"), late_time),
        ):
            session.add(
                FinancialFactRecord(
                    **{
                        **baseline_columns,
                        "period_end": baseline_columns["period_end"] - timedelta(days=offset),
                        "value": value,
                    },
                    created_at=created,
                )
            )

    async with factory() as session, session.begin():
        old = await session.get(DataSnapshot, old.id)
        assert old.revision == original_hash and old.payload == original_payload
        new = await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC))
        assert new is not None and new.id != old.id
        new_run = await _admit(session, new, "new")
        new_run_id = new_run.id

    from nexus.persistence.models.agents import AgentResearchRunRecord

    async with factory() as session:
        for run_id, expected in (
            (old_run_id, {base_value, Decimal("123.25")}),
            (new_run_id, {base_value, Decimal("123.25"), Decimal("456.75")}),
        ):
            run = await session.get(AgentResearchRunRecord, run_id)
            tools = await database_read_only_tools(session, run, allow_fixture_evidence=False)
            payload = {
                "instrument_id": str(INSTRUMENT),
                "data_snapshot_id": str(run.data_snapshot_id),
                "as_of": run.decision_time.isoformat(),
                "fields": ["revenue"],
            }
            with pytest.raises(ToolInputError, match="exceeds frozen"):
                await tools.execute(
                    "get_financial_facts", {**payload, "instrument_id": str(UUID(int=1))}
                )
            result = await tools.execute("get_financial_facts", payload)
            assert {Decimal(row["value"]) for row in result["items"]} == expected
            assert {row["evidence_ref_id"] for row in result["items"]} == {
                str(baseline_columns["evidence_ref_id"])
            }

    # Authenticated exact-run GET must keep the same cutoff, not select latest.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
        base_url="http://testserver",
    ) as client:
        for run_id, expected_count in ((old_run_id, 12), (new_run_id, 13)):
            path = f"/internal/v1/research-runs/{run_id}"
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200
            inputs = response.json()["data"]["inputs"]
            assert inputs["total_facts"] == len(inputs["facts"]) == expected_count
            values = {Decimal(row["value"]) for row in inputs["facts"]}
            assert Decimal("123.25") in values
            assert (Decimal("456.75") in values) is (run_id == new_run_id)


async def _admit(session, snapshot, key):
    cutoff = datetime.fromisoformat(snapshot.payload["as_of"])
    admission = await admit_research_run(
        session,
        actor_id="test",
        route="/internal/v1/research-runs",
        idempotency_key=f"financial-pit-{key}",
        request_hash="0" * 64,
        request=ResearchRequest(
            instrument_id=INSTRUMENT, data_snapshot_id=snapshot.id, decision_time=cutoff
        ),
        prompt_version_id=None,
        model_config_id=None,
        daily_limit_usd=Decimal("3"),
        now=cutoff,
    )
    return admission.run
