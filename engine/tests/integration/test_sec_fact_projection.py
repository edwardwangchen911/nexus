"""Repair missing SEC projections through the normal local entry without rewriting research."""

from datetime import UTC, datetime
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops import sec_local
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import Filing, FinancialFactRecord
from nexus.persistence.session import session_factory
from nexus.providers import sec
from nexus.settings import EngineSettings
from sqlalchemy import select

from .test_financial_fact_visibility import (
    _admit,
    synthetic_financial_context_model,  # noqa: F401
)
from .test_sec_local import INSTRUMENT, InMemoryRawEvidenceStore, sec_responder, seed_instrument

TAG = "RevenueFromContractWithCustomerExcludingAssessedTax"


class ProjectionStore(InMemoryRawEvidenceStore):
    async def read_evidence(self, key, *, uri):
        assert uri.endswith(key.rsplit("/", 1)[-1])
        return self.objects[key]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["hash", "length", "equal_basis", "different_basis", "missing_owner"]
)
async def test_saved_sec_projection_rejects_damage_without_any_fact_changes(
    migrated_database_url,
    monkeypatch,
    damage,
):
    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    requests = []
    respond = sec_responder(requests)

    def source(request):
        response = respond(request)
        if "companyfacts" not in request.url.path:
            return response
        body = response.json()
        tags = body["facts"]["us-gaap"]
        if damage in {"equal_basis", "different_basis"}:
            tags[TAG] = {"units": {"USD": [dict(tags["Revenues"]["units"]["USD"][0])]}}
            if damage == "different_basis":
                tags[TAG]["units"]["USD"][0]["val"] = 80
        else:
            tags[TAG] = tags.pop("Revenues")
        return httpx.Response(200, json=body)

    store = ProjectionStore()
    with monkeypatch.context() as legacy:
        legacy.delitem(sec._SEC_TAG_TO_CANONICAL_FACT, ("us-gaap", TAG))
        async with httpx.AsyncClient(transport=httpx.MockTransport(source)) as client:
            await sec_local.refresh_sec_research(
                settings,
                INSTRUMENT,
                contact="test@example.invalid",
                http_client=client,
                store=store,
            )
    factory = session_factory(settings)
    async with factory() as session:
        filing = await session.scalar(select(Filing))
        filing_id = filing.id
        before = {
            row.id: tuple(getattr(row, c.key) for c in FinancialFactRecord.__table__.columns)
            for row in await session.scalars(select(FinancialFactRecord))
        }
    if damage in {"hash", "length"}:
        for key, raw in tuple(store.objects.items()):
            if key.startswith("evidence/sec/"):
                store.objects[key] = b" " + raw if damage == "length" else b"!" + raw[1:]
    if damage == "missing_owner":
        filing_id = UUID(int=0)
    with pytest.raises(ValueError, match="SEC_|AMBIGUOUS_REVENUE_BASIS"):
        await sec_local.project_saved_sec_facts(settings, filing_id, store=store)
    async with factory() as session:
        after = {
            row.id: tuple(getattr(row, c.key) for c in FinancialFactRecord.__table__.columns)
            for row in await session.scalars(select(FinancialFactRecord))
        }
        assert after == before
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_saved_sec_projection_is_append_only_idempotent_and_cutoff_safe(
    migrated_database_url,
    monkeypatch,
):
    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    await seed_instrument(settings)
    requests = []
    respond = sec_responder(requests)

    def contract_revenue(request):
        response = respond(request)
        if "companyfacts" in request.url.path:
            body = response.json()
            tags = body["facts"]["us-gaap"]
            tags[TAG] = tags.pop("Revenues")
            return httpx.Response(200, json=body)
        return response

    store = ProjectionStore()
    # Emulate the old application with the actual producer and original source:
    # the complete raw object is saved, while the then-unrecognized tag is skipped.
    with monkeypatch.context() as legacy:
        legacy.delitem(sec._SEC_TAG_TO_CANONICAL_FACT, ("us-gaap", TAG))
        async with httpx.AsyncClient(transport=httpx.MockTransport(contract_revenue)) as client:
            prepared = await sec_local.refresh_sec_research(
                settings,
                INSTRUMENT,
                contact="test@example.invalid",
                http_client=client,
                store=store,
            )
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        snapshot = await session.get(DataSnapshot, UUID(prepared["data_snapshot_id"]))
        old_run = await _admit(session, snapshot, "projection-old")
        old_id, old_cutoff = old_run.id, old_run.decision_time
        old_snapshot = snapshot.revision, dict(snapshot.payload)
        filing = await session.scalar(select(Filing))
        filing_id = filing.id
        old_rows = tuple(await session.scalars(select(FinancialFactRecord)))
        old_values = {
            row.id: tuple(getattr(row, col.key) for col in FinancialFactRecord.__table__.columns)
            for row in old_rows
        }
        assert all(row.canonical_fact != "revenue" for row in old_rows)
    before_objects = dict(store.objects)
    request_count = len(requests)
    first = await sec_local.project_saved_sec_facts(settings, filing_id, store=store)
    second = await sec_local.project_saved_sec_facts(settings, filing_id, store=store)
    assert first["facts_created"] == 1 and second["facts_created"] == 0
    assert first["provider_requests"] == first["model_requests"] == 0
    assert first["execution_locked"] is True
    assert len(requests) == request_count and store.objects == before_objects
    async with factory() as session, session.begin():
        rows = tuple(await session.scalars(select(FinancialFactRecord)))
        assert len(rows) == len(old_rows) + 1
        for row in rows:
            if row.id in old_values:
                assert (
                    tuple(getattr(row, c.key) for c in FinancialFactRecord.__table__.columns)
                    == old_values[row.id]
                )
            else:
                assert row.raw_tag == TAG and row.created_at > old_cutoff
                assert row.accepted_at < row.created_at
        snapshot = await session.get(DataSnapshot, snapshot.id)
        assert (snapshot.revision, snapshot.payload) == old_snapshot
        new_snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC))
        new_run = await _admit(session, new_snapshot, "projection-new")
        new_id = new_run.id
    async with factory() as session:
        for run_id, count in ((old_id, 0), (new_id, 1)):
            run = await session.get(AgentResearchRunRecord, run_id)
            tools = await database_read_only_tools(session, run, allow_fixture_evidence=False)
            result = await tools.execute(
                "get_financial_facts",
                {
                    "instrument_id": str(INSTRUMENT),
                    "data_snapshot_id": str(run.data_snapshot_id),
                    "as_of": run.decision_time.isoformat(),
                    "fields": ["revenue"],
                },
            )
            assert len(result["items"]) == count
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
        base_url="http://testserver",
    ) as client:
        for run_id, count in ((old_id, 0), (new_id, 1)):
            path = f"/internal/v1/research-runs/{run_id}"
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200
            facts = response.json()["data"]["inputs"]["facts"]
            assert sum(row["raw_tag"] == TAG for row in facts) == count
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tag,canonical",
    [
        ("DepreciationDepletionAndAmortization", "depreciation_depletion_amortization"),
        ("IncreaseDecreaseInAccountsReceivable", "cash_flow_change_accounts_receivable"),
    ],
)
async def test_saved_cash_flow_component_is_append_only_and_cutoff_safe(
    migrated_database_url,
    monkeypatch,
    tag,
    canonical,
):
    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    await seed_instrument(settings)
    requests = []
    respond = sec_responder(requests)

    def contract_revenue(request):
        response = respond(request)
        if "companyfacts" in request.url.path:
            body = response.json()
            tags = body["facts"]["us-gaap"]
            tags[tag] = tags.pop("Revenues")
            return httpx.Response(200, json=body)
        return response

    store = ProjectionStore()
    # Emulate the old application with the actual producer and original source:
    # the complete raw object is saved, while the then-unrecognized tag is skipped.
    with monkeypatch.context() as legacy:
        legacy.delitem(sec._SEC_TAG_TO_CANONICAL_FACT, ("us-gaap", tag))
        async with httpx.AsyncClient(transport=httpx.MockTransport(contract_revenue)) as client:
            prepared = await sec_local.refresh_sec_research(
                settings,
                INSTRUMENT,
                contact="test@example.invalid",
                http_client=client,
                store=store,
            )
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        snapshot = await session.get(DataSnapshot, UUID(prepared["data_snapshot_id"]))
        old_run = await _admit(session, snapshot, "projection-old")
        old_id, old_cutoff = old_run.id, old_run.decision_time
        old_snapshot = snapshot.revision, dict(snapshot.payload)
        filing = await session.scalar(select(Filing))
        filing_id = filing.id
        old_rows = tuple(await session.scalars(select(FinancialFactRecord)))
        old_values = {
            row.id: tuple(getattr(row, col.key) for col in FinancialFactRecord.__table__.columns)
            for row in old_rows
        }
        assert all(row.canonical_fact != canonical for row in old_rows)
    before_objects = dict(store.objects)
    request_count = len(requests)
    first = await sec_local.project_saved_sec_facts(settings, filing_id, store=store)
    second = await sec_local.project_saved_sec_facts(settings, filing_id, store=store)
    assert first["facts_created"] == 1 and second["facts_created"] == 0
    assert first["provider_requests"] == first["model_requests"] == 0
    assert first["execution_locked"] is True
    assert len(requests) == request_count and store.objects == before_objects
    async with factory() as session, session.begin():
        rows = tuple(await session.scalars(select(FinancialFactRecord)))
        assert len(rows) == len(old_rows) + 1
        for row in rows:
            if row.id in old_values:
                assert (
                    tuple(getattr(row, c.key) for c in FinancialFactRecord.__table__.columns)
                    == old_values[row.id]
                )
            else:
                assert row.raw_tag == tag and row.created_at > old_cutoff
                assert row.accepted_at < row.created_at
        snapshot = await session.get(DataSnapshot, snapshot.id)
        assert (snapshot.revision, snapshot.payload) == old_snapshot
        new_snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC))
        new_run = await _admit(session, new_snapshot, "projection-new")
        new_id = new_run.id
    async with factory() as session:
        for run_id, count in ((old_id, 0), (new_id, 1)):
            run = await session.get(AgentResearchRunRecord, run_id)
            tools = await database_read_only_tools(session, run, allow_fixture_evidence=False)
            result = await tools.execute(
                "get_financial_facts",
                {
                    "instrument_id": str(INSTRUMENT),
                    "data_snapshot_id": str(run.data_snapshot_id),
                    "as_of": run.decision_time.isoformat(),
                    "fields": [canonical],
                },
            )
            assert len(result["items"]) == count
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
        base_url="http://testserver",
    ) as client:
        for run_id, count in ((old_id, 0), (new_id, 1)):
            path = f"/internal/v1/research-runs/{run_id}"
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200
            facts = response.json()["data"]["inputs"]["facts"]
            assert sum(row["raw_tag"] == tag for row in facts) == count
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_component_selection_keeps_core_eps_periods_and_same_frozen_valuation(
    migrated_database_url, monkeypatch
):
    from copy import deepcopy

    from nexus.agents import codex_sdk
    from nexus.agents.models import ResearchRequest
    from nexus.agents.tools import CASH_FLOW_COMPONENT_FIELDS
    from unit.agents.test_codex_sdk import _authority
    from unit.agents.test_trailing_valuation import _financial_facts, _security_snapshot

    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    await seed_instrument(settings)
    respond = sec_responder([])
    eps = _financial_facts()["items"]

    def many_periods(request):
        response = respond(request)
        if "companyfacts" not in request.url.path:
            return response
        body = response.json()
        base = deepcopy(body["facts"]["us-gaap"]["Revenues"]["units"]["USD"][0])
        tags = {}
        for (taxonomy, tag), canonical in sec._SEC_TAG_TO_CANONICAL_FACT.items():
            if (
                taxonomy != "us-gaap"
                or tag == TAG
                or canonical in {"total_assets", "total_liabilities"}
            ):
                continue
            unit = "USD/shares" if canonical == "diluted_eps" else "USD"
            rows = []
            for period in eps:
                row = {
                    **base,
                    "start": period["period_start"],
                    "end": period["period_end"],
                    "val": period["value"] if canonical == "diluted_eps" else 100,
                }
                row.pop("frame", None)
                rows.append(row)
            tags[tag] = {"units": {unit: rows}}
        body["facts"]["us-gaap"] = tags
        return httpx.Response(200, json=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(many_periods)) as client:
        prepared = await sec_local.refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=ProjectionStore(),
        )
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            snapshot = await session.get(DataSnapshot, UUID(prepared["data_snapshot_id"]))
            run = await _admit(session, snapshot, "components-core")
            tools = await database_read_only_tools(session, run, allow_fixture_evidence=False)
            scope = {
                "instrument_id": str(run.instrument_id),
                "data_snapshot_id": str(run.data_snapshot_id),
                "as_of": run.decision_time.isoformat(),
            }
            # Show the actual shared-cap regression, then verify separate requests.
            core = [
                "revenue",
                "gross_profit",
                "operating_income",
                "net_income",
                "operating_cash_flow",
                "capital_expenditure",
                "stockholders_equity",
                "diluted_shares",
                "diluted_eps",
            ]
            crowded = await tools.execute(
                "get_financial_facts", {**scope, "fields": core + list(CASH_FLOW_COMPONENT_FIELDS)}
            )
            assert (
                codex_sdk.derive_trailing_valuation(_security_snapshot(), crowded)["status"]
                == "unavailable"
            )
            actual_execute = tools.execute

            async def execute(name, payload):
                if name == "get_security_snapshot":
                    return _security_snapshot()
                return await actual_execute(name, payload)

            monkeypatch.setattr(tools, "execute", execute)
            _, _, prompts = _authority()
            frozen = await codex_sdk.CodexCommittee(prompts=prompts, tools=tools)._inputs(
                ResearchRequest(
                    instrument_id=run.instrument_id,
                    data_snapshot_id=run.data_snapshot_id,
                    decision_time=run.decision_time,
                )
            )
            financial = frozen["financial_facts"]
            assert len([r for r in financial["items"] if r["fact"] == "diluted_eps"]) == 4
            assert len(financial["items"]) <= 50
            assert len(financial["cash_flow_components"]["items"]) == 44
            assert frozen["trailing_valuation"]["status"] == "available"
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_new_component_fact_prevents_schema_downgrade(migrated_database_url, monkeypatch):
    import asyncio

    from alembic import command
    from alembic.config import Config
    from sqlalchemy import text

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    respond = sec_responder([])

    def component(request):
        response = respond(request)
        if "companyfacts" not in request.url.path:
            return response
        body = response.json()
        body["facts"]["us-gaap"]["ShareBasedCompensation"] = body["facts"]["us-gaap"]["Revenues"]
        return httpx.Response(200, json=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(component)) as client:
        await sec_local.refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=ProjectionStore(),
        )
    factory = session_factory(settings)
    try:
        async with factory() as session:
            before = tuple(
                await session.scalars(
                    select(FinancialFactRecord.id).order_by(FinancialFactRecord.id)
                )
            )
            assert (
                await session.scalar(
                    select(FinancialFactRecord.id).where(
                        FinancialFactRecord.canonical_fact == "share_based_compensation"
                    )
                )
                is not None
            )
        monkeypatch.setenv("DATABASE_URL", migrated_database_url)
        with pytest.raises(RuntimeError, match="SEC_COMPONENT_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(
                command.downgrade, Config("engine/alembic.ini"), "0120_sol_formal_output_binding"
            )
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                == "0121_sec_cash_flow_components"
            )
            assert (
                tuple(
                    await session.scalars(
                        select(FinancialFactRecord.id).order_by(FinancialFactRecord.id)
                    )
                )
                == before
            )
    finally:
        await factory.kw["bind"].dispose()
