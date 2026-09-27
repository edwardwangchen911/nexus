"""Normal SEC entry keeps ordinary and amended evidence distinct and frozen."""

import httpx
import pytest
from nexus.ops.sec_local import refresh_sec_research
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.research import Filing, FinancialFactRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from test_sec_amendment_storage import Store
from test_sec_local import INSTRUMENT, disclosure_responder, seed_instrument


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "amended_at", ["2026-08-21T21:00:00Z", "2026-08-21T20:00:00Z", "2026-08-21T19:00:00Z"]
)
@pytest.mark.parametrize("ordinary", [True, False])
async def test_normal_intake_freezes_independent_amendment_with_limits(
    migrated_database_url, amended_at, ordinary, monkeypatch
):
    from nexus.api.research_inputs import read_research_inputs
    from nexus.ops.openai_local import prepare_database
    from nexus.research.sec_disclosures import read_disclosures

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings)
    await seed_instrument(settings)
    requests, store = [], Store()
    base = disclosure_responder(requests, form="8-K")

    def respond(request):
        if request.url.path.endswith("amendment.htm"):
            requests.append(request)
            return httpx.Response(
                200,
                content=(
                    b"<p>Amendment supplies additional context, not complete replacement facts.</p>"
                ),
            )
        response = base(request)
        if request.url.path.startswith("/submissions/"):
            p = response.json()
            r = p["filings"]["recent"]
            if not ordinary:
                for value in r.values():
                    value.clear()
            for key, value in {
                "accessionNumber": "0000320193-26-000002",
                "acceptanceDateTime": amended_at,
                "filingDate": "2026-08-21",
                "form": "8-K/A",
                "primaryDocument": "amendment.htm",
            }.items():
                r[key].append(value)
            return httpx.Response(200, json=p)
        return response

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=store,
            disclosure_only=True,
            disclosure_form="8-K",
        )
        repeated = await refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=store,
            disclosure_only=True,
            disclosure_form="8-K",
        )
    assert result["provider_requests"] == repeated["provider_requests"] == (4 if ordinary else 3)
    assert len(requests) == (8 if ordinary else 6)
    assert repeated["filings_created"] == 0
    assert repeated["filings_replayed"] == (2 if ordinary else 1)
    assert result["filings_created"] == (2 if ordinary else 1)
    assert result["model_requests"] == 0 and result["execution_locked"] is True
    async with session_factory(settings)() as session:
        session.info["sec_exhibit_store"] = store
        from uuid import UUID

        sid = UUID(result["data_snapshot_id"])
        ids = tuple(
            await session.scalars(
                select(AgentDataSnapshotEvidenceRecord.evidence_ref_id).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == sid
                )
            )
        )
        now = await session.scalar(select(func.clock_timestamp()))
        docs = await read_disclosures(
            session, instrument_id=INSTRUMENT, evidence_ids=ids, as_of=now
        )
        assert {d.form for d in docs.values()} == ({"8-K", "8-K/A"} if ordinary else {"8-K/A"})
        summary = await read_research_inputs(
            session, snapshot_id=sid, instrument_id=INSTRUMENT, as_of=now
        )
        amendment = next(f for f in summary.filings if f.form == "8-K/A")
        assert amendment.amendment.relation_status == "unknown"
        assert amendment.amendment.attachments_status == "not_collected"
        assert amendment.amendment.accounting_revision_status == "unverified"
        assert amendment.received_at == next(
            d.received_at for d in docs.values() if d.form == "8-K/A"
        )
        assert await session.scalar(select(func.count()).select_from(FinancialFactRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(Filing)) == (
            2 if ordinary else 1
        )

    # One representative case traverses the normal HTTP admission and Worker/SDK path.
    if ordinary and amended_at == "2026-08-21T21:00:00Z":
        import json
        from datetime import UTC, datetime
        from unittest.mock import AsyncMock

        import nexus.research.sec_exhibit_storage as exhibit_storage
        from helpers.portfolio_api import app, headers
        from nexus.agents import codex_sdk
        from nexus.agents.committee import ROLE_ORDER
        from nexus.agents.responses_budget import UsageSnapshot
        from nexus.worker import run_agent_worker_once

        monkeypatch.setattr(exhibit_storage, "configured_exhibit_store", lambda session: store)
        invoke = AsyncMock(
            return_value=(
                json.dumps(
                    {
                        "roles": [
                            {
                                "role": role,
                                "vote": "abstain",
                                "claims": [
                                    {
                                        "text": "独立修订文本存在，关联与会计影响尚未核实。",
                                        "citations": [0],
                                    }
                                ],
                                "conflicting_facts": False,
                            }
                            for role in ROLE_ORDER
                        ]
                    }
                ),
                UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1),
            )
        )
        monkeypatch.setattr(codex_sdk, "invoke_codex", invoke)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
            base_url="http://testserver",
        ) as client:
            path = (
                f"/internal/v1/instruments/{INSTRUMENT}/research-admission?"
                + result["research_path"].split("?", 1)[1]
            )
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200
            admission = response.json()["data"]
            filing = next(f for f in admission["inputs"]["filings"] if f["form"] == "8-K/A")
            assert filing["amendment"]["relation_status"] == "unknown"
            assert filing["amendment"]["accounting_revision_status"] == "unverified"
            body = json.dumps(
                {
                    "instrument_id": str(INSTRUMENT),
                    "data_snapshot_id": result["data_snapshot_id"],
                    "decision_time": admission["decision_time"],
                }
            ).encode()
            path = "/internal/v1/research-runs"
            submitted = await client.post(
                path,
                content=body,
                headers={**headers("POST", path, body), "Idempotency-Key": "amendment-research"},
            )
            assert submitted.status_code == 202
            assert await run_agent_worker_once(settings)
            invoke.assert_awaited_once()
            frozen = invoke.call_args.args[0]["frozen_inputs"]
            amendment = next(
                f["disclosure"]
                for f in frozen["filing_disclosures"]["items"]
                if f["form"] == "8-K/A"
            )
            assert amendment["relation_status"] == "unknown"
            assert amendment["content_coverage"] == "primary_document_only"
            assert amendment["attachments_status"] == "not_collected"
            assert amendment["accounting_revision_status"] == "unverified"
            assert frozen["financial_facts"]["items"] == []
            from nexus.agents.database_tools import database_read_only_tools
            from nexus.persistence.models.agents import AgentResearchRunRecord

            async with session_factory(settings)() as session:
                session.info["sec_exhibit_store"] = store
                run = await session.get(
                    AgentResearchRunRecord, UUID(submitted.json()["data"]["run_id"])
                )
                tools = await database_read_only_tools(session, run, allow_fixture_evidence=True)
                scope = {
                    "instrument_id": INSTRUMENT,
                    "data_snapshot_id": sid,
                    "as_of": run.decision_time,
                }
                selected = {
                    **scope,
                    "forms": ["8-K/A"],
                    "filed_from": "2026-08-01",
                    "filed_to": "2026-09-27",
                }
                valid = await tools.execute("search_filing_evidence", selected)
                assert valid["status"] == "ok" and valid["items"][0]["disclosure"]
                raw_key = next(
                    key for key in store.objects if key.startswith("raw/sec/amendments/")
                )
                canonical_key = next(
                    key
                    for key in store.objects
                    if key.startswith("evidence/") and "0000320193-26-000002" in key
                )
                for key in (raw_key, canonical_key):
                    saved_bytes = store.objects.pop(key)
                    invalid = await tools.execute("search_filing_evidence", selected)
                    assert invalid["status"] == "unavailable"
                    assert invalid["reason"] == "invalid_frozen_evidence_authority"
                    assert invalid["items"] == []
                    store.objects[key] = saved_bytes


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["same_form_tie", "amendment_path"])
async def test_ambiguous_or_unsafe_amendment_stops_before_document_fetch(
    migrated_database_url, failure
):
    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    requests, store = [], Store()
    base = disclosure_responder(requests, form="8-K")

    def respond(request):
        response = base(request)
        if request.url.path.startswith("/submissions/"):
            payload = response.json()
            recent = payload["filings"]["recent"]
            for key, value in {
                "accessionNumber": "0000320193-26-000002",
                "acceptanceDateTime": "2026-08-21T20:00:00Z",
                "filingDate": "2026-08-21",
                "form": "8-K" if failure == "same_form_tie" else "8-K/A",
                "primaryDocument": "doc.htm" if failure == "same_form_tie" else "../amendment.htm",
            }.items():
                recent[key].append(value)
            return httpx.Response(200, json=payload)
        return response

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError, match="DISCLOSURE_AMBIGUOUS|DISCLOSURE_PATH_INVALID"):
            await refresh_sec_research(
                settings,
                INSTRUMENT,
                contact="test@example.invalid",
                http_client=client,
                store=store,
                disclosure_only=True,
                disclosure_form="8-K",
            )
    assert len(requests) == 2
    async with session_factory(settings)() as session:
        assert await session.scalar(select(func.count()).select_from(Filing)) == 0
