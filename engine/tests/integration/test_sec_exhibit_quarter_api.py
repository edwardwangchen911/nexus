"""Protected API returns only the projection frozen into that research run."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from integration.test_sec_exhibit_revisions import setup_archive, write_archives
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops.sec_exhibit_local import import_sec_exhibit
from nexus.persistence.session import session_factory
from nexus.research.sec_disclosures import read_disclosures
from unit.research.test_sec_exhibit_quarter_metrics import exhibit_html
from unit.research.test_sec_exhibits import archive


@pytest.mark.asyncio
async def test_protected_api_projects_only_new_frozen_revision(migrated_database_url, tmp_path):
    settings, store, identity, contents, _ = await setup_archive(migrated_database_url, tmp_path)
    contents[2] = archive(exhibit_html(), json.loads(contents[2][1])["source_url"])
    paths = write_archives(tmp_path / "quarter-originals", contents)
    factory = session_factory(settings)
    factory.configure(info={"sec_exhibit_store": store})
    application = app(migrated_database_url, now=datetime.now(UTC))
    application.state.session_factory.configure(info={"sec_exhibit_store": store})
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://testserver"
        ) as client:
            results = []
            for include in (False, True):
                imported = await import_sec_exhibit(
                    settings,
                    identity_snapshot_id=identity.snapshot_id,
                    archives=paths,
                    store=store,
                    include_quarter_metrics=include,
                )
                now = datetime.now(UTC)
                async with factory() as session, session.begin():
                    snapshot = await prepare_research_snapshot(
                        session, identity.instrument_id, as_of=now
                    )
                    assert snapshot is not None
                    request = ResearchRequest(
                        instrument_id=identity.instrument_id,
                        data_snapshot_id=snapshot.id,
                        decision_time=now,
                    )
                    admitted = await admit_research_run(
                        session,
                        actor_id="test",
                        route="/internal/v1/research-runs",
                        idempotency_key=f"quarter-api-{include}",
                        request_hash="0" * 64,
                        request=request,
                        prompt_version_id=None,
                        model_config_id=None,
                        daily_limit_usd=Decimal("3"),
                        now=now,
                    )
                    run_id = admitted.run.id
                    evidence_id = UUID(imported["evidence_ref_id"])
                    reports = await read_disclosures(
                        session,
                        instrument_id=identity.instrument_id,
                        evidence_ids=(evidence_id,),
                        as_of=now,
                    )
                    projection = reports[evidence_id].quarter_metrics
                    expected = projection.model_dump(mode="json") if projection else None
                path = f"/internal/v1/research-runs/{run_id}"
                response = await client.get(path, headers=headers("GET", path))
                assert response.status_code == 200
                result = response.json()["data"]
                filings = result["inputs"]["filings"]
                assert len(filings) == 1
                assert filings[0]["exhibit"].get("quarter_metrics") == expected
                results.append((path, result))
            old_response = await client.get(results[0][0], headers=headers("GET", results[0][0]))
            assert old_response.status_code == 200
            assert old_response.json()["data"] == results[0][1]
    finally:
        await factory.kw["bind"].dispose()
        await application.state.session_factory.kw["bind"].dispose()
