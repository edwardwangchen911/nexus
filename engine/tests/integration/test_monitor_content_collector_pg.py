"""Normal exact SEC content collection imports immutable originals, never research jobs."""

import json
from uuid import UUID

import httpx
import pytest
from integration.test_sec_index_pg import response, seed, submissions
from nexus.monitoring.filing_archive import SOURCE, read_filing_content
from nexus.ops.sec_index_local import collect_index
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from sqlalchemy import func, select


def documents(entry, *, invalid_text=False):
    bodies = {
        entry["primaryDocument"][
            0
        ]: b"<p>Item 2.02. Quarterly results require a full investment review.</p>",
        "ex992.htm": b"<p>Reconciliation of GAAP and non-GAAP earnings for this quarter.</p>",
    }
    if invalid_text:
        bodies["ex992.htm"] = b"<p>short</p>"
    rows = "".join(
        f'<tr><td>{i}</td><td>Document</td><td><a href="{name}">{name}</a></td>'
        f"<td>{entry['form'][0] if i == 1 else 'EX-99.2'}</td>"
        f"<td>{len(body)}</td></tr>"
        for i, (name, body) in enumerate(bodies.items(), 1)
    )
    return ('<table summary="Document Format Files">' + rows + "</table>").encode(), bodies


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["8-K", "8-K/A", "failed_document", "absent_entry", "import_failure"]
)
async def test_normal_collector_binds_exact_index_and_preserves_failed_originals(
    migrated_database_url, tmp_path, monkeypatch, mode
):
    settings, store, identity, parent = await seed(migrated_database_url, tmp_path)
    monkeypatch.setattr(store, "read_evidence", store.read_raw, raising=False)
    metadata = submissions()
    if mode == "8-K/A":
        metadata["filings"]["recent"]["form"][0] = "8-K/A"
    async with httpx.AsyncClient(transport=httpx.MockTransport(response(metadata, []))) as client:
        first = await collect_index(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archive_dir=parent / "index",
            contact="test@example.invalid",
            client=client,
            store=store,
        )
    recent = metadata["filings"]["recent"]
    raw_inventory, bodies = documents(recent, invalid_text=mode == "import_failure")
    accession = recent["accessionNumber"][0]
    # A newer capture deliberately disagrees. The collector must honor first's
    # exact ID, not pick this more recent primary or revise the frozen old entry.
    recent["primaryDocument"][0] = "newer.htm"
    async with httpx.AsyncClient(transport=httpx.MockTransport(response(metadata, []))) as client:
        await collect_index(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archive_dir=parent / "later",
            contact="test@example.invalid",
            client=client,
            store=store,
        )
    requests = []

    def respond(request):
        requests.append(str(request.url))
        name = request.url.path.rsplit("/", 1)[-1]
        assert request.url.host == "www.sec.gov"
        if name == accession + "-index.html":
            return httpx.Response(200, content=raw_inventory)
        if mode == "failed_document":
            return httpx.Response(503)
        assert name != "newer.htm"
        return httpx.Response(200, content=bodies[name])

    # Import here so RED identifies the missing normal product entry after the
    # real source and migration fixtures have completed successfully.
    from nexus.ops.monitor_content_local import collect_filing_content

    archive = parent / "content"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        kwargs = dict(
            index_snapshot_id=UUID(first["snapshot_id"]),
            accession=accession if mode != "absent_entry" else accession[:-6] + "999999",
            archive_dir=archive,
            contact="test@example.invalid",
            http_client=client,
            store=store,
        )
        if mode in {"failed_document", "absent_entry", "import_failure"}:
            with pytest.raises(ValueError, match="SEC_MONITOR_"):
                await collect_filing_content(settings, **kwargs)
        else:
            result = await collect_filing_content(settings, **kwargs)
            assert result["status"] == "created"
            assert result["provider_requests"] == 3
            assert result["model_requests"] == 0 and result["job_created"] is False
        assert not client.is_closed
    factory = session_factory(settings)
    try:
        async with factory() as session:
            session.info["sec_exhibit_store"] = store
            assert await session.scalar(select(func.count()).select_from(Job)) == 0
            snapshots = list(
                await session.scalars(select(DataSnapshot).where(DataSnapshot.source == SOURCE))
            )
            if mode in {"failed_document", "absent_entry", "import_failure"}:
                assert snapshots == []
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(ResearchEvidenceRef)
                        .where(ResearchEvidenceRef.source == SOURCE)
                    )
                    == 0
                )
            else:
                assert len(snapshots) == 1
                now = await session.scalar(select(func.clock_timestamp()))
                read = await read_filing_content(session, snapshot_id=snapshots[0].id, as_of=now)
                assert read is not None
                assert read.archive.index_snapshot_id == UUID(first["snapshot_id"])
                assert read.archive.entry.primary_document == "first.htm"
                assert [d.filename for d in read.archive.documents] == ["first.htm", "ex992.htm"]
                assert read.archive.accounting_revision_status == "unverified"
                assert read.archive.amendment_relation_status == "not_assessed"
                assert all(not d.truncated for d in read.archive.documents)
                assert json.loads((archive / "result.json").read_bytes())["snapshot_id"] == str(
                    snapshots[0].id
                )
    finally:
        await factory.kw["bind"].dispose()
    if mode == "failed_document":
        assert len(requests) == 2
        assert (archive / (accession + "-index.html")).read_bytes() == raw_inventory
        assert json.loads((archive / "result.json").read_bytes())["provider_requests"] == 2
    elif mode == "absent_entry":
        assert requests == [] and not archive.exists()

    elif mode == "import_failure":
        assert len(requests) == 3
        assert (archive / "ex992.htm").read_bytes() == b"<p>short</p>"
        failure = json.loads((archive / "result.json").read_bytes())
        assert failure["provider_requests"] == 3
        assert failure["stage"] == "import"
        assert failure["error_code"] == "SEC_MONITOR_CONTENT_CAPACITY"
