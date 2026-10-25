"""Exact content collection never follows an inventory URL or retries a failed GET."""

import json
import stat
from datetime import UTC, datetime
from hashlib import sha256

import httpx
import pytest
from nexus.research.sec_disclosure_index import SecIndexEntry

CIK = "0000123456"
ACCESSION = CIK + "-26-000002"
ROOT = "https://www.sec.gov/Archives/edgar/data/123456/000012345626000002/"
ENTRY = SecIndexEntry(
    accession_number=ACCESSION,
    form="8-K/A",
    accepted_at=datetime(2026, 10, 1, tzinfo=UTC),
    filed_at=datetime(2026, 10, 1, tzinfo=UTC),
    primary_document="main.htm",
)
BODIES = {
    "main.htm": b"<p>Item 2.02 amendment: results remain subject to complete review.</p>",
    "extra.htm": b"<p>Additional reconciliation exhibit with exact fiscal period definitions.</p>",
}


def inventory(href="extra.htm", filename="extra.htm"):
    return (
        '<table summary="Document Format Files">'
        '<tr><td>1</td><td>Report</td><td><a href="main.htm">main.htm</a></td>'
        f"<td>8-K/A</td><td>{len(BODIES['main.htm'])}</td></tr>"
        f'<tr><td>2</td><td>Exhibit</td><td><a href="{href}">{filename}</a></td>'
        f"<td>EX-99.2</td><td>{len(BODIES['extra.htm'])}</td></tr></table>"
    ).encode()


def destination(tmp_path):
    parent = tmp_path / "private"
    parent.mkdir(mode=0o700)
    return parent / "capture"


@pytest.mark.asyncio
async def test_each_real_200_is_frozen_with_exact_receipt_and_private_permissions(tmp_path):
    from nexus.ops.monitor_content_local import _collect_documents

    calls = []
    raw_inventory = inventory()

    def respond(request):
        calls.append(str(request.url))
        assert request.method == "GET"
        assert request.headers["user-agent"] == "NEXUS test@example.invalid"
        return httpx.Response(
            200,
            content=raw_inventory
            if request.url.path.endswith("-index.html")
            else BODIES[request.url.path.rsplit("/", 1)[-1]],
        )

    path = destination(tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        count = await _collect_documents(
            entry=ENTRY,
            issuer_cik=CIK,
            archive_dir=path,
            contact="test@example.invalid",
            http_client=client,
        )
    assert count == 3
    assert calls == [ROOT + ACCESSION + "-index.html", ROOT + "main.htm", ROOT + "extra.htm"]
    assert stat.S_IMODE(path.stat().st_mode) == 0o700
    for name, raw in {ACCESSION + "-index.html": raw_inventory, **BODIES}.items():
        assert (path / name).read_bytes() == raw
        receipt = json.loads((path / (name + ".receipt.json")).read_bytes())
        assert receipt["http_status"] == 200
        assert receipt["source_url"] == ROOT + name
        assert receipt["byte_length"] == len(raw)
        assert receipt["sha256"] == sha256(raw).hexdigest()
        assert datetime.fromisoformat(receipt["received_at"]).tzinfo is not None
    assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in path.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [206, 204, 301, 403, 429, 503])
async def test_partial_redirect_or_failed_response_is_not_archived_or_retried(tmp_path, status):
    from nexus.ops.monitor_content_local import _collect_documents

    calls = []

    def respond(request):
        calls.append(str(request.url))
        if len(calls) == 1:
            return httpx.Response(200, content=inventory())
        return httpx.Response(
            status, headers={"location": "https://evil.invalid/body"}, content=b"no"
        )

    path = destination(tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError, match="SEC_MONITOR_COLLECTION_FAILED"):
            await _collect_documents(
                entry=ENTRY,
                issuer_cik=CIK,
                archive_dir=path,
                contact="test@example.invalid",
                http_client=client,
            )
        assert not client.is_closed
    assert len(calls) == 2
    assert (path / (ACCESSION + "-index.html")).read_bytes() == inventory()
    assert not (path / "main.htm").exists()
    failure = json.loads((path / "result.json").read_bytes())
    assert failure["provider_requests"] == 2
    assert failure["stage"] == "documents"
    assert failure["job_created"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "href,filename",
    [
        ("https://evil.invalid/extra.htm", "extra.htm"),
        ("../extra.htm", "extra.htm"),
        ("%2e%2e/extra.htm", "extra.htm"),
        ("extra.pdf", "extra.pdf"),
    ],
)
async def test_inventory_cannot_expand_origin_path_or_document_type(tmp_path, href, filename):
    from nexus.ops.monitor_content_local import _collect_documents

    calls = []

    def respond(request):
        calls.append(str(request.url))
        return httpx.Response(200, content=inventory(href, filename))

    path = destination(tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError, match="SEC_MONITOR_"):
            await _collect_documents(
                entry=ENTRY,
                issuer_cik=CIK,
                archive_dir=path,
                contact="test@example.invalid",
                http_client=client,
            )
    assert calls == [ROOT + ACCESSION + "-index.html"]
    assert json.loads((path / "result.json").read_bytes())["provider_requests"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["contact", "public_parent", "existing", "symlink"])
async def test_invalid_contact_or_archive_directory_cannot_send_requests(tmp_path, mode):
    from nexus.ops.monitor_content_local import _collect_documents

    path = destination(tmp_path)
    if mode == "public_parent":
        path.parent.chmod(0o755)
    elif mode == "existing":
        path.mkdir(mode=0o700)
        (path / "saved").write_bytes(b"original")
    elif mode == "symlink":
        path.symlink_to(tmp_path, target_is_directory=True)

    def respond(request):
        pytest.fail("invalid input sent an HTTP request")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError, match="SEC_MONITOR_"):
            await _collect_documents(
                entry=ENTRY,
                issuer_cik=CIK,
                archive_dir=path,
                contact="invalid\ncontact" if mode == "contact" else "test@example.invalid",
                http_client=client,
            )
    if mode == "existing":
        assert (path / "saved").read_bytes() == b"original"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["database", "store"])
async def test_nonlocal_configuration_is_rejected_before_database_or_http(mode, tmp_path):
    from uuid import uuid4

    from nexus.ops.monitor_content_local import collect_filing_content
    from nexus.settings import EngineSettings

    settings = EngineSettings(
        database_url="postgresql+asyncpg://synthetic:unused@"
        + ("remote.invalid/test" if mode == "database" else "127.0.0.1:1/test"),
        object_store_endpoint="https://remote.invalid",
    )
    with pytest.raises(ValueError):
        await collect_filing_content(
            settings,
            index_snapshot_id=uuid4(),
            accession=ACCESSION,
            archive_dir=tmp_path / "content",
            contact="test@example.invalid",
        )
    assert not (tmp_path / "content").exists()


@pytest.mark.asyncio
async def test_stream_size_limit_retains_prior_originals_and_does_not_retry(tmp_path):
    from nexus.ops.monitor_content_local import _collect_documents

    calls = []

    def respond(request):
        calls.append(str(request.url))
        return (
            httpx.Response(200, content=inventory())
            if len(calls) == 1
            else httpx.Response(
                200,
                headers={"content-length": str(8 * 1024 * 1024 + 1)},
                content=b"not complete",
            )
        )

    path = destination(tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError, match="SEC_MONITOR_COLLECTION_FAILED"):
            await _collect_documents(
                entry=ENTRY,
                issuer_cik=CIK,
                archive_dir=path,
                contact="test@example.invalid",
                http_client=client,
            )
    assert len(calls) == 2
    assert not (path / "main.htm").exists()
    assert json.loads((path / "result.json").read_bytes())["provider_requests"] == 2


@pytest.mark.asyncio
async def test_cancellation_keeps_completed_capture_and_propagates_without_retry(tmp_path):
    import asyncio

    from nexus.ops.monitor_content_local import _collect_documents

    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def respond(request):
        calls.append(str(request.url))
        if len(calls) == 1:
            return httpx.Response(200, content=inventory())
        entered.set()
        await release.wait()
        pytest.fail("cancelled response must not resume")

    path = destination(tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        task = asyncio.create_task(
            _collect_documents(
                entry=ENTRY,
                issuer_cik=CIK,
                archive_dir=path,
                contact="test@example.invalid",
                http_client=client,
            )
        )
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not client.is_closed
    assert len(calls) == 2
    assert (path / (ACCESSION + "-index.html")).read_bytes() == inventory()
    assert json.loads((path / "result.json").read_bytes())["provider_requests"] == 2
