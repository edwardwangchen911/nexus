import importlib
import json
from hashlib import sha256

import httpx
import pytest


@pytest.mark.asyncio
async def test_normal_collection_archives_both_fixed_sources_without_database(tmp_path):
    module = importlib.import_module("nexus.ops.mic_local")
    requests = []
    raws = {
        module.INDEX_URL: b"official publication page",
        module.CSV_URL: b"MIC,STATUS\nTEST,ACTIVE\n",
    }

    def handle(request):
        requests.append(str(request.url))
        return httpx.Response(200, content=raws[str(request.url)])

    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        await module.collect_mic(output_dir=output, client=client)
    assert requests == [module.INDEX_URL, module.CSV_URL]
    for filename, url in (
        ("publication.html", module.INDEX_URL),
        ("ISO10383_MIC.csv", module.CSV_URL),
    ):
        assert (output / filename).read_bytes() == raws[url]
        receipt = json.loads((output / (filename + ".receipt.json")).read_text())
        assert receipt["source_url"] == url
        assert receipt["sha256"] == sha256(raws[url]).hexdigest()
        assert receipt["byte_length"] == len(raws[url])
        assert receipt["http_status"] == 200
        assert (output / filename).stat().st_mode & 0o777 == 0o600
    assert output.stat().st_mode & 0o777 == 0o700


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 302, 429, 500])
async def test_failure_stops_without_retry_or_partial_success(tmp_path, status):
    module = importlib.import_module("nexus.ops.mic_local")
    requests = []

    def handle(request):
        requests.append(str(request.url))
        return httpx.Response(status, headers={"Location": "https://example.com"})

    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ValueError, match="MIC_COLLECTION_"):
            await module.collect_mic(output_dir=output, client=client)
    assert requests == [module.INDEX_URL]
    failure = json.loads((output / "failure.json").read_text())
    assert failure["stage"] == "publication_fetch"
    assert not (output / "complete.json").exists()


@pytest.mark.asyncio
async def test_second_source_failure_preserves_first_and_does_not_retry(tmp_path):
    module = importlib.import_module("nexus.ops.mic_local")
    requests = []

    def handle(request):
        requests.append(str(request.url))
        return httpx.Response(200, content=b"index") if len(requests) == 1 else httpx.Response(403)

    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ValueError, match="MIC_COLLECTION_"):
            await module.collect_mic(output_dir=output, client=client)
    assert len(requests) == 2
    assert (output / "publication.html").read_bytes() == b"index"
    assert json.loads((output / "failure.json").read_text())["stage"] == "csv_fetch"
    assert not (output / "complete.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [201, 206])
@pytest.mark.parametrize("failed_call", [1, 2])
async def test_non_200_success_status_is_rejected_without_a_success_receipt(
    tmp_path, status, failed_call
):
    module = importlib.import_module("nexus.ops.mic_local")
    requests = []

    def handle(request):
        requests.append(str(request.url))
        return httpx.Response(status if len(requests) == failed_call else 200, content=b"original")

    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ValueError, match="MIC_COLLECTION_STATUS_REJECTED"):
            await module.collect_mic(output_dir=output, client=client)
        assert client.event_hooks["response"] == []
    rejected = "publication.html" if failed_call == 1 else "ISO10383_MIC.csv"
    assert len(requests) == failed_call
    assert not (output / (rejected + ".receipt.json")).exists()
    assert not (output / "complete.json").exists()
    failure = json.loads((output / "failure.json").read_text())
    assert failure["code"] == "MIC_COLLECTION_STATUS_REJECTED"
    if failed_call == 2:
        assert (output / "publication.html").read_bytes() == b"original"


@pytest.mark.asyncio
async def test_existing_archive_rejected_before_network_and_preserved(tmp_path):
    module = importlib.import_module("nexus.ops.mic_local")
    output = tmp_path / "existing"
    output.mkdir()
    original = output / "original"
    original.write_bytes(b"unchanged")

    def forbidden(request):
        pytest.fail("existing archive must not be fetched again")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
        with pytest.raises(ValueError, match="MIC_COLLECTION_NEW_DIRECTORY_REQUIRED"):
            await module.collect_mic(output_dir=output, client=client)
    assert original.read_bytes() == b"unchanged"


def test_product_cli_is_archive_only(tmp_path, monkeypatch, capsys):
    import sys

    from nexus.ops import instrument_identity_local as module

    calls = []

    async def collect(**kwargs):
        calls.append(kwargs)

    def forbidden(*args, **kwargs):
        pytest.fail("must not construct a database or broker")

    monkeypatch.setattr(module, "collect_mic", collect, raising=False)
    monkeypatch.setattr(module, "session_factory", forbidden)
    monkeypatch.setattr(module, "FosunGatewayClient", forbidden)
    monkeypatch.setattr(module, "load_local_environment", lambda *args: {})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "identity",
            "--repo-root",
            str(tmp_path),
            "collect-mic",
            "--output-dir",
            str(tmp_path / "new"),
        ],
    )
    module.main()
    assert len(calls) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "collected"
