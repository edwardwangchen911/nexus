import json
from hashlib import sha256

import httpx
import pytest
from nexus.ops.instrument_identity_local import collect_sec

URL = "https://www.sec.gov/Archives/edgar/data/123456/000123456726000001/0001234567-26-000001.txt"


def test_normal_submission_cli_loads_config_and_does_not_import(tmp_path, monkeypatch, capsys):
    import sys

    from nexus.ops import instrument_identity_local as module

    calls = []

    async def collect(**kwargs):
        calls.append(kwargs)

    def forbidden(*args, **kwargs):
        pytest.fail("archive-only CLI must not construct database or gateway")

    monkeypatch.setattr(module, "collect_sec", collect)
    monkeypatch.setattr(module, "session_factory", forbidden)
    monkeypatch.setattr(module, "FosunGatewayClient", forbidden)
    monkeypatch.setattr(
        module, "load_local_environment", lambda *args: {"SEC_CONTACT": "test@example.com"}
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "identity",
            "--repo-root",
            str(tmp_path),
            "collect-submission",
            "--source-url",
            URL,
            "--output-dir",
            str(tmp_path / "new"),
        ],
    )
    module.main()
    assert len(calls) == 1
    assert calls[0]["submission"] is True
    assert calls[0]["source_url"] == URL
    assert json.loads(capsys.readouterr().out)["status"] == "collected"


@pytest.mark.asyncio
async def test_submission_archive_retains_exact_bytes_and_receipt(tmp_path):
    raw = b"<SEC-DOCUMENT>synthetic submission</SEC-DOCUMENT>"
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, content=raw)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        output = tmp_path / "new"
        await collect_sec(
            source_url=URL,
            contact="test@example.com",
            output_dir=output,
            client=client,
            submission=True,
        )
    assert len(requests) == 1
    assert (output / "0001234567-26-000001.txt").read_bytes() == raw
    receipt = json.loads((output / "receipt.json").read_text())
    assert receipt["source_url"] == URL
    assert receipt["sha256"] == sha256(raw).hexdigest()
    assert receipt["byte_length"] == len(raw)
    assert output.stat().st_mode & 0o777 == 0o700


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        URL.replace("0001234567-26-000001.txt", "0001234567-26-000002.txt"),
        URL.replace("www.sec.gov", "example.com"),
        URL + "?x=1",
        URL.replace(".txt", ".html"),
        URL.replace("/123456/", "/0123456/"),
    ],
)
async def test_submission_url_rejected_before_request_or_directory(tmp_path, url):
    def forbidden(request):
        pytest.fail("invalid source must not be contacted")

    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
        with pytest.raises(ValueError, match="CURRENT_IDENTITY_SEC_REQUEST_INVALID"):
            await collect_sec(
                source_url=url,
                contact="test@example.com",
                output_dir=output,
                client=client,
                submission=True,
            )
    assert not output.exists()


@pytest.mark.asyncio
async def test_existing_html_collector_does_not_accept_submission(tmp_path):
    with pytest.raises(ValueError, match="CURRENT_IDENTITY_SEC_REQUEST_INVALID"):
        await collect_sec(source_url=URL, contact="test@example.com", output_dir=tmp_path / "new")


@pytest.mark.asyncio
async def test_submission_failure_is_archived_without_retry(tmp_path):
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(403)

    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ValueError):
            await collect_sec(
                source_url=URL,
                contact="test@example.com",
                output_dir=output,
                client=client,
                submission=True,
            )
    assert len(calls) == 1
    assert not (output / "receipt.json").exists()
    failure = json.loads((output / "failure.json").read_text())
    assert failure["http_status"] == 403
