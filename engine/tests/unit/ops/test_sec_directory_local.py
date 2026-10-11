import json

import httpx
import pytest
from nexus.ops.instrument_identity_local import collect_sec


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,filename",
    [("company", "company_tickers_exchange.json"), ("fund", "company_tickers_mf.json")],
)
async def test_fixed_directory_collection_preserves_raw_and_single_request(
    tmp_path, kind, filename
):
    requests = []
    raw = b'{"fields":[],"data":[]}'

    def handle(request):
        requests.append(str(request.url))
        return httpx.Response(200, content=raw)

    url = "https://www.sec.gov/files/" + filename
    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        await collect_sec(
            source_url=url,
            contact="test@example.com",
            output_dir=output,
            client=client,
            directory_kind=kind,
        )
    assert requests == [url]
    assert (output / filename).read_bytes() == raw
    assert json.loads((output / "receipt.json").read_text())["source_url"] == url


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,url",
    [
        ("fund", "https://www.sec.gov/files/company_tickers_exchange.json"),
        ("other", "https://www.sec.gov/files/company_tickers_mf.json"),
        ("fund", "https://example.com/files/company_tickers_mf.json"),
    ],
)
async def test_directory_kind_and_fixed_source_must_agree_before_fetch(tmp_path, kind, url):
    def forbidden(request):
        pytest.fail("invalid selection must not contact upstream")

    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
        with pytest.raises(ValueError, match="CURRENT_IDENTITY_SEC_REQUEST_INVALID"):
            await collect_sec(
                source_url=url,
                contact="test@example.com",
                output_dir=output,
                client=client,
                directory_kind=kind,
            )
    assert not output.exists()


def test_directory_cli_loads_protected_contact_without_business_services(
    tmp_path, monkeypatch, capsys
):
    import sys

    from nexus.ops import instrument_identity_local as module

    calls = []

    async def collect(**kwargs):
        calls.append(kwargs)

    def forbidden(*args, **kwargs):
        pytest.fail("must not construct business services")

    monkeypatch.setattr(module, "collect_sec", collect)
    monkeypatch.setattr(
        module, "load_local_environment", lambda *args: {"SEC_CONTACT": "test@example.com"}
    )
    monkeypatch.setattr(module, "session_factory", forbidden)
    monkeypatch.setattr(module, "FosunGatewayClient", forbidden)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "identity",
            "collect-sec-directory",
            "--kind",
            "fund",
            "--output-dir",
            str(tmp_path / "new"),
        ],
    )
    module.main()
    assert len(calls) == 1 and calls[0]["directory_kind"] == "fund"
    assert calls[0]["source_url"] == "https://www.sec.gov/files/company_tickers_mf.json"
    assert json.loads(capsys.readouterr().out)["status"] == "collected"
