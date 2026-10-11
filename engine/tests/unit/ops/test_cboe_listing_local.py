import importlib
import json

import httpx
import pytest


@pytest.mark.asyncio
async def test_fixed_listing_sources_are_archived_once(tmp_path):
    module = importlib.import_module("nexus.ops.cboe_listing_local")
    requests = []

    def handle(request):
        requests.append(str(request.url))
        return httpx.Response(200, content=str(request.url).encode())

    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        await module.collect_listing(symbol="TEST", output_dir=output, client=client)
    assert requests == [
        "https://www.cboe.com/us/equities/market_statistics/listed_symbols/",
        "https://www.cboe.com/us/equities/market_statistics/listed_symbols/csv",
        "https://www.cboe.com/us/equities/listings/listed_products/symbols/TEST/",
    ]
    for name, url in zip(("listed.html", "listed.csv", "product.html"), requests, strict=True):
        assert (output / name).read_bytes() == url.encode()
        assert json.loads((output / (name + ".receipt.json")).read_text())["source_url"] == url
    assert (
        json.loads((output / "complete.json").read_text())["status"]
        == "CBOE_LISTING_ORIGINALS_ARCHIVED"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol", ["../TEST", "TEST?x=y", "test", "", "TEST/OTHER"])
async def test_invalid_symbol_rejected_before_directory_and_request(tmp_path, symbol):
    module = importlib.import_module("nexus.ops.cboe_listing_local")

    def forbidden(request):
        pytest.fail("invalid symbol must not be requested")

    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
        with pytest.raises(ValueError, match="CBOE_LISTING_SYMBOL_INVALID"):
            await module.collect_listing(symbol=symbol, output_dir=output, client=client)
    assert not output.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_call", [1, 2, 3])
async def test_partial_failure_does_not_retry_or_complete(tmp_path, failed_call):
    module = importlib.import_module("nexus.ops.cboe_listing_local")
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(403 if len(requests) == failed_call else 200, content=b"original")

    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ValueError, match="CBOE_LISTING_"):
            await module.collect_listing(symbol="TEST", output_dir=output, client=client)
    assert len(requests) == failed_call
    assert (output / "failure.json").exists()
    assert not (output / "complete.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [201, 206])
@pytest.mark.parametrize("failed_call", [1, 2, 3])
async def test_non_200_success_status_is_rejected_without_a_success_receipt(
    tmp_path, status, failed_call
):
    module = importlib.import_module("nexus.ops.cboe_listing_local")
    requests = []

    def handle(request):
        requests.append(str(request.url))
        return httpx.Response(status if len(requests) == failed_call else 200, content=b"original")

    output = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ValueError, match="CBOE_LISTING_STATUS_REJECTED"):
            await module.collect_listing(symbol="TEST", output_dir=output, client=client)
        assert client.event_hooks["response"] == []
    rejected = ("listed.html", "listed.csv", "product.html")[failed_call - 1]
    assert len(requests) == failed_call
    assert not (output / (rejected + ".receipt.json")).exists()
    assert not (output / "complete.json").exists()
    failure = json.loads((output / "failure.json").read_text())
    assert failure["code"] == "CBOE_LISTING_STATUS_REJECTED"
    for preserved in ("listed.html", "listed.csv")[: failed_call - 1]:
        assert (output / preserved).read_bytes() == b"original"


def test_normal_cli_has_no_business_or_broker_write(tmp_path, monkeypatch, capsys):
    import sys

    from nexus.ops import instrument_identity_local as module

    calls = []

    async def collect(**kwargs):
        calls.append(kwargs)

    def forbidden(*args, **kwargs):
        pytest.fail("archive-only CLI must not construct business services")

    monkeypatch.setattr(module, "collect_listing", collect, raising=False)
    monkeypatch.setattr(module, "load_local_environment", lambda *args: {})
    monkeypatch.setattr(module, "session_factory", forbidden)
    monkeypatch.setattr(module, "FosunGatewayClient", forbidden)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "identity",
            "collect-cboe-listing",
            "--symbol",
            "TEST",
            "--output-dir",
            str(tmp_path / "new"),
        ],
    )
    module.main()
    assert len(calls) == 1 and calls[0]["symbol"] == "TEST"
    assert json.loads(capsys.readouterr().out)["status"] == "collected"
