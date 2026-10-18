import json
import sys
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from nexus.ops import forecast_probe as probe
from pydantic import SecretStr

RAW = b'[{"symbol":"AVGO","date":"2027-10-31","epsAvg":5.25,"revenueAvg":100}]'


@pytest.mark.asyncio
async def test_collect_readback_and_new_input_preserve_time_and_never_grant_execution(tmp_path):
    tmp_path.chmod(0o700)
    calls = []

    def reply(request):
        calls.append(request)
        return httpx.Response(200, content=RAW)

    directory = tmp_path / "probe"
    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
        result = await probe.collect(
            directory, symbol="AVGO", key=SecretStr("secret-test"), client=client
        )
        with pytest.raises(FileExistsError):
            await probe.collect(
                directory, symbol="AVGO", key=SecretStr("secret-test"), client=client
            )
    assert len(calls) == 1
    assert "apikey" not in str(calls[0].url)
    assert result["access_status"] == "AVAILABLE"
    old = datetime(2026, 10, 16, tzinfo=UTC)
    read = probe.read_archive(directory, as_of=old)
    assert read["historical_binding_status"] == "FORECAST_RECEIVED_AFTER_CUTOFF"
    with pytest.raises(ValueError, match="FORECAST_RECEIVED_AFTER_CUTOFF"):
        probe.current_context(directory, as_of=old)
    now = datetime.now(UTC) + timedelta(seconds=1)
    context = probe.current_context(directory, as_of=now)
    assert context["status"] == "partial"
    assert context["execution_permitted"] is False
    assert context["historical_binding_status"] == "FORECAST_VINTAGE_UNVERIFIED"
    assert context["items"][0]["epsAvg"] == "5.25"
    assert (directory / "raw.json").read_bytes() == RAW


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [302, 401, 403, 429, 500])
async def test_denied_probe_has_fixed_safe_code_no_retry_or_error_body(tmp_path, status, caplog):
    tmp_path.chmod(0o700)
    calls = []

    def reply(request):
        calls.append(request)
        return httpx.Response(status, content=b"secret-test PRIVATE_ERROR")

    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
        result = await probe.collect(
            tmp_path / "probe", symbol="AVGO", key=SecretStr("secret-test"), client=client
        )
    assert len(calls) == 1
    assert result["http_status"] == status
    assert result["access_status"] == "UNVERIFIED"
    assert not (tmp_path / "probe/raw.json").exists()
    assert "PRIVATE_ERROR" not in json.dumps(result) + caplog.text
    assert "secret-test" not in caplog.text
    assert probe.read_archive(tmp_path / "probe", as_of=datetime.now(UTC))["http_status"] == status


@pytest.mark.parametrize(
    "raw",
    [
        b"[]",
        b"{}",
        b'[{"symbol":"AAPL","date":"2027-01-01","epsAvg":1}]',
        b'[{"symbol":"AVGO","date":"2027-01-01","epsAvg":NaN}]',
        b'[{"symbol":"AVGO","date":"2027-01-01","epsAvg":1,"epsAvg":2}]',
    ],
)
def test_invalid_response_is_not_forecast_evidence(raw):
    with pytest.raises(ValueError, match="FORECAST_PAYLOAD_INVALID"):
        probe.parse_estimates(raw, "AVGO")


@pytest.mark.asyncio
async def test_raw_tamper_is_rejected_on_readback(tmp_path):
    tmp_path.chmod(0o700)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, content=RAW))
    ) as client:
        await probe.collect(
            tmp_path / "probe", symbol="AVGO", key=SecretStr("secret-test"), client=client
        )
    (tmp_path / "probe/raw.json").write_bytes(RAW + b" ")
    with pytest.raises(ValueError, match="FORECAST_ARCHIVE_INVALID"):
        probe.read_archive(tmp_path / "probe", as_of=datetime.now(UTC))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("code", "PRIVATE_PAYLOAD"),
        ("received_at", "PRIVATE_PAYLOAD"),
        ("http_status", "PRIVATE_PAYLOAD"),
        ("execution_permitted", True),
    ],
)
async def test_receipt_cannot_inject_summary_or_permission(tmp_path, field, value):
    tmp_path.chmod(0o700)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(403))
    ) as client:
        await probe.collect(
            tmp_path / "probe", symbol="AVGO", key=SecretStr("secret-test"), client=client
        )
    path = tmp_path / "probe/receipt.json"
    receipt = json.loads(path.read_bytes())
    receipt[field] = value
    path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="FORECAST_ARCHIVE_INVALID"):
        probe.read_archive(tmp_path / "probe", as_of=datetime.now(UTC))


def test_normal_cli_claim_read_and_no_second_request(tmp_path, monkeypatch, capsys):
    (tmp_path / ".secrets").mkdir(mode=0o700)
    monkeypatch.setattr(probe, "load_local_environment", lambda *a: {"FMP_API_KEY": "secret-test"})
    original = probe.collect
    calls = []

    async def collect(directory, *, symbol, key):
        def reply(request):
            calls.append(request)
            return httpx.Response(200, content=RAW)

        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
            return await original(directory, symbol=symbol, key=key, client=client)

    monkeypatch.setattr(probe, "collect", collect)
    args = [
        "forecast",
        "probe",
        "--repo-root",
        str(tmp_path),
        "--symbol",
        "AVGO",
        "--as-of",
        "2026-10-16T14:00:00+00:00",
    ]
    monkeypatch.setattr(sys, "argv", args)
    assert probe.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["historical_binding_status"] == "FORECAST_RECEIVED_AFTER_CUTOFF"
    assert probe.main() == 1
    assert json.loads(capsys.readouterr().out)["code"] == "FORECAST_ALREADY_ATTEMPTED"
    assert len(calls) == 1
    directory = next((tmp_path / ".secrets/nexus-forecast-probe").iterdir())
    monkeypatch.setattr(
        sys,
        "argv",
        ["forecast", "read", "--directory", str(directory), "--as-of", "2026-10-16T14:00:00+00:00"],
    )
    assert probe.main() == 0
    assert json.loads(capsys.readouterr().out) == result
    assert len(calls) == 1
