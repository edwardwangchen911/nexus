import importlib
import json
import stat
from hashlib import sha256

import httpx
import pytest


def payloads():
    return {
        "/files/company_tickers_exchange.json": {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[320193, "Synthetic issuer", "AAPL", "Nasdaq"]],
        },
        "/submissions/CIK0000320193.json": {
            "cik": "320193",
            "tickers": ["AAPL"],
            "exchanges": ["Nasdaq"],
        },
    }


@pytest.mark.asyncio
async def test_collect_archives_exact_originals_and_receipts_without_authority(tmp_path):
    module = importlib.import_module("nexus.ops.research_identity_local")
    seen = []
    bodies = payloads()

    def respond(request):
        seen.append(request.url.path)
        return httpx.Response(200, json=bodies[request.url.path])

    out = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await module.collect_identity_sources(
            symbols=("AAPL",), contact="test@example.invalid", output_dir=out, client=client
        )
    assert result["status"] == "archived"
    assert result["authority_registered"] is False
    assert result["requests"] == 2
    assert seen == list(bodies)
    assert stat.S_IMODE(out.stat().st_mode) == 0o700
    for name in ("directory", "AAPL-submissions"):
        raw = (out / f"{name}.json").read_bytes()
        receipt = json.loads((out / f"{name}-receipt.json").read_bytes())
        assert receipt["sha256"] == sha256(raw).hexdigest()
        assert receipt["byte_length"] == len(raw)
        assert receipt["received_at"]
        assert stat.S_IMODE((out / f"{name}.json").stat().st_mode) == 0o600
    with pytest.raises(ValueError, match="IDENTITY_COLLECTION_DIRECTORY_INVALID"):
        await module.collect_identity_sources(
            symbols=("AAPL",), contact="test@example.invalid", output_dir=out
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["duplicate_ticker", "wrong_cik", "duplicate_submission_ticker", "http_failure"]
)
async def test_collection_failure_preserves_archive_and_never_retries(tmp_path, fault):
    module = importlib.import_module("nexus.ops.research_identity_local")
    bodies = payloads()
    if fault == "duplicate_ticker":
        bodies[next(iter(bodies))]["data"] *= 2
    elif fault == "wrong_cik":
        bodies["/submissions/CIK0000320193.json"]["cik"] = "42"
    elif fault == "duplicate_submission_ticker":
        bodies["/submissions/CIK0000320193.json"]["tickers"] *= 2
        bodies["/submissions/CIK0000320193.json"]["exchanges"] *= 2
    seen = []

    def respond(request):
        seen.append(request.url.path)
        return httpx.Response(
            503 if fault == "http_failure" else 200, json=bodies[request.url.path]
        )

    out = tmp_path / "new"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError, match="IDENTITY_COLLECTION_FAILED"):
            await module.collect_identity_sources(
                symbols=("AAPL",), contact="test@example.invalid", output_dir=out, client=client
            )
    failure = json.loads((out / "result.json").read_bytes())
    assert failure["status"] == "unavailable"
    assert failure["authority_registered"] is False
    assert len(seen) == (2 if fault in {"wrong_cik", "duplicate_submission_ticker"} else 1)
    assert "test@example.invalid" not in json.dumps(failure)


def test_duplicate_json_fields_cannot_shadow_issuer():
    module = importlib.import_module("nexus.ops.research_identity_local")
    with pytest.raises(ValueError, match="IDENTITY_COLLECTION_JSON_INVALID"):
        module._document(b'{"cik": 42, "cik": 320193}')


@pytest.mark.asyncio
async def test_shared_parent_permissions_rejected_before_any_request(tmp_path):
    module = importlib.import_module("nexus.ops.research_identity_local")
    tmp_path.chmod(0o755)
    with pytest.raises(ValueError, match="IDENTITY_COLLECTION_DIRECTORY_INVALID"):
        await module.collect_identity_sources(
            symbols=("AAPL",), contact="test@example.invalid", output_dir=tmp_path / "new"
        )
    assert not (tmp_path / "new").exists()


@pytest.mark.asyncio
async def test_reflected_contact_never_enters_archive(tmp_path):
    module = importlib.import_module("nexus.ops.research_identity_local")
    out = tmp_path / "new"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text="test@example.invalid")
        )
    ) as client:
        with pytest.raises(ValueError, match="IDENTITY_COLLECTION_FAILED"):
            await module.collect_identity_sources(
                symbols=("AAPL",), contact="test@example.invalid", output_dir=out, client=client
            )
    assert not (out / "directory.json").exists()
    assert "test@example.invalid" not in (out / "result.json").read_text()
