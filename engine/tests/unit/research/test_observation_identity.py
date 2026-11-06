"""Historical observations preserve original timing without claiming current identity."""

import importlib
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest
from unit.research.test_current_identity_archive import originals


def validate(data, cutoff):
    module = importlib.import_module("nexus.research.observation_identity")
    return module.validate_observation_source_originals(
        contents=tuple(data), symbol="AAPL", cik="0000123456", exchange="XNYS", as_of=cutoff
    )


@pytest.mark.parametrize("age", [1, 30, 400])
def test_historical_receipts_remain_observations_without_ttl(age):
    cutoff = datetime(2026, 1, 8, 14, tzinfo=UTC)
    received = cutoff - timedelta(days=age)
    result = validate(originals(received), cutoff)
    assert result.available_at == received
    assert result.policy_id == "research_observation_identity_v1"
    assert result.remote_latest_status == "unverified"
    assert result.source_effective_at is None


@pytest.mark.parametrize("fault", ["future", "hash", "url", "length", "cik", "duplicate"])
def test_observation_rejects_future_or_broken_originals(fault):
    cutoff = datetime(2026, 1, 8, 14, tzinfo=UTC)
    data = list(originals(cutoff + timedelta(seconds=1) if fault == "future" else cutoff))
    if fault == "hash":
        data[0] += b" "
    if fault in {"url", "length"}:
        receipt = json.loads(data[1])
        receipt["source_url" if fault == "url" else "byte_length"] = (
            "https://other.test" if fault == "url" else True
        )
        data[1] = json.dumps(receipt).encode()
    if fault in {"cik", "duplicate"}:
        raw = json.loads(data[0])
        if fault == "cik":
            raw["data"][0][0] = 42
        else:
            raw["data"].append(raw["data"][0])
        data[0] = json.dumps(raw).encode()
        receipt = json.loads(data[1])
        receipt.update(sha256=sha256(data[0]).hexdigest(), byte_length=len(data[0]))
        data[1] = json.dumps(receipt).encode()
    with pytest.raises(ValueError, match="RESEARCH_OBSERVATION_SOURCE_INVALID"):
        validate(data, cutoff)


def test_mapping_parse_scope_reuses_only_exact_content_and_never_receipt_authority(monkeypatch):
    from unittest.mock import Mock

    from nexus.research import identity_sources as sources

    cutoff = datetime(2026, 1, 8, 14, tzinfo=UTC)
    data = list(originals(cutoff))
    parser = Mock(wraps=sources.parse_identity_source)
    monkeypatch.setattr(sources, "parse_identity_source", parser)
    with sources.identity_source_parse_scope():
        for _ in range(3):
            assert sources.directory_binding_from_raw(data[0], "AAPL") == ("0000123456", "XNYS")
            sources.require_submissions_from_raw(data[2], "AAPL", "0000123456", "XNYS")
        assert parser.call_count == 2
        # Full reader still checks receipt hashes and cutoff on every invocation.
        assert validate(data, cutoff).available_at == cutoff
        with pytest.raises(ValueError):
            validate(data, cutoff - timedelta(seconds=1))
        with pytest.raises(ValueError):
            sources.directory_binding_from_raw(data[0], "NVDA")
        with pytest.raises(ValueError):
            sources.require_submissions_from_raw(data[2], "AAPL", "0000999999", "XNYS")
        malformed = b'{"data":[],"data":[]}'
        before = parser.call_count
        for _ in range(2):
            with pytest.raises(ValueError):
                sources.directory_binding_from_raw(malformed, "AAPL")
        assert parser.call_count == before + 2
    before = parser.call_count
    sources.directory_binding_from_raw(data[0], "AAPL")
    assert parser.call_count == before + 1


def test_mapping_parse_scope_is_bounded_and_cleared_after_exception(monkeypatch):
    from unittest.mock import Mock

    from nexus.research import identity_sources as sources

    raw = originals(datetime(2026, 1, 8, 14, tzinfo=UTC))[0]
    parser = Mock(wraps=sources.parse_identity_source)
    monkeypatch.setattr(sources, "parse_identity_source", parser)
    with pytest.raises(RuntimeError), sources.identity_source_parse_scope():
        for count in range(129):
            sources.directory_binding_from_raw(raw + b" " * count, "AAPL")
        before = parser.call_count
        sources.directory_binding_from_raw(raw + b" " * 128, "AAPL")
        assert parser.call_count == before + 1
        raise RuntimeError("end read")
    before = parser.call_count
    with sources.identity_source_parse_scope():
        sources.directory_binding_from_raw(raw, "AAPL")
    assert parser.call_count == before + 1


@pytest.mark.asyncio
async def test_mapping_parse_scope_cannot_escape_in_an_inherited_task(monkeypatch):
    import asyncio
    from unittest.mock import Mock

    from nexus.research import identity_sources as sources

    raw = originals(datetime(2026, 1, 8, 14, tzinfo=UTC))[0]
    parser = Mock(wraps=sources.parse_identity_source)
    monkeypatch.setattr(sources, "parse_identity_source", parser)
    released = asyncio.Event()

    async def inherited():
        await released.wait()
        sources.directory_binding_from_raw(raw, "AAPL")

    with sources.identity_source_parse_scope():
        sources.directory_binding_from_raw(raw, "AAPL")
        task = asyncio.create_task(inherited())
    released.set()
    await task
    assert parser.call_count == 2
