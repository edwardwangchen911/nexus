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
