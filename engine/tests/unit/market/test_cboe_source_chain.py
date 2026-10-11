import importlib
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest
from helpers.cboe_source_chain import originals
from helpers.iso_mic import NOW


def validate(contents=None, now=NOW):
    module = importlib.import_module("nexus.market.cboe_source_chain")
    return module.validate_cboe_source_chain(
        symbol="TEST", contents=contents or originals(), now=now
    )


def test_complete_chain_retains_exact_class_mic_and_current_listing_without_authorizing_identity():
    result = validate()
    assert result.submission.class_id == "C000123456"
    assert result.mic.mic == "BATS" and result.listing.symbol == "TEST"
    assert result.submission.class_name == "Synthetic Daily 2X ETF"
    assert result.listing.product_name == "Synthetic Daily 2x ETF"
    assert result.identity_registration_ready is False
    assert result.execution_locked is True
    assert len(result.contents) == 14
    assert validate().revision == result.revision


@pytest.mark.parametrize("pair", range(7))
def test_any_original_byte_change_without_matching_receipt_rejected(pair):
    values = list(originals())
    values[2 * pair] += b" "
    with pytest.raises(ValueError, match="CBOE_SOURCE_CHAIN_INVALID"):
        validate(tuple(values))


@pytest.mark.parametrize(
    "change", ["source", "future", "stale", "extra", "duplicate", "boolean_status"]
)
def test_receipts_reject_wrong_sources_times_and_schema(change):
    values = list(originals())
    receipt = json.loads(values[9])
    if change == "source":
        receipt["source_url"] = "https://example.com/listed"
    elif change == "future":
        receipt["received_at"] = (NOW + timedelta(minutes=1)).isoformat()
    elif change == "stale":
        receipt["received_at"] = (NOW - timedelta(days=2)).isoformat()
    elif change == "extra":
        receipt["approved"] = True
    elif change == "boolean_status":
        receipt["http_status"] = True
    values[9] = json.dumps(receipt).encode()
    if change == "duplicate":
        values[9] = values[9][:-1] + b',"http_status":200}'
    with pytest.raises(ValueError, match="CBOE_SOURCE_CHAIN_INVALID"):
        validate(tuple(values))


def test_old_pair_does_not_become_a_new_observation_by_using_a_new_receipt_on_other_source():
    values = list(originals())
    receipt = json.loads(values[7])
    receipt["received_at"] = (NOW - timedelta(hours=3)).isoformat()
    values[7] = json.dumps(receipt).encode()
    with pytest.raises(ValueError, match="CBOE_SOURCE_CHAIN_INVALID"):
        validate(tuple(values))


def test_later_prospectus_amendment_is_not_rejected_by_original_listing_date():
    values = list(originals())
    values[2] = values[2].replace(b"20260422", b"20260425")
    receipt = json.loads(values[3])
    receipt["sha256"] = sha256(values[2]).hexdigest()
    receipt["byte_length"] = len(values[2])
    values[3] = json.dumps(receipt).encode()
    result = validate(tuple(values))
    assert result.listing.listed_on < result.submission.filed_on
    assert result.identity_registration_ready is False


@pytest.mark.parametrize(
    "change",
    [
        {"isEtf": False},
        {"currency": "HKD"},
        {"exchange": "NYSE"},
        {"cik": "0000654321"},
        {"isAdr": True},
    ],
)
def test_conflicting_provider_identity_is_rejected_even_with_matching_receipt(change):
    values = list(originals())
    profile = json.loads(values[0])
    profile[0].update(change)
    values[0] = json.dumps(profile).encode()
    receipt = json.loads(values[1])
    receipt["raw_sha256"] = sha256(values[0]).hexdigest()
    receipt["raw_byte_length"] = len(values[0])
    values[1] = json.dumps(receipt).encode()
    with pytest.raises(ValueError, match="CBOE_SOURCE_CHAIN_INVALID"):
        validate(tuple(values))


def next_publication_contents():
    now = datetime(2026, 10, 13, 14, tzinfo=UTC)
    values = list(originals())
    previous = values[4:8]
    values[4] = (
        values[4]
        .replace(b"14 September 2026", b"12 October 2026")
        .replace(b"28 September 2026", b"26 October 2026")
        .replace(b"<td>12 October 2026</td></tr>", b"<td>09 November 2026</td></tr>")
    )
    values[8] = values[8].replace(b"2026-10-10", b"2026-10-12")
    for index in range(7):
        receipt = json.loads(values[index * 2 + 1])
        receipt["received_at"] = (now - timedelta(minutes=1)).isoformat()
        prefix = "raw_" if index == 0 else ""
        receipt[prefix + "sha256"] = sha256(values[index * 2]).hexdigest()
        receipt[prefix + "byte_length"] = len(values[index * 2])
        values[index * 2 + 1] = json.dumps(receipt).encode()
    return tuple(values + previous), now


def test_future_implementation_requires_bound_previous_effective_originals():
    contents, now = next_publication_contents()
    result = validate(contents, now=now)
    assert result.mic.applicability_basis == "unchanged_from_previous_effective_dataset"
    assert len(result.contents) == len(result.received) == 18
    assert result.identity_registration_ready is False


def test_previous_original_receipt_mismatch_is_not_a_fallback():
    contents, now = next_publication_contents()
    values = list(contents)
    values[16] += b" "
    with pytest.raises(ValueError, match="CBOE_SOURCE_CHAIN_INVALID"):
        validate(tuple(values), now=now)


def test_sec_original_cannot_have_been_received_before_acceptance():
    values = list(originals())
    receipt = json.loads(values[3])
    receipt["received_at"] = "2026-04-22T21:08:14+00:00"
    values[3] = json.dumps(receipt).encode()
    with pytest.raises(ValueError, match="CBOE_SOURCE_CHAIN_INVALID"):
        validate(tuple(values))


def test_sec_original_received_at_acceptance_keeps_its_historical_capture_time():
    values = list(originals())
    receipt = json.loads(values[3])
    receipt["received_at"] = "2026-04-22T21:08:15+00:00"
    values[3] = json.dumps(receipt).encode()
    result = validate(tuple(values))
    assert result.received[2].isoformat() == "2026-04-22T21:08:15+00:00"
