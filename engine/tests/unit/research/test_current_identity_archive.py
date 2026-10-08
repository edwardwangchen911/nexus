import importlib
import json
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256

import pytest


def originals(received):
    directory = json.dumps(
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[123456, "Synthetic issuer", "AAPL", "NYSE"]],
        }
    ).encode()
    submissions = json.dumps({"cik": "123456", "tickers": ["AAPL"], "exchanges": ["NYSE"]}).encode()

    def receipt(raw, url):
        return json.dumps(
            {
                "schema": "sec_identity_source_receipt_v1",
                "source_url": url,
                "http_status": 200,
                "received_at": received.isoformat(),
                "sha256": sha256(raw).hexdigest(),
                "byte_length": len(raw),
            }
        ).encode()

    return (
        directory,
        receipt(directory, "https://www.sec.gov/files/company_tickers_exchange.json"),
        submissions,
        receipt(submissions, "https://data.sec.gov/submissions/CIK0000123456.json"),
    )


def check(contents, decision, session=None):
    module = importlib.import_module("nexus.research.current_identity_archive")
    return module.validate_identity_source_originals(
        contents=contents,
        symbol="AAPL",
        cik="0000123456",
        exchange="XNYS",
        decision_time=decision,
        decision_session=session,
    )


@pytest.mark.parametrize(
    "decision,received",
    [
        (datetime(2026, 1, 8, 14, tzinfo=UTC), datetime(2026, 1, 8, 10, tzinfo=UTC)),
        (datetime(2026, 3, 8, 8, tzinfo=UTC), datetime(2026, 3, 8, 5, tzinfo=UTC)),
        (datetime(2026, 3, 9, 4, tzinfo=UTC), datetime(2026, 3, 9, 4, tzinfo=UTC)),
        (datetime(2026, 1, 9, 4, tzinfo=UTC), datetime(2026, 1, 8, 10, tzinfo=UTC)),
    ],
)
def test_original_decision_ny_day_applies_including_premarket_dst_and_next_utc_day(
    decision, received
):
    result = check(originals(received), decision)
    assert len(result.originals) == 4
    assert result.available_at == received
    assert result.cik == "0000123456"
    assert len(result.policy_hash) == 64


@pytest.mark.parametrize("fault", ["yesterday", "future", "hash", "cik", "session", "bool_size"])
def test_unusable_originals_do_not_become_current_binding(fault):
    decision = datetime(2026, 1, 8, 14, tzinfo=UTC)
    received = decision - timedelta(hours=1)
    if fault == "yesterday":
        received -= timedelta(days=1)
    if fault == "future":
        received = decision + timedelta(seconds=1)
    data = list(originals(received))
    if fault == "hash":
        data[0] += b" "
    if fault == "cik":
        data = list(originals(received))
        receipt = json.loads(data[3])
        document = json.loads(data[2])
        document["cik"] = "42"
        data[2] = json.dumps(document).encode()
        receipt.update(sha256=sha256(data[2]).hexdigest(), byte_length=len(data[2]))
        data[3] = json.dumps(receipt).encode()
    if fault == "bool_size":
        receipt = json.loads(data[1])
        receipt["byte_length"] = True
        data[1] = json.dumps(receipt).encode()
    with pytest.raises(ValueError, match="RESEARCH_IDENTITY_SOURCE_INVALID"):
        check(tuple(data), decision, date(2026, 1, 7) if fault == "session" else None)


def test_loader_rejects_symlink_original(tmp_path):
    module = importlib.import_module("nexus.research.current_identity_archive")
    decision = datetime(2026, 1, 8, 14, tzinfo=UTC)
    for name, raw in zip(
        (
            "directory.json",
            "directory-receipt.json",
            "AAPL-submissions.json",
            "AAPL-submissions-receipt.json",
        ),
        originals(decision),
        strict=True,
    ):
        path = tmp_path / name
        path.write_bytes(raw)
        path.chmod(0o600)
    result = tmp_path / "result.json"
    result.write_text(
        json.dumps(
            {
                "status": "archived",
                "requests": 2,
                "authority_registered": False,
                "execution_locked": True,
            }
        )
    )
    result.chmod(0o600)
    saved = tmp_path / "directory.json"
    target = tmp_path.parent / (tmp_path.name + "-target")
    saved.rename(target)
    saved.symlink_to(target)
    with pytest.raises(ValueError, match="RESEARCH_IDENTITY_ARCHIVE_INVALID"):
        module.load_identity_source_archive(
            tmp_path, symbol="AAPL", cik="0000123456", exchange="XNYS", decision_time=decision
        )
