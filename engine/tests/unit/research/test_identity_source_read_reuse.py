"""One read may reuse pure bindings, never receipt, cutoff or current bytes."""

from datetime import UTC, datetime, timedelta

import pytest
from nexus.research import current_identity_archive, identity_sources
from unit.research.test_current_identity_archive import check, originals


def test_same_read_reuses_mapping_but_checks_every_receipt_and_cutoff(monkeypatch):
    calls = []
    native = identity_sources.parse_identity_source

    def counted(raw):
        value = native(raw)
        calls.append(
            "directory"
            if "fields" in value
            else "receipt"
            if "http_status" in value
            else "submissions"
        )
        return value

    monkeypatch.setattr(identity_sources, "parse_identity_source", counted)
    monkeypatch.setattr(current_identity_archive, "parse_identity_source", counted)
    now = datetime(2026, 1, 8, 14, tzinfo=UTC)
    data = originals(now)
    with identity_sources.identity_source_parse_scope():
        first = check(data, now)
        assert check(data, now) == first
        assert calls.count("directory") == 1
        assert calls.count("submissions") == 1
        assert calls.count("receipt") == 4
        with pytest.raises(ValueError, match="RESEARCH_IDENTITY_SOURCE_INVALID"):
            check(data, now - timedelta(microseconds=1))
        changed = (
            data[0],
            data[1].replace(b'"http_status": 200', b'"http_status": 201'),
            *data[2:],
        )
        with pytest.raises(ValueError, match="RESEARCH_IDENTITY_SOURCE_INVALID"):
            check(changed, now)
        with pytest.raises(ValueError, match="RESEARCH_IDENTITY_SOURCE_INVALID"):
            check((data[0] + b" ", *data[1:]), now)
    with identity_sources.identity_source_parse_scope():
        assert check(data, now) == first
    assert calls.count("directory") == 2
