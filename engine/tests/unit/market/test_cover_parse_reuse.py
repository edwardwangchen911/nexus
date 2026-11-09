"""Cross-refresh parsing must never carry identity or source authority."""

import pytest
from helpers.current_identity import filing, profile
from nexus.market import current_identity as identity


def test_cover_syntax_reused_but_identity_policy_runs_for_each_scope(monkeypatch):
    calls = []
    feed = identity._Cover.feed
    monkeypatch.setattr(
        identity._Cover, "feed", lambda self, raw: (calls.append(raw), feed(self, raw))[1]
    )
    raw = filing().replace(b"<html ", b'<html data-cache-case="policy" ')
    for _ in range(2):
        with identity.identity_parse_scope(reuse_cover_parses=True):
            assert (
                identity.parse_current_common_stock(
                    symbol="AAPL", fmp_raw=profile(), sec_raw=raw
                ).cik
                == "0000123456"
            )
    assert len(calls) == 1
    monkeypatch.setattr(
        identity,
        "member_for",
        lambda symbol: (_ for _ in ()).throw(ValueError("CURRENT_IDENTITY_SCOPE_INVALID")),
    )
    with (
        identity.identity_parse_scope(reuse_cover_parses=True),
        pytest.raises(ValueError, match="CURRENT_IDENTITY_"),
    ):
        identity.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)
    assert len(calls) == 1


def test_cover_reuse_is_opt_in_and_returns_independent_trees():
    raw = filing().replace(b"<html ", b'<html data-cache-case="mutation" ')
    with identity.identity_parse_scope(reuse_cover_parses=True):
        first = identity._parse_sec_cover(raw)
        first.facts.clear()
    with identity.identity_parse_scope(reuse_cover_parses=True):
        assert identity._parse_sec_cover(raw).facts
    with identity.identity_parse_scope(reuse_cover_parses=True), pytest.raises(ValueError):
        identity._parse_sec_cover(raw[:-10])


def test_default_and_custom_policy_paths_do_not_use_shared_parses(monkeypatch):
    calls = []
    feed = identity._Cover.feed
    monkeypatch.setattr(
        identity._Cover, "feed", lambda self, raw: (calls.append(raw), feed(self, raw))[1]
    )
    raw = filing().replace(b"<html ", b'<html data-cache-case="opt-in" ')
    with identity.identity_parse_scope(reuse_cover_parses=True):
        identity._parse_sec_cover(raw)
    with identity.identity_parse_scope():
        identity._parse_sec_cover(raw)
    with identity.identity_parse_scope(reuse_cover_parses=True):
        identity._parse_sec_cover(raw, use_cache=False)
    assert len(calls) == 3


@pytest.mark.parametrize("byte_limit,entries", [(1, 4), (16 * 1024 * 1024, 1)])
def test_shared_parses_are_bounded_and_evicted(monkeypatch, byte_limit, entries):
    from collections import OrderedDict

    monkeypatch.setattr(identity, "_COVER_PARSES", OrderedDict())
    monkeypatch.setattr(identity, "_COVER_PARSE_SIZE", 0)
    monkeypatch.setattr(identity, "_COVER_PARSE_BYTES", byte_limit)
    monkeypatch.setattr(identity, "_COVER_PARSE_ENTRIES", entries)
    calls = []
    feed = identity._Cover.feed
    monkeypatch.setattr(
        identity._Cover, "feed", lambda self, raw: (calls.append(raw), feed(self, raw))[1]
    )
    for label in (b"a", b"b", b"a"):
        raw = filing().replace(b"<html ", b'<html data-cache-case="' + label + b'" ')
        with identity.identity_parse_scope(reuse_cover_parses=True):
            identity._parse_sec_cover(raw)
    assert len(calls) == 3
    assert len(identity._COVER_PARSES) <= entries
    assert identity._COVER_PARSE_SIZE <= byte_limit


def test_warm_cover_does_not_authorize_changed_profile():
    raw = filing().replace(b"<html ", b'<html data-cache-case="profile" ')
    with identity.identity_parse_scope(reuse_cover_parses=True):
        identity.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=raw)
    changed = profile().replace(b"USD", b"EUR")
    assert changed != profile()
    with identity.identity_parse_scope(reuse_cover_parses=True), pytest.raises(ValueError):
        identity.parse_current_common_stock(symbol="AAPL", fmp_raw=changed, sec_raw=raw)
