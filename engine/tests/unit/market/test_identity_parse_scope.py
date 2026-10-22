"""Repeated readback must recheck originals without reparsing identical SEC HTML."""

from datetime import timedelta
from unittest.mock import patch

import pytest
from helpers.current_identity import archives, filing, profile
from nexus.market import current_identity as identity


def parse():
    return identity.parse_current_common_stock(symbol="AAPL", fmp_raw=profile(), sec_raw=filing())


def test_scope_reuses_only_successful_exact_inputs_and_does_not_leak():
    with patch.object(
        identity._Cover, "feed", autospec=True, side_effect=identity._Cover.feed
    ) as feed:
        with identity.identity_parse_scope():
            first = parse()
            first.context["changed"] = "caller mutation"
            assert "changed" not in parse().context
            assert feed.call_count == 1
            # Changed raw bytes must still be parsed and rejected.
            for _ in range(2):
                with pytest.raises(ValueError):
                    identity.parse_current_common_stock(
                        symbol="AAPL", fmp_raw=profile(), sec_raw=filing() + b"<html>"
                    )
            assert feed.call_count == 3
        parse()
        assert feed.call_count == 4


def test_scope_restores_on_exception_and_separates_nested_reads():
    with patch.object(
        identity._Cover, "feed", autospec=True, side_effect=identity._Cover.feed
    ) as feed:
        with identity.identity_parse_scope():
            parse()
            with pytest.raises(RuntimeError), identity.identity_parse_scope():
                parse()
                raise RuntimeError("read failed")
            parse()
            assert feed.call_count == 2
        parse()
        assert feed.call_count == 3


def test_cache_does_not_skip_receipt_time_or_raw_profile_validation(tmp_path):
    fm, sec = archives(tmp_path)
    # Use the normal archive validator; even an exact parser cache hit cannot
    # authorize a receipt that had not arrived at the caller's cutoff.
    inputs = identity.load_identity_archives(symbol="AAPL", fmp_archive=fm, sec_archive=sec)
    with identity.identity_parse_scope():
        identity.validate_identity_originals(
            symbol="AAPL", contents=inputs.contents, now=max(inputs.received)
        )
        with pytest.raises(ValueError, match="CURRENT_IDENTITY_ARCHIVE_INVALID"):
            identity.validate_identity_originals(
                symbol="AAPL",
                contents=inputs.contents,
                now=min(inputs.received) - timedelta(seconds=1),
            )
        with pytest.raises(ValueError):
            identity.parse_current_common_stock(
                symbol="AAPL", fmp_raw=profile(isEtf=True), sec_raw=filing()
            )
        with pytest.raises(ValueError):
            identity.parse_current_common_stock(symbol="AVGO", fmp_raw=profile(), sec_raw=filing())


def test_custom_policy_does_not_reuse_default_policy_result():
    with patch.object(
        identity._Cover, "feed", autospec=True, side_effect=identity._Cover.feed
    ) as feed:
        with identity.identity_parse_scope():
            parse()
            for _ in range(2):
                identity.parse_current_common_stock(
                    symbol="AAPL",
                    fmp_raw=profile(),
                    sec_raw=filing(),
                    _allow_outside_definition=True,
                )
            parse()
            assert feed.call_count == 3


@pytest.mark.asyncio
async def test_interleaved_requests_have_independent_parse_scopes():
    import asyncio

    first_ready, second_ready = asyncio.Event(), asyncio.Event()

    async def first():
        with identity.identity_parse_scope():
            parse()
            first_ready.set()
            await second_ready.wait()
            parse()

    async def second():
        await first_ready.wait()
        with identity.identity_parse_scope():
            parse()
            second_ready.set()
            parse()

    with patch.object(
        identity._Cover, "feed", autospec=True, side_effect=identity._Cover.feed
    ) as feed:
        await asyncio.gather(first(), second())
        assert feed.call_count == 2
