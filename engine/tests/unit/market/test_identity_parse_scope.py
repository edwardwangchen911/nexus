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


def test_one_read_hashes_each_identical_immutable_original_once():
    fmp, sec = profile(), filing()
    with patch.object(identity, "sha256", wraps=identity.sha256) as digest:
        with identity.identity_parse_scope():
            first = identity.parse_current_common_stock(symbol="AAPL", fmp_raw=fmp, sec_raw=sec)
            second = identity.parse_current_common_stock(symbol="AAPL", fmp_raw=fmp, sec_raw=sec)
            assert first == second
            assert digest.call_count == 2
        identity.parse_current_common_stock(symbol="AAPL", fmp_raw=fmp, sec_raw=sec)
        assert digest.call_count > 2


def test_original_digest_keeps_mutable_buffers_and_subclasses_uncached():
    import hashlib

    class Original(bytes):
        pass

    mutable = bytearray(b"abc")
    derived = Original(b"abc")
    with patch.object(identity, "sha256", wraps=identity.sha256) as digest:
        with identity.identity_parse_scope():
            assert identity.identity_original_digest(mutable) == hashlib.sha256(b"abc").hexdigest()
            mutable[0] = ord("z")
            assert identity.identity_original_digest(mutable) == hashlib.sha256(b"zbc").hexdigest()
            identity.identity_original_digest(derived)
            identity.identity_original_digest(derived)
            assert digest.call_count == 4


@pytest.mark.parametrize("entries,byte_limit", [(0, 16), (4, 2)])
def test_original_digest_falls_back_when_read_retention_is_full(monkeypatch, entries, byte_limit):
    monkeypatch.setattr(identity, "_ORIGINAL_DIGEST_ENTRIES", entries)
    monkeypatch.setattr(identity, "_ORIGINAL_DIGEST_BYTES", byte_limit)
    with patch.object(identity, "sha256", wraps=identity.sha256) as digest:
        with identity.identity_parse_scope():
            first = identity.identity_original_digest(b"abc")
            assert identity.identity_original_digest(b"abc") == first
            assert digest.call_count == 2


def test_original_digest_restores_nested_reads_and_clears_inherited_scope():
    from contextvars import copy_context

    original = b"original"
    with patch.object(identity, "sha256", wraps=identity.sha256) as digest:
        with identity.identity_parse_scope():
            identity.identity_original_digest(original)
            with pytest.raises(RuntimeError), identity.identity_parse_scope():
                identity.identity_original_digest(original)
                raise RuntimeError("cancelled read")
            identity.identity_original_digest(original)
            with identity.identity_parse_scope(reuse_active=True):
                identity.identity_original_digest(original)
            assert digest.call_count == 2
            inherited = copy_context()
            scope = identity._PARSE_CACHE.get()
        assert not scope.original_digests and scope.original_bytes == 0
        inherited.run(identity.identity_original_digest, original)
        inherited.run(identity.identity_original_digest, original)
        assert digest.call_count == 4


def test_distinct_original_instances_with_equal_bytes_keep_correct_digests():
    import hashlib

    first, second = b"abc", bytes(bytearray(b"abc"))
    assert first is not second
    with identity.identity_parse_scope():
        assert identity.identity_original_digest(first) == hashlib.sha256(b"abc").hexdigest()
        assert identity.identity_original_digest(second) == hashlib.sha256(b"abc").hexdigest()
        assert identity.identity_original_digest(b"abd") == hashlib.sha256(b"abd").hexdigest()


def test_original_digest_entry_limit_keeps_existing_originals_and_rehashes_overflow(monkeypatch):
    monkeypatch.setattr(identity, "_ORIGINAL_DIGEST_ENTRIES", 1)
    with patch.object(identity, "sha256", wraps=identity.sha256) as digest:
        with identity.identity_parse_scope():
            for raw in (b"abc", b"def", b"abc", b"def"):
                identity.identity_original_digest(raw)
            assert digest.call_count == 3


@pytest.mark.asyncio
async def test_inherited_inflight_hash_cannot_refill_an_exited_read():
    import asyncio
    import threading

    started, release = threading.Event(), threading.Event()
    native = identity.sha256

    def hashing(raw):
        started.set()
        assert release.wait(timeout=5)
        return native(raw)

    with patch.object(identity, "sha256", side_effect=hashing):
        with identity.identity_parse_scope():
            scope = identity._PARSE_CACHE.get()
            task = asyncio.create_task(asyncio.to_thread(identity.identity_original_digest, b"abc"))
            assert await asyncio.to_thread(started.wait, 5)
        release.set()
        assert await task == native(b"abc").hexdigest()
        assert not scope.original_digests and scope.original_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("research", [False, True])
async def test_warm_digest_does_not_authorize_a_changed_expected_binding(research):
    import hashlib
    from datetime import UTC, datetime

    from nexus.market import current_identity_storage as market_storage
    from nexus.persistence.models.data import DataArtifact
    from nexus.research import current_identity_storage as research_storage

    raw = b"abc"
    as_of = datetime(2026, 1, 1, tzinfo=UTC)
    binding = market_storage.ArtifactBinding(
        uri="s3://archive/raw/original?versionId=one",
        sha256=hashlib.sha256(raw).hexdigest(),
        byte_size=len(raw),
        source="sec",
        schema_version="test",
    )
    record = DataArtifact(
        **binding.model_dump(),
        mime_type="application/json",
        created_at=as_of,
        available_at=as_of,
        observed_at=as_of,
        received_at=as_of,
    )

    class Store:
        async def read_raw(self, key, *, uri):
            return raw

    async def read(expected):
        if research:
            return await research_storage._read_registered_artifact(
                Store(), record, expected, as_of
            )
        return await market_storage._read_artifact(Store(), expected)

    with identity.identity_parse_scope():
        assert await read(binding) == raw
        changed = binding.model_copy(update={"sha256": "0" * 64})
        record.sha256 = changed.sha256
        with pytest.raises(ValueError, match="IDENTITY_ARTIFACT_INVALID"):
            await read(changed)
        record.sha256 = binding.sha256
        assert await read(binding) == raw
        if research:
            record.created_at = as_of + timedelta(seconds=1)
            with pytest.raises(ValueError, match="RESEARCH_IDENTITY_ARTIFACT_INVALID"):
                await read(binding)


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


def test_exited_scope_cannot_be_refilled_by_inherited_context():
    from contextvars import copy_context

    with patch.object(
        identity._Cover, "feed", autospec=True, side_effect=identity._Cover.feed
    ) as feed:
        with identity.identity_parse_scope():
            parse()
            inherited = copy_context()
        inherited.run(parse)
        inherited.run(parse)
        assert feed.call_count == 3


def test_same_sec_original_is_parsed_once_across_distinct_profiles():
    with patch.object(
        identity._Cover, "feed", autospec=True, side_effect=identity._Cover.feed
    ) as feed:
        with identity.identity_parse_scope():
            first = parse()
            second = identity.parse_current_common_stock(
                symbol="AAPL", fmp_raw=profile(price=123), sec_raw=filing()
            )
            assert first == second
            with pytest.raises(ValueError):
                identity.parse_current_common_stock(
                    symbol="AAPL", fmp_raw=profile(exchange="NASDAQ"), sec_raw=filing()
                )
            assert feed.call_count == 1
        parse()
        assert feed.call_count == 2


def test_resolver_cannot_mutate_shared_sec_cover():
    with identity.identity_parse_scope():
        parser = identity._parse_sec_cover(filing())
        parser.namespaces.clear()
        parser.facts.clear()
        assert parse().symbol == "AAPL"


def test_summary_can_explicitly_reuse_parent_parse_without_changing_default_isolation():
    with patch.object(
        identity._Cover, "feed", autospec=True, side_effect=identity._Cover.feed
    ) as feed:
        with identity.identity_parse_scope():
            parse()
            with pytest.raises(RuntimeError), identity.identity_parse_scope(reuse_active=True):
                value = parse()
                value.context.clear()
                assert parse().context
                raise RuntimeError("summary cancelled")
            assert parse().context
            assert feed.call_count == 1
            with identity.identity_parse_scope():
                parse()
            assert feed.call_count == 2
        with identity.identity_parse_scope(reuse_active=True):
            parse()
        assert feed.call_count == 3
