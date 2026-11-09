"""Reuse only exact source structures; receipt and time checks still run."""

import json
from unittest.mock import patch

import pytest
from nexus.research import identity_sources as identity
from nexus.research import sec_disclosure_index as index
from unit.research.test_sec_disclosure_index import build, encode, receipt, source


def directory():
    return json.dumps(
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[1, "one", "AAA", "Nasdaq"], [2, "two", "BBB", "NYSE"]],
        }
    ).encode()


def test_directory_structure_is_shared_across_symbols_but_not_requests():
    original = identity.parse_identity_source
    with patch.object(identity, "parse_identity_source", wraps=original) as parse:
        with identity.identity_source_parse_scope():
            assert identity.directory_binding_from_raw(directory(), "AAA") == ("0000000001", "XNAS")
            assert identity.directory_binding_from_raw(directory(), "BBB") == ("0000000002", "XNYS")
            assert parse.call_count == 1
        identity.directory_binding_from_raw(directory(), "AAA")
        assert parse.call_count == 2


def test_index_structure_reuse_never_reuses_receipt_or_identity_authority():
    raw = encode(source())
    with patch.object(index, "_object", wraps=index._object) as parse:
        with index.sec_index_parse_scope():
            first = build(raw)
            second = build(raw)
            assert first == second and first is not second
            assert sum(call.args[0] == raw for call in parse.call_args_list) == 1
            with pytest.raises(ValueError):
                build(raw, receipt(raw, received_at="2099-01-05T20:00:00+00:00"))
            with pytest.raises(ValueError):
                build(raw + b"broken", receipt(raw))
        build(raw)
        assert sum(call.args[0] == raw for call in parse.call_args_list) == 2


def test_index_failure_and_cancelled_scope_do_not_leak():
    raw = encode(
        source(rows=[("broken", "8-K", "2026-01-05T19:00:00Z", "2026-01-05", "report.htm")])
    )
    with patch.object(index, "_object", wraps=index._object) as parse:
        with index.sec_index_parse_scope():
            for _ in range(2):
                with pytest.raises(ValueError):
                    build(raw)
        assert sum(call.args[0] == raw for call in parse.call_args_list) == 2
    with pytest.raises(RuntimeError):
        with index.sec_index_parse_scope():
            build()
            raise RuntimeError("read cancelled")
    with patch.object(index, "_object", wraps=index._object) as parse:
        build()
        assert parse.call_count == 2


def test_structure_limits_fall_back_and_cached_entries_do_not_share_mutations():
    with patch.object(index, "_parse_source_rows", wraps=index._parse_source_rows) as parse:
        with index.sec_index_parse_scope():
            for number in range(17):
                raw = encode(source() | {"extra": number})
                build(raw)
            build(raw)
            assert parse.call_count == 18
    with index.sec_index_parse_scope():
        first = build()
        second = build()
        assert first.entries[0] is not second.entries[0]
        first.entries[0].__dict__["primary_document"] = "caller-mutation.htm"
        assert build().entries[0].primary_document == "report.htm"
    with patch.object(
        identity, "parse_identity_source", wraps=identity.parse_identity_source
    ) as parse:
        with identity.identity_source_parse_scope():
            for number in range(17):
                raw = json.dumps(json.loads(directory()) | {"extra": number}).encode()
                identity.directory_binding_from_raw(raw, "AAA")
            identity.directory_binding_from_raw(raw, "BBB")
            assert parse.call_count == 18


def test_exited_inherited_index_scope_cannot_be_refilled():
    from contextvars import copy_context

    with index.sec_index_parse_scope():
        build()
        inherited = copy_context()
    with patch.object(index, "_parse_source_rows", wraps=index._parse_source_rows) as parse:
        inherited.run(build)
        inherited.run(build)
        assert parse.call_count == 2
