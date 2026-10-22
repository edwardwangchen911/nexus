"""Request-local pure work cannot cache a safety decision or mutable input."""

import asyncio
import base64
import hashlib
import importlib
from contextvars import copy_context
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID

import pytest
from nexus.agents import codex_input_view as view
from nexus.agents import codex_token_budget as budget
from nexus.agents.codex_sdk import codex_request_wire


def test_projection_preserves_exact_wire_order_flags_and_caller_isolation():
    from nexus.agents.preparation_reuse import research_preparation_scope

    source = {
        "instructions": "frozen",
        "frozen_inputs": {"security_snapshot": {"items": []}},
        "evidence": [],
        "unknown": None,
        "unit": "USD",
    }
    expected = view.project_staged_payload(source)
    with patch.object(view, "_project_staged_payload", wraps=view._project_staged_payload) as build:
        with research_preparation_scope() as state:
            first = view.project_staged_payload(source)
            first["unit"] = "caller mutation"
            assert codex_request_wire(view.project_staged_payload(source)) == codex_request_wire(
                expected
            )
            assert build.call_count == 1
            reordered = dict(reversed(list(source.items())))
            assert codex_request_wire(view.project_staged_payload(reordered)) == codex_request_wire(
                reordered
            )
            view.project_staged_payload(source, inline_citations=True)
            source["unknown"] = 0
            view.project_staged_payload(source)
            assert build.call_count == 4
            assert state.counts["projection_hits"] == 1
        assert not state.projections and not state.encodings and not state.active
        view.project_staged_payload(source)
        assert build.call_count == 5


def test_projection_errors_are_not_reused_or_hidden():
    from nexus.agents.preparation_reuse import research_preparation_scope

    with research_preparation_scope():
        for _ in range(2):
            with pytest.raises(ValueError, match="MODEL_FINANCIAL_TABLE_BINDING_REQUIRED"):
                view.project_staged_payload({}, financial_table_candidate=True)
        # Unsupported serialization must retain the existing fallback behavior.
        source = {"frozen_inputs": {"number": float("nan")}}
        assert view.project_staged_payload(source) is source
        # JSON alone would alias tuple/list or integer/string mapping keys.
        tuple_source = {"frozen_inputs": {"items": (1, 2)}}
        assert view.project_staged_payload(tuple_source) is tuple_source
        assert isinstance(view.project_staged_payload({"items": [1, 2]})["items"], list)


def test_token_reuse_revalidates_authority_and_encoder(tmp_path, monkeypatch):
    from nexus.agents.preparation_reuse import research_preparation_scope

    tokens = [bytes([value]) for value in range(256)] + [b"ab", b"abc"]
    data = b"".join(base64.b64encode(t) + f" {i}\n".encode() for i, t in enumerate(tokens))
    path = tmp_path / "vocab"
    path.write_bytes(data)
    monkeypatch.setattr(budget, "tokenizer_cache_path", lambda: path)
    monkeypatch.setattr(budget, "VOCABULARY_SHA256", hashlib.sha256(data).hexdigest())
    encoding = importlib.import_module("tiktoken").Encoding
    with patch.object(
        encoding, "encode_ordinary", autospec=True, side_effect=encoding.encode_ordinary
    ) as encode:
        with research_preparation_scope() as state:
            assert budget.payload_input_units("abc abc", model="gpt-5.6-terra") == 3
            assert budget.payload_input_units("abc abc", model="gpt-5.6-terra") == 3
            assert encode.call_count == 1
            path.write_bytes(b"corrupt")
            assert budget.payload_input_units("abc abc", model="gpt-5.6-terra") == 7
            path.write_bytes(data)
            assert budget.payload_input_units("abc abc!", model="gpt-5.6-terra") == 4
            monkeypatch.setattr(budget.metadata, "version", lambda _: "0.13.0")
            assert budget.payload_input_units("abc abc", model="gpt-5.6-terra") == 7
            assert state.counts["token_hits"] == 1
            monkeypatch.setattr(budget.metadata, "version", lambda _: "0.14.0")
            budget._validated_encoding.cache_clear()
            assert budget.payload_input_units("abc abc", model="gpt-5.6-terra") == 3
            assert encode.call_count == 3  # A new validated encoder is not an old cache entry.
            for index in range(150):
                assert budget.payload_input_units(f"abc {index}", model="gpt-5.6-terra") > 0
            assert len(state.encodings) == 128
        assert not state.encodings


def test_nested_scopes_cancel_and_inherited_context_cannot_retain_values():
    from nexus.agents.preparation_reuse import current_reuse, research_preparation_scope

    with research_preparation_scope() as outer:
        view.project_staged_payload({"a": 1})
        inherited = copy_context()
        with pytest.raises(asyncio.CancelledError):
            with research_preparation_scope() as inner:
                view.project_staged_payload({"a": 1})
                raise asyncio.CancelledError()
        assert not inner.active and not inner.projections
        assert current_reuse() is outer
        view.project_staged_payload({"a": 1})
        assert outer.counts["projection_hits"] == 1
    assert inherited.run(current_reuse) is None
    assert current_reuse() is None


def test_projection_storage_is_bounded_and_oversized_values_still_compile():
    from nexus.agents.preparation_reuse import research_preparation_scope

    with research_preparation_scope() as state:
        for index in range(50):
            source = {"number": index, "text": "x" * 10_000}
            assert view.project_staged_payload(source) == source
        assert len(state.projections) <= 32
        assert state.projection_bytes <= 8 * 1024 * 1024
        source = {"text": "x" * (8 * 1024 * 1024 + 1)}
        assert view.project_staged_payload(source) == source
        assert state.projection_bytes <= 8 * 1024 * 1024


@pytest.mark.asyncio
async def test_normal_dispatch_owns_and_clears_scope_even_when_cancelled(monkeypatch):
    from nexus.agents.preparation_reuse import current_reuse
    from nexus.ops import manual_paper_host as host

    seen = []

    def cancelled(_settings):
        seen.append(current_reuse())
        assert seen[-1] is not None
        view.project_staged_payload({"a": 1})
        raise asyncio.CancelledError()

    monkeypatch.setattr(host, "session_factory", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await host.dispatch_pure_research_request(SimpleNamespace(), request_id=UUID(int=1))
    assert current_reuse() is None and not seen[0].active and not seen[0].projections
