"""Tokenizer preparation is explicit; request preflight never fetches vocabulary."""

import base64
import hashlib
import importlib
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def official_vocab() -> bytes:
    source = os.environ.get("NEXUS_TEST_TIKTOKEN_VOCAB")
    if source is None:
        pytest.skip("explicit offline public vocabulary fixture was not provided")
    data = Path(source).read_bytes()
    assert hashlib.sha256(data).hexdigest() == (
        "446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d"
    )
    return data


@pytest.fixture
def forbid_network(monkeypatch):
    import socket

    attempts = []

    def forbidden(*args, **kwargs):
        attempts.append(1)
        raise AssertionError("token measurement must never connect")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    return attempts


@pytest.mark.parametrize(
    "text, expected",
    [
        ("hello world", 2),
        ("你好，世界！", 4),
        ("<|endoftext|>", 7),
        (
            json.dumps(
                {"text": "只分析冻结证据，缺失数据保持未知。" * 1000},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            12004,
        ),
        (
            json.dumps(
                {"text": '{"kind":"synthetic","status":"unknown"}' * 1000},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            11004,
        ),
        (
            json.dumps(
                {"text": "<|endoftext|> <|im_start|> 数据"},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            18,
        ),
    ],
)
def test_official_encoding_counts_ordinary_text_offline(
    tmp_path,
    monkeypatch,
    official_vocab,
    forbid_network,
    text,
    expected,
):
    from nexus.agents import codex_token_budget as budget

    path = tmp_path / "o200k_base.tiktoken"
    path.write_bytes(official_vocab)
    monkeypatch.setattr(budget, "tokenizer_cache_path", lambda: path)
    assert budget.payload_input_units(text, model="gpt-5.6-terra") == expected
    assert not forbid_network


def test_real_encoding_merges_synthetic_vocabulary_without_official_asset(
    tmp_path, monkeypatch, forbid_network
):
    from nexus.agents import codex_token_budget as budget

    tokens = [bytes([value]) for value in range(256)] + [b"ab", b"abc"]
    data = b"".join(
        base64.b64encode(token) + f" {rank}\n".encode() for rank, token in enumerate(tokens)
    )
    path = tmp_path / "synthetic-vocabulary"
    path.write_bytes(data)
    # Only the public fixture identity changes: use the real pinned Encoding,
    # complete production regex/special config, and encode_ordinary path.
    monkeypatch.setattr(budget, "tokenizer_cache_path", lambda: path)
    monkeypatch.setattr(budget, "VOCABULARY_SHA256", hashlib.sha256(data).hexdigest())
    text = "abc abc"
    count = budget.payload_input_units(text, model="gpt-5.6-terra")
    assert count == 3 and count < len(text.encode())
    assert not forbid_network


@pytest.mark.parametrize("case", ["missing", "corrupt", "oversized", "model", "version", "package"])
def test_local_counter_failures_fall_back_to_utf8_bytes_without_network(
    tmp_path,
    monkeypatch,
    forbid_network,
    case,
):
    from nexus.agents import codex_token_budget as budget

    path = tmp_path / "o200k_base.tiktoken"
    if case != "missing":
        path.write_bytes(
            b"bad"
            if case == "corrupt"
            else (b"x" * (4 * 1024 * 1024 + 1) if case == "oversized" else b"invalid vocabulary")
        )
    monkeypatch.setattr(budget, "tokenizer_cache_path", lambda: path)
    if case == "version":
        monkeypatch.setattr(budget.metadata, "version", lambda name: "0.13.0")
    if case == "package":
        real_import = importlib.import_module

        def missing(name):
            if name == "tiktoken":
                raise ModuleNotFoundError(name)
            return real_import(name)

        monkeypatch.setattr(budget.importlib, "import_module", missing)
    text = "中文与<|endoftext|>"
    assert budget.payload_input_units(
        text, model="unknown" if case == "model" else "gpt-5.4-mini"
    ) == len(text.encode())
    assert not forbid_network


@pytest.mark.parametrize("case", ["model", "version", "package", "load", "encode"])
def test_failure_boundaries_are_exercised_without_official_asset(
    tmp_path, monkeypatch, forbid_network, case
):
    from nexus.agents import codex_token_budget as budget

    data = b"YQ== 0\n"
    path = tmp_path / "synthetic-vocabulary"
    path.write_bytes(data)
    monkeypatch.setattr(budget, "tokenizer_cache_path", lambda: path)
    monkeypatch.setattr(budget, "VOCABULARY_SHA256", hashlib.sha256(data).hexdigest())
    monkeypatch.setattr(
        budget.metadata, "version", lambda name: "0.13.0" if case == "version" else "0.14.0"
    )
    calls = []

    def encode(text):
        calls.append("encode")
        raise ValueError("synthetic encoding failure")

    def construct(**kwargs):
        calls.append("construct")
        if case == "load":
            raise ValueError("synthetic load failure")
        return SimpleNamespace(encode_ordinary=encode)

    def load(name):
        calls.append("import")
        assert name == "tiktoken"
        if case == "package":
            raise ModuleNotFoundError(name)
        return SimpleNamespace(Encoding=construct)

    monkeypatch.setattr(budget.importlib, "import_module", load)
    assert (
        budget.payload_input_units(
            "中文", model="gpt-5-unknown" if case == "model" else "gpt-5.4-mini"
        )
        == 6
    )
    assert (
        calls
        == {
            "model": [],
            "version": [],
            "package": ["import"],
            "load": ["import", "construct"],
            "encode": ["import", "construct", "encode"],
        }[case]
    )
    assert not forbid_network


def test_preflight_counts_schema_explicit_instructions_and_original_margin(tmp_path, monkeypatch):
    from nexus.agents import codex_token_budget as budget

    monkeypatch.setattr(budget, "tokenizer_cache_path", lambda: tmp_path / "missing")
    schema = {"type": "object", "description": "中文"}
    assert (
        budget.input_preflight_units(
            "中文",
            model="gpt-5.4-mini",
            output_schema=schema,
            base_instructions="base",
            developer_instructions="研发",
        )
        == 6
        + len(json.dumps(schema, ensure_ascii=False, separators=(",", ":")).encode())
        + 4
        + 6
        + 8192
    )


def test_fifo_cache_is_rejected_without_blocking(tmp_path):
    path = tmp_path / "not-a-regular-vocabulary"
    os.mkfifo(path)
    script = (
        "from pathlib import Path; from nexus.agents import codex_token_budget as b; "
        f"b.tokenizer_cache_path = lambda: Path({str(path)!r}); "
        "assert b.local_vocabulary() is None; "
        "assert b.payload_input_units('中文', model='gpt-5.4-mini') == 6"
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, timeout=2)
    assert result.returncode == 0
