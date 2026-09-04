"""Public vocabulary preparation has no account or model dependency."""

import asyncio
import hashlib
import json

import httpx
import pytest
from nexus.agents import codex_token_budget as budget
from nexus.ops import openai_local


@pytest.fixture
def vocabulary(tmp_path, monkeypatch):
    data = b"YQ== 0\n"
    path = tmp_path / "public-cache" / "o200k_base.tiktoken"
    monkeypatch.setattr(budget, "tokenizer_cache_path", lambda: path)
    monkeypatch.setattr(budget, "VOCABULARY_SHA256", hashlib.sha256(data).hexdigest())
    return path, data


def transport(monkeypatch, handler):
    original = httpx.AsyncClient
    options = []

    def client(**kwargs):
        options.append(kwargs)
        return original(**kwargs, transport=httpx.MockTransport(handler))

    monkeypatch.setattr(openai_local.httpx, "AsyncClient", client)
    return options


@pytest.mark.asyncio
async def test_prepares_fixed_public_vocabulary_once_then_no_network(vocabulary, monkeypatch):
    path, data = vocabulary
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=data)

    options = transport(monkeypatch, handler)
    result = await openai_local.prepare_tokenizer()
    assert result["downloaded"] is True
    assert path.read_bytes() == data
    assert len(calls) == 1 and str(calls[0].url) == budget.VOCABULARY_URL
    assert calls[0].method == "GET"
    assert options[0]["follow_redirects"] is False
    assert options[0]["trust_env"] is False
    assert options[0]["timeout"].read <= 30
    assert (await openai_local.prepare_tokenizer())["downloaded"] is False
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["oversized", "sha", "redirect", "timeout"])
async def test_invalid_download_never_replaces_existing_file(vocabulary, monkeypatch, case):
    path, data = vocabulary
    path.parent.mkdir()
    path.write_bytes(b"old invalid public file")
    calls = []

    def handler(request):
        calls.append(request)
        if case == "timeout":
            raise httpx.ReadTimeout("synthetic")
        if case == "redirect":
            return httpx.Response(302, headers={"location": "https://example.invalid/"})
        return httpx.Response(
            200,
            content=(
                b"x" * (budget.MAX_VOCABULARY_BYTES + 1) if case == "oversized" else b"wrong hash"
            ),
        )

    transport(monkeypatch, handler)
    with pytest.raises(ValueError, match="CODEX_TOKENIZER_"):
        await openai_local.prepare_tokenizer()
    assert len(calls) == 1
    assert path.read_bytes() == b"old invalid public file"
    assert list(path.parent.iterdir()) == [path]


@pytest.mark.asyncio
async def test_entire_stream_has_wall_clock_deadline(vocabulary, monkeypatch):
    path, _ = vocabulary

    class SlowStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                await asyncio.sleep(0.005)
                yield b"x"

    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, stream=SlowStream())

    transport(monkeypatch, handler)
    monkeypatch.setattr(openai_local, "_TOKENIZER_TIMEOUT_SECONDS", 0.02)
    with pytest.raises(ValueError, match="CODEX_TOKENIZER_"):
        await asyncio.wait_for(openai_local.prepare_tokenizer(), timeout=1)
    assert len(calls) == 1 and not path.exists()


@pytest.mark.parametrize("cached", [False, True])
def test_prepare_cli_skip_key_never_reads_settings_or_network(
    vocabulary, monkeypatch, capsys, cached
):
    path, data = vocabulary
    if cached:
        path.parent.mkdir()
        path.write_bytes(data)

    def forbidden(*args, **kwargs):
        pytest.fail("public preparation must not load settings or connect")

    monkeypatch.setattr(openai_local, "local_settings", forbidden)
    monkeypatch.setattr(openai_local.httpx, "AsyncClient", forbidden)
    monkeypatch.chdir(path.parent.parent)
    monkeypatch.setattr("sys.argv", ["nexus-openai-local", "prepare-tokenizer", "--skip-key"])
    assert openai_local.main() == (0 if cached else 1)
    result = json.loads(capsys.readouterr().out)
    if cached:
        assert result["downloaded"] is False
    else:
        assert result["code"] == "CODEX_TOKENIZER_NETWORK_DISABLED"


def test_explicit_prepare_cli_can_download_without_any_local_settings(
    vocabulary, monkeypatch, capsys
):
    path, data = vocabulary

    def forbidden(*args, **kwargs):
        pytest.fail("public preparation must not load settings")

    monkeypatch.setattr(openai_local, "local_settings", forbidden)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=data)

    transport(monkeypatch, handler)
    monkeypatch.chdir(path.parent.parent)
    monkeypatch.setattr("sys.argv", ["nexus-openai-local", "prepare-tokenizer"])
    assert openai_local.main() == 0
    assert json.loads(capsys.readouterr().out)["downloaded"] is True
    assert path.read_bytes() == data and len(calls) == 1
