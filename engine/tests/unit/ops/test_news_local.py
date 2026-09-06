import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from nexus.ops.news_local import collect_news, news_window
from nexus.providers.news import NewsError, parse_news
from pydantic import SecretStr

NOW = datetime(2026, 9, 6, tzinfo=UTC)


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    monkeypatch.setattr("nexus.ops.news_local.datetime", Clock)


def article(**overrides):
    return {"id": 1, "headline": "Example", "summary": "Plain text", "author": "Writer",
            "created_at": "2026-09-05T00:00:00Z", "updated_at": "2026-09-05T01:00:00Z",
            "url": "https://example.com/news", "symbols": ["EXAMPLE"], "source": "example",
            **overrides}


def payload(**overrides):
    return json.dumps({"news": [article(**overrides)], "next_page_token": "private-token"}).encode()


def test_parser_preserves_timestamps_and_discards_unsafe_link():
    page = parse_news(payload(url="javascript:alert(1)"), symbol="EXAMPLE", received_at=NOW)
    assert page.has_more and page.articles[0].url is None
    assert page.articles[0].created_at < page.articles[0].updated_at


@pytest.mark.parametrize("changes", [
    {"symbols": ["OTHER"]}, {"id": True}, {"headline": 42},
    {"created_at": "2026-09-05T00:00:00"},
    {"updated_at": "2026-09-04T00:00:00Z"},
    {"updated_at": "2026-09-07T00:00:00Z"},
])
def test_reject_bad_article(changes):
    with pytest.raises(NewsError, match="NEWS_RESPONSE_INVALID"):
        parse_news(payload(**changes), symbol="EXAMPLE", received_at=NOW)


@pytest.mark.parametrize("raw", [
    b'{"news": [], "news": []}',
    json.dumps({"news": [article(), article()]}).encode(),
])
def test_reject_duplicate(raw):
    with pytest.raises(NewsError):
        parse_news(raw, symbol="EXAMPLE", received_at=NOW)


def test_window_is_delayed_and_bounded():
    start, end = news_window(now=NOW)
    assert start == NOW - timedelta(days=7)
    assert end == NOW - timedelta(minutes=15)
    with pytest.raises(NewsError):
        news_window(now=NOW, end=NOW)


@pytest.mark.asyncio
async def test_single_page_private_receipt(tmp_path: Path):
    requests = []
    def respond(request):
        requests.append(request)
        assert request.url.host == "data.alpaca.markets"
        assert request.url.params["sort"] == "desc"
        assert request.url.params["include_content"] == "false"
        return httpx.Response(200, content=payload())
    output = tmp_path / "archive"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await collect_news(symbol="EXAMPLE", output_dir=output,
            key_id=SecretStr("test-key"), secret=SecretStr("test-secret"), client=client,
            start=NOW - timedelta(days=2), end=NOW - timedelta(hours=1))
    assert len(requests) == 1 and result["has_more"]
    receipt = json.loads((output / "receipt.json").read_bytes())
    assert receipt["schema"] == "nexus_news_receipt_v1"
    assert "private-token" not in (output / "receipt.json").read_text()
    assert output.stat().st_mode & 0o777 == 0o700
    assert (output / "raw.json").stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
@pytest.mark.parametrize("status,body,code", [
    (429, b'{}', "NEWS_HTTP_FAILED"),
    (200, payload(summary="test-secret"), "NEWS_SECRET_REFLECTION"),
    (200, payload(summary="test-secret").replace(b"test-secret", b"test-\\u0073ecret"),
     "NEWS_SECRET_REFLECTION"),
    (200, payload(updated_at="2026-09-06T00:00:00Z"), "NEWS_WINDOW_RESPONSE_INVALID"),
])
async def test_failure_no_retry_no_archive(tmp_path, status, body, code):
    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(status, content=body)
    output = tmp_path / "archive"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(NewsError, match=code):
            await collect_news(symbol="EXAMPLE", output_dir=output,
                key_id=SecretStr("test-key"), secret=SecretStr("test-secret"), client=client,
                start=NOW - timedelta(days=2), end=NOW - timedelta(hours=1))
    assert len(requests) == 1
    assert list(output.iterdir()) == []
