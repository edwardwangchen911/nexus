"""Strict, display-only Alpaca news parsing; no content HTML is consumed."""

import json
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator


class NewsError(ValueError):
    """Fixed safe news error."""


def validate_symbol(symbol: str) -> str:
    if re.fullmatch(r"[A-Z][A-Z0-9.-]{0,14}", symbol) is None:
        raise NewsError("NEWS_SYMBOL_INVALID")
    return symbol


def aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise NewsError("NEWS_TIME_INVALID")
    return value


class NewsArticle(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    id: StrictInt
    headline: StrictStr = Field(max_length=10000)
    summary: StrictStr = Field(max_length=100000)
    author: StrictStr = Field(max_length=2000)
    created_at: datetime
    updated_at: datetime
    url: StrictStr | None = None
    symbols: tuple[StrictStr, ...]
    source: StrictStr = Field(max_length=2000)

    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def timestamp(cls, value: Any) -> datetime:
        if not isinstance(value, str):
            raise NewsError("NEWS_TIME_INVALID")
        return aware(datetime.fromisoformat(value.replace("Z", "+00:00")))

    @field_validator("url")
    @classmethod
    def safe_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            parsed = urlsplit(value)
            if (
                parsed.scheme in {"http", "https"} and parsed.hostname
                and not parsed.username and not parsed.password
                and not any(ord(char) <= 32 for char in value)
            ):
                return value
        except ValueError:
            pass
        return None


class NewsPage(BaseModel):
    model_config = ConfigDict(frozen=True)
    articles: tuple[NewsArticle, ...]
    has_more: bool


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise NewsError("NEWS_RESPONSE_INVALID")
        result[key] = value
    return result


def parse_news(raw: bytes, *, symbol: str, received_at: datetime) -> NewsPage:
    validate_symbol(symbol)
    aware(received_at)
    try:
        payload = json.loads(raw, object_pairs_hook=_unique)
        if not isinstance(payload, dict) or not isinstance(payload.get("news"), list):
            raise ValueError
        token = payload.get("next_page_token")
        if token is not None and not isinstance(token, str):
            raise ValueError
        if len(payload["news"]) > 50:
            raise ValueError
        fields = NewsArticle.model_fields
        if any(not isinstance(item, dict) for item in payload["news"]):
            raise ValueError
        articles = tuple(
            NewsArticle.model_validate({key: value for key, value in item.items() if key in fields})
            for item in payload["news"]
        )
        if len({item.id for item in articles}) != len(articles) or any(
            symbol not in item.symbols
            or item.created_at > item.updated_at
            or item.updated_at > received_at
            for item in articles
        ):
            raise ValueError
        return NewsPage(articles=articles, has_more=bool(token))
    except (ValueError, TypeError, OverflowError) as error:
        raise NewsError("NEWS_RESPONSE_INVALID") from error
