"""Fixed-origin, market-only client for the isolated Fosun read gateway."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import re
import ssl
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal
from uuid import UUID

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)

from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc

FosunMarketOperation = Literal["fosun_get_quotes", "fosun_get_bars"]
FosunBarType = Literal["day", "minute"]

_PATHS: dict[FosunMarketOperation, str] = {
    "fosun_get_quotes": "/internal/v1/read-only/fosun_get_quotes",
    "fosun_get_bars": "/internal/v1/read-only/fosun_get_bars",
}
_US_CODE = re.compile(r"^us[A-Z][A-Z0-9.-]{0,14}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_MAX_RESPONSE_BYTES = 1024 * 1024
_MAX_REQUEST_SECONDS = 10.0
_MAX_GATEWAY_CLOCK_SKEW_SECONDS = 60


class FosunMarketGatewayError(RuntimeError):
    """Sanitized transport or contract failure safe for logs and job results."""


@dataclass(frozen=True, repr=False)
class FosunMarketGatewayIdentity:
    """Independent Provider Worker identity; intentionally has no broker fields."""

    origin: str
    key_id: str
    secret: SecretStr
    client_cert: Path
    client_key: Path
    client_ca: Path


class RawGatewayResponse(BaseModel):
    """Exact gateway HTTP bytes captured before JSON interpretation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: FosunMarketOperation
    gateway_request_id: UUID
    request_body_sha256: str
    raw_payload: bytes
    sent_at: datetime
    received_at: datetime

    @field_validator("request_body_sha256")
    @classmethod
    def _canonical_hash(cls, value: str) -> str:
        if _HASH.fullmatch(value) is None:
            raise ValueError("request body digest is invalid")
        return value

    @field_validator("raw_payload")
    @classmethod
    def _bounded_payload(cls, value: bytes) -> bytes:
        if not value or len(value) > _MAX_RESPONSE_BYTES:
            raise ValueError("gateway response bytes are invalid")
        return value

    @field_validator("sent_at", "received_at")
    @classmethod
    def _transport_time_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _bounded_transport_envelope(self) -> RawGatewayResponse:
        elapsed = (self.received_at - self.sent_at).total_seconds()
        if elapsed < 0 or elapsed > _MAX_REQUEST_SECONDS + 1:
            raise ValueError("gateway transport envelope is invalid")
        return self


class FosunQuoteItem(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["quote"]
    instrument_code: str
    price: Decimal | None
    as_of: datetime

    @field_validator("instrument_code")
    @classmethod
    def _safe_code(cls, value: str) -> str:
        return _validated_code(value)

    @field_validator("price", mode="before")
    @classmethod
    def _exact_price(cls, value: object) -> object:
        if value is None:
            return None
        return _positive_decimal(value, "quote price")

    @field_validator("as_of")
    @classmethod
    def _as_of_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class FosunBarItem(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["bar"]
    instrument_code: str
    interval: FosunBarType
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    @field_validator("instrument_code")
    @classmethod
    def _safe_code(cls, value: str) -> str:
        return _validated_code(value)

    @field_validator("timestamp")
    @classmethod
    def _timestamp_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @field_validator("open", "high", "low", "close", mode="before")
    @classmethod
    def _positive_prices(cls, value: object) -> Decimal:
        return _positive_decimal(value, "bar price")

    @field_validator("volume", mode="before")
    @classmethod
    def _nonnegative_volume(cls, value: object) -> Decimal:
        result = _exact_decimal(value, "bar volume")
        if result < 0:
            raise ValueError("bar volume is invalid")
        return result

    @model_validator(mode="after")
    def _valid_ohlc(self) -> FosunBarItem:
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("bar OHLC is invalid")
        return self


class FosunQuotePage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    items: tuple[FosunQuoteItem, ...]
    source_request_id: str
    has_more: Literal[False]
    next_cursor: None
    as_of: datetime
    quality: Literal["PARTIAL"]

    @field_validator("source_request_id")
    @classmethod
    def _safe_request_id(cls, value: str) -> str:
        return _safe_request_id(value)

    @field_validator("as_of")
    @classmethod
    def _as_of_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class FosunBarPage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    items: tuple[FosunBarItem, ...]
    source_request_id: str
    has_more: Literal[False]
    next_cursor: None
    as_of: datetime
    quality: Literal["PARTIAL"]

    @field_validator("source_request_id")
    @classmethod
    def _safe_request_id(cls, value: str) -> str:
        return _safe_request_id(value)

    @field_validator("as_of")
    @classmethod
    def _as_of_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class FosunMarketGatewayClient:
    """Expose exactly the two signed market reads and return raw bytes first."""

    def __init__(
        self,
        identity: FosunMarketGatewayIdentity,
        *,
        client: httpx.AsyncClient | None = None,
        wall_time: Callable[[], float] = time.time,
        now: Callable[[], datetime] | None = None,
        uuid_factory: Callable[[], UUID] = new_uuid7,
    ) -> None:
        self._origin = _validated_origin(identity.origin)
        if not identity.key_id.strip() or not identity.secret.get_secret_value().strip():
            raise ValueError("FOSUN_MARKET_GATEWAY_IDENTITY_REQUIRED")
        self._key_id = identity.key_id
        self._secret = identity.secret.get_secret_value()
        self._wall_time = wall_time
        self._now = now or (lambda: datetime.now(UTC))
        self._uuid_factory = uuid_factory
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            verify=_mtls_context(
                identity.client_ca,
                identity.client_cert,
                identity.client_key,
            ),
            timeout=httpx.Timeout(10.0),
            follow_redirects=False,
            trust_env=False,
        )

    async def read_quotes_raw(self, codes: tuple[str, ...]) -> RawGatewayResponse:
        _validate_codes(codes)
        return await self._post("fosun_get_quotes", {"codes": list(codes)})

    async def read_bars_raw(
        self,
        *,
        code: str,
        ktype: FosunBarType,
        start_time: datetime,
        end_time: datetime,
        limit: int,
    ) -> RawGatewayResponse:
        return await self._post(
            "fosun_get_bars",
            _bars_request_payload(
                code=code,
                ktype=ktype,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            ),
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _post(
        self,
        operation: FosunMarketOperation,
        payload: dict[str, object],
    ) -> RawGatewayResponse:
        path = _PATHS[operation]
        body = _canonical_request_body(payload)
        timestamp = int(self._wall_time())
        nonce = self._uuid_factory()
        request_id = self._uuid_factory()
        if nonce.version != 7 or request_id.version != 7:
            raise FosunMarketGatewayError("gateway request identity generation failed")
        body_hash = hashlib.sha256(body).hexdigest()
        actor_id = "nexus-provider-worker"
        canonical = "\n".join(
            (
                "hmac-v2",
                "POST",
                path,
                str(timestamp),
                str(nonce),
                actor_id,
                str(request_id),
                body_hash,
            )
        )
        signature = base64.urlsafe_b64encode(
            hmac.new(
                self._secret.encode(),
                canonical.encode(),
                hashlib.sha256,
            ).digest()
        ).rstrip(b"=")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Nexus-Signature-Version": "hmac-v2",
            "X-Nexus-Key-Id": self._key_id,
            "X-Nexus-Timestamp": str(timestamp),
            "X-Nexus-Nonce": str(nonce),
            "X-Nexus-Body-SHA256": body_hash,
            "X-Nexus-Signature": signature.decode(),
            "X-Nexus-Actor-Id": actor_id,
            "X-Request-Id": str(request_id),
        }
        sent_at = self._now()
        try:
            async with asyncio.timeout(_MAX_REQUEST_SECONDS):
                async with self._client.stream(
                    "POST",
                    f"{self._origin}{path}",
                    content=body,
                    headers=headers,
                    follow_redirects=False,
                ) as response:
                    if response.status_code != 200 or not _is_exact_json_media_type(
                        response.headers.get("content-type")
                    ):
                        raise FosunMarketGatewayError("gateway response rejected")
                    _validate_content_length(response.headers.get("content-length"))
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > _MAX_RESPONSE_BYTES:
                            raise FosunMarketGatewayError("gateway response rejected")
                        chunks.append(chunk)
                    raw_payload = b"".join(chunks)
        except FosunMarketGatewayError:
            raise
        except TimeoutError:
            raise FosunMarketGatewayError("gateway request failed") from None
        except httpx.HTTPError:
            raise FosunMarketGatewayError("gateway request failed") from None
        if not raw_payload:
            raise FosunMarketGatewayError("gateway response rejected")
        return RawGatewayResponse(
            operation=operation,
            gateway_request_id=request_id,
            request_body_sha256=body_hash,
            raw_payload=raw_payload,
            sent_at=sent_at,
            received_at=self._now(),
        )


def fosun_bars_request_body_sha256(
    *,
    code: str,
    ktype: FosunBarType,
    start_time: datetime,
    end_time: datetime,
    limit: int,
) -> str:
    """Hash the exact canonical JSON body used by the bars gateway POST."""
    payload = _bars_request_payload(
        code=code,
        ktype=ktype,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )
    return hashlib.sha256(_canonical_request_body(payload)).hexdigest()


def _bars_request_payload(
    *,
    code: str,
    ktype: FosunBarType,
    start_time: datetime,
    end_time: datetime,
    limit: int,
) -> dict[str, object]:
    _validated_code(code)
    if ktype not in {"day", "minute"}:
        raise FosunMarketGatewayError("gateway bars request rejected")
    start_time = _aware_utc(start_time)
    end_time = _aware_utc(end_time)
    if start_time > end_time or isinstance(limit, bool) or not 1 <= limit <= 500:
        raise FosunMarketGatewayError("gateway bars request rejected")
    return {
        "code": code,
        "ktype": ktype,
        "start_time": _format_utc(start_time),
        "end_time": _format_utc(end_time),
        "limit": limit,
    }


def _canonical_request_body(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def parse_quote_page(
    response: RawGatewayResponse,
    *,
    expected_codes: tuple[str, ...],
) -> FosunQuotePage:
    """Parse strict quote bytes only after the caller has an archiveable envelope."""
    _validate_codes(expected_codes)
    if response.operation != "fosun_get_quotes":
        raise FosunMarketGatewayError("gateway quote contract rejected")
    try:
        page = FosunQuotePage.model_validate(_load_exact_json(response.raw_payload))
        if (
            tuple(item.instrument_code for item in page.items) != expected_codes
            or not _gateway_time_is_plausible(
                page.as_of,
                sent_at=response.sent_at,
                received_at=response.received_at,
            )
            or any(item.as_of != page.as_of for item in page.items)
        ):
            raise ValueError
        return page
    except (ValidationError, TypeError, ValueError):
        raise FosunMarketGatewayError("gateway quote contract rejected") from None


def parse_bar_page(
    response: RawGatewayResponse,
    *,
    expected_code: str,
    expected_ktype: FosunBarType,
    start: datetime,
    end: datetime,
    limit: int,
) -> FosunBarPage:
    """Revalidate gateway-owned bars before they become normalized market facts."""
    expected_code = _validated_code(expected_code)
    start = _aware_utc(start)
    end = _aware_utc(end)
    if (
        response.operation != "fosun_get_bars"
        or expected_ktype not in {"day", "minute"}
        or start > end
        or isinstance(limit, bool)
        or not 1 <= limit <= 500
    ):
        raise FosunMarketGatewayError("gateway bar contract rejected")
    try:
        page = FosunBarPage.model_validate(_load_exact_json(response.raw_payload))
        if (
            not _gateway_time_is_plausible(
                page.as_of,
                sent_at=response.sent_at,
                received_at=response.received_at,
            )
            or len(page.items) > limit
        ):
            raise ValueError
        previous: datetime | None = None
        for item in page.items:
            in_window = (
                start.date() <= item.timestamp.date() <= end.date()
                if expected_ktype == "day"
                else start <= item.timestamp <= end
            )
            if (
                item.instrument_code != expected_code
                or item.interval != expected_ktype
                or (
                    expected_ktype == "minute"
                    and (item.timestamp.second != 0 or item.timestamp.microsecond != 0)
                )
                or not in_window
                or item.timestamp > page.as_of
                or (previous is not None and item.timestamp <= previous)
            ):
                raise ValueError
            previous = item.timestamp
        return page
    except (ValidationError, TypeError, ValueError):
        raise FosunMarketGatewayError("gateway bar contract rejected") from None


def _load_exact_json(raw: bytes) -> object:
    try:
        return json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_float=Decimal,
            parse_int=Decimal,
            parse_constant=_reject_json_constant,
        )
    except FosunMarketGatewayError:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise FosunMarketGatewayError("gateway contract rejected") from None


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise FosunMarketGatewayError("gateway contract rejected")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    del value
    raise FosunMarketGatewayError("gateway contract rejected")


def _validate_codes(codes: tuple[str, ...]) -> None:
    if not 1 <= len(codes) <= 50 or len(set(codes)) != len(codes) or codes != tuple(sorted(codes)):
        raise FosunMarketGatewayError("gateway quote codes rejected")
    for code in codes:
        _validated_code(code)


def _validated_code(value: object) -> str:
    if not isinstance(value, str) or _US_CODE.fullmatch(value) is None:
        raise FosunMarketGatewayError("gateway market code rejected")
    return value


def _exact_decimal(value: object, label: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError(f"{label} is invalid")
    if not isinstance(value, Decimal | int | str):
        raise ValueError(f"{label} is invalid")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except Exception as error:
        raise ValueError(f"{label} is invalid") from error
    if not result.is_finite():
        raise ValueError(f"{label} is invalid")
    return result


def _positive_decimal(value: object, label: str) -> Decimal:
    result = _exact_decimal(value, label)
    if result <= 0:
        raise ValueError(f"{label} is invalid")
    return result


def _safe_request_id(value: str) -> str:
    if (
        not value.isascii()
        or not 1 <= len(value.encode()) <= 128
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError("source request id is invalid")
    return value


def _aware_utc(value: datetime) -> datetime:
    try:
        return require_utc(value)
    except (TypeError, ValueError):
        raise FosunMarketGatewayError("gateway timestamp rejected") from None


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _is_exact_json_media_type(value: str | None) -> bool:
    return value is not None and value.lower().strip() in {
        "application/json",
        "application/json; charset=utf-8",
    }


def _validate_content_length(value: str | None) -> None:
    if value is None:
        return
    if not value.isascii() or not value.isdecimal() or int(value) > _MAX_RESPONSE_BYTES:
        raise FosunMarketGatewayError("gateway response rejected")


def _gateway_time_is_plausible(
    gateway_as_of: datetime,
    *,
    sent_at: datetime,
    received_at: datetime,
) -> bool:
    """Accept only gateway time inside the measured request envelope plus skew."""
    skew = timedelta(seconds=_MAX_GATEWAY_CLOCK_SKEW_SECONDS)
    return sent_at - skew <= gateway_as_of <= received_at + skew


def _validated_origin(value: str) -> str:
    try:
        url = httpx.URL(value)
    except ValueError:
        raise ValueError("gateway origin must be an absolute HTTPS origin") from None
    if (
        url.scheme != "https"
        or not url.host
        or bool(url.username)
        or bool(url.password)
        or url.raw_path not in (b"", b"/")
        or url.query
        or url.fragment
    ):
        raise ValueError("gateway origin must be an absolute HTTPS origin")
    return str(url).rstrip("/")


def _mtls_context(client_ca: Path, client_cert: Path, client_key: Path) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=str(client_ca))
    context.load_cert_chain(certfile=str(client_cert), keyfile=str(client_key))
    return context
