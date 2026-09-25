"""Single explicit FRED vintage, exclusive private originals before parsing."""

import asyncio
import json
import os
import stat
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from nexus.ops.local_recovery import write_private
from nexus.providers.fred_http import FredHttpTransport
from nexus.providers.http import HttpPolicyError
from nexus.research.fred_observations import (
    MAX_PAGES,
    FredCapture,
    FredVintageCollection,
    FredVintageRequest,
    decode,
    metadata_unit,
    page_count,
    parse_collected_vintage,
    public_query,
    validate_captured_pages,
)

ORIGIN = "https://api.stlouisfed.org"
_SAFE_HTTP_FAILURES = frozenset(
    {
        "DEADLINE_EXCEEDED",
        "REDIRECT_REJECTED",
        "RESPONSE_TOO_LARGE",
        "RETRY_BUDGET_EXHAUSTED",
        "STATUS_REJECTED",
    }
)
_SAFE_VALIDATION_FAILURES = frozenset(
    {"FRED_ORIGINAL_LIMIT", "FRED_PAGE_LIMIT", "FRED_VINTAGE_INVALID"}
)


def json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def capture_receipt(request: FredVintageRequest, capture: FredCapture) -> dict[str, object]:
    return {
        "schema": "fred_raw_receipt_v1",
        "origin": ORIGIN,
        "path": "/fred/series" + ("/observations" if capture.kind == "observations" else ""),
        "query": public_query(request, capture.kind, capture.offset),
        "kind": capture.kind,
        "offset": capture.offset,
        "received_at": capture.received_at.isoformat(),
        "sha256": sha256(capture.raw_payload).hexdigest(),
        "byte_length": len(capture.raw_payload),
    }


def _safe_failure_code(error: Exception) -> str:
    if isinstance(error, HttpPolicyError):
        code = str(error)
        return code if code in _SAFE_HTTP_FAILURES else "FRED_HTTP_FAILURE_UNKNOWN"
    if isinstance(error, TimeoutError):
        return "FRED_COLLECTION_TIMEOUT"
    if isinstance(error, ValueError):
        code = str(error)
        return code if code in _SAFE_VALIDATION_FAILURES else "FRED_VALIDATION_FAILURE_UNKNOWN"
    return "FRED_COLLECTION_FAILURE_UNKNOWN"


async def collect_fred_vintage(
    request: FredVintageRequest,
    *,
    transport: FredHttpTransport,
    output_dir: Path,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> FredVintageCollection:
    """At most five GETs, no retries; an incomplete directory cannot be replayed."""
    if any(output_dir.iterdir()):
        raise ValueError("FRED_NEW_DIRECTORY_REQUIRED")
    captures: list[FredCapture] = []
    stage = "metadata_fetch"
    offset: int | None = None

    def archive(capture: FredCapture) -> None:
        index = len(captures)
        write_private(output_dir / f"{index:03}.raw.json", capture.raw_payload)
        write_private(
            output_dir / f"{index:03}.receipt.json", json_bytes(capture_receipt(request, capture))
        )
        captures.append(capture)

    try:
        async with asyncio.timeout(60):
            raw = await transport.fetch_vintage(series=request.series, vintage=request.vintage)
            stage = "metadata_validate"
            metadata = FredCapture("series", None, raw, clock())
            archive(metadata)
            # Reject an invalid metadata response before issuing another request.
            metadata_unit(request, metadata)
            offset = 0
            for _ in range(MAX_PAGES):
                stage = "observations_fetch"
                raw = await transport.fetch_vintage(
                    series=request.series,
                    vintage=request.vintage,
                    observation_start=request.observation_start,
                    observation_end=request.observation_end,
                    offset=offset,
                )
                stage = "observations_validate"
                page = FredCapture("observations", offset, raw, clock())
                archive(page)
                validate_captured_pages(request, metadata, tuple(captures[1:]))
                count, size = page_count(raw, expected_offset=offset)
                offset += size
                if offset == count:
                    stage = "complete"
                    result = parse_collected_vintage(request, metadata, tuple(captures[1:]))
                    write_private(
                        output_dir / "complete.json",
                        json_bytes(
                            {
                                "schema": "fred_collection_v1",
                                "request": request.model_dump(mode="json"),
                                "capture_count": len(captures),
                            }
                        ),
                    )
                    return result
            raise ValueError("FRED_PAGE_LIMIT")
    except Exception as error:
        try:
            write_private(
                output_dir / "failure.json",
                json_bytes(
                    {
                        "schema": "fred_collection_failure_v1",
                        "stage": stage,
                        "offset": offset,
                        "code": _safe_failure_code(error),
                        "capture_count": len(captures),
                    }
                ),
            )
        except OSError:
            # Preserve the original provider or validation failure if diagnostics cannot be saved.
            pass
        raise


def _read(path: Path) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_size > 1024 * 1024:
            raise ValueError("FRED_PRIVATE_ORIGINAL_REQUIRED")
        return stream.read(1024 * 1024 + 1)


def has_archived_timeout_failure(directory: Path) -> bool:
    """Only a complete private failure receipt makes a collection timeout known."""
    if (directory / "complete.json").exists():
        return False
    try:
        raw = _read(directory / "failure.json")
        if len(raw) > 512:
            return False
        failure = decode(raw)
        if set(failure) != {
            "schema",
            "stage",
            "offset",
            "code",
            "capture_count",
        }:
            return False
        offset = failure["offset"]
        count = failure["capture_count"]
        return (
            failure["schema"] == "fred_collection_failure_v1"
            and failure["code"] == "FRED_COLLECTION_TIMEOUT"
            and (
                (
                    failure["stage"] == "metadata_fetch"
                    and offset is None
                    and type(count) is int
                    and count == 0
                )
                or (
                    failure["stage"] == "observations_fetch"
                    and type(offset) is int
                    and 0 <= offset <= 32
                    and type(count) is int
                    and 1 <= count <= MAX_PAGES + 1
                )
            )
        )
    except (OSError, ValueError, TypeError):
        return False


def load_fred_collection(directory: Path) -> FredVintageCollection:
    """No trust in a success boolean: reparse each exact original and request receipt."""
    try:
        manifest = decode(_read(directory / "complete.json"))
        count = manifest["capture_count"]
        if (
            manifest["schema"] != "fred_collection_v1"
            or type(count) is not int
            or not 2 <= count <= MAX_PAGES + 1
        ):
            raise ValueError
        request = FredVintageRequest.model_validate(manifest["request"])
        captures = []
        for index in range(count):
            receipt = decode(_read(directory / f"{index:03}.receipt.json"))
            offset, received = receipt["offset"], receipt["received_at"]
            if (offset is not None and type(offset) is not int) or not isinstance(received, str):
                raise ValueError
            capture = FredCapture(
                "series" if index == 0 else "observations",
                offset,
                _read(directory / f"{index:03}.raw.json"),
                datetime.fromisoformat(received),
            )
            if receipt != capture_receipt(request, capture):
                raise ValueError
            captures.append(capture)
        return parse_collected_vintage(request, captures[0], tuple(captures[1:]))
    except (ValueError, TypeError, KeyError, OSError) as error:
        raise ValueError("FRED_ARCHIVE_INVALID") from error
