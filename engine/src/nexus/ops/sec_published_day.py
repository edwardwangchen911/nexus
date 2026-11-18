"""Custody of one published SEC daily index version, independent of monitoring effects."""

import json
import os
import re
import stat
import zlib
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from hashlib import sha256
from pathlib import Path
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select, text

from nexus.ops.local_recovery import write_private
from nexus.ops.sec_index_local import _read
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.sec_monitor import SecMonitorRule
from nexus.persistence.session import session_factory
from nexus.providers.http import (
    AllowedOrigin,
    FixedOriginHttpClient,
    HttpPolicyError,
    HttpRequestPolicy,
)
from nexus.research.identity_sources import parse_identity_source
from nexus.research.sec_index_storage import _receipt
from nexus.settings import EngineSettings

SOURCE = "https://www.sec.gov"
MAX_BYTES = 8 * 1024 * 1024
INVALID = "SEC_DAY_INDEX_EVIDENCE_INVALID"
NY = ZoneInfo("America/New_York")
_HEADER = "CIK|Company Name|Form Type|Date Filed|File Name"
_DESCRIPTION = "Description:           Daily Index of EDGAR Dissemination Feed"
_ARCHIVE_PATH = re.compile(r"edgar/data/([0-9]{1,10})/([0-9]{10}-[0-9]{2}-[0-9]{6})\.txt")


def _directory_parts(day: date) -> tuple[str, ...]:
    return ("Archives", "edgar", "daily-index", str(day.year), f"QTR{(day.month - 1) // 3 + 1}")


def _index_name(day: date) -> str:
    return f"master.{day:%Y%m%d}.idx"


def _listed_modified(raw: bytes, day: date) -> datetime:
    document = parse_identity_source(raw)
    directory = document.get("directory")
    expected_name = "/".join(_directory_parts(day)[2:]) + "/"
    if not isinstance(directory, dict) or directory.get("name") != expected_name:
        raise ValueError(INVALID)
    entries = directory.get("item")
    if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
        raise ValueError(INVALID)
    name = _index_name(day)
    selected = [item for item in entries if item.get("name") == name]
    if not selected:
        raise ValueError("SEC_DAY_INDEX_NOT_PUBLISHED")
    if len(selected) != 1 or selected[0].get("type") != "file" or selected[0].get("href") != name:
        raise ValueError(INVALID)
    stamp = selected[0].get("last-modified")
    if not isinstance(stamp, str):
        raise ValueError(INVALID)
    local = datetime.strptime(stamp, "%m/%d/%Y %I:%M:%S %p")
    # A directory listing on a DST fold does not identify a unique HTTP version.
    first, other = local.replace(tzinfo=NY), local.replace(tzinfo=NY, fold=1)
    if first.utcoffset() != other.utcoffset() or local.date() < day:
        raise ValueError(INVALID)
    return first.astimezone(UTC)


def _matching_rows(
    raw: bytes, *, day: date, issuer_cik: str, forms: tuple[str, ...]
) -> tuple[int, list[dict[str, str]]]:
    if not 0 < len(raw) <= MAX_BYTES or any(byte < 32 and byte not in (10, 13) for byte in raw):
        raise ValueError(INVALID)
    lines = raw.decode("latin-1").replace("\r\n", "\n").split("\n")
    if lines[-1] == "":
        lines.pop()
    if len(lines) < 8 or lines[0] != _DESCRIPTION or lines[5] != _HEADER:
        raise ValueError(INVALID)
    label, _, reported = lines[1].partition(":")
    if (
        label != "Last Data Received"
        or datetime.strptime(reported.strip(), "%b %d, %Y").date() != day
        or not re.fullmatch(r"-{20,}", lines[6])
    ):
        raise ValueError(INVALID)
    selected = []
    issuer_entries: dict[str, tuple[str, str]] = {}
    for row in lines[7:]:
        cells = row.split("|")
        if len(cells) != 5:
            raise ValueError(INVALID)
        cik, company, form, filed, path = cells
        match = _ARCHIVE_PATH.fullmatch(path)
        if (
            not company.strip()
            or not re.fullmatch(r"[!-~]+(?: [!-~]+)*", form)
            or not re.fullmatch(r"[0-9]{8}", filed)
            or date.fromisoformat(filed) > day
            or match is None
            or match[1] != cik
            or int(cik) == 0
        ):
            raise ValueError(INVALID)
        if cik.zfill(10) == issuer_cik:
            prior = issuer_entries.setdefault(path, (form, filed))
            if prior != (form, filed) and (prior[0] in forms or form in forms):
                raise ValueError(INVALID)
            if form in forms:
                selected.append(
                    {
                        "form": form,
                        "accession": match[2],
                        "filing_date": date.fromisoformat(filed).isoformat(),
                    }
                )
    return len(lines) - 7, selected


class _SourceResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)
    last_modified: str | None = Field(min_length=20, max_length=64)
    content_length: int | None = Field(ge=1, le=MAX_BYTES)
    content_encoding: Literal["identity", "gzip", "deflate"]
    chunked: bool

    def check_length(self, body: bytes) -> None:
        if (
            not 0 < len(body) <= MAX_BYTES
            or self.chunked == (self.content_length is not None)
            or (self.content_length is not None and self.content_length != len(body))
        ):
            raise ValueError(INVALID)

    def version(self) -> datetime:
        if self.last_modified is None:
            raise ValueError(INVALID)
        modified = parsedate_to_datetime(self.last_modified)
        if modified.utcoffset() is None:
            raise ValueError(INVALID)
        return modified.astimezone(UTC)

    def decode(self, body: bytes) -> bytes:
        self.check_length(body)
        if self.content_encoding == "identity":
            return body
        decoder = zlib.decompressobj(31 if self.content_encoding == "gzip" else 15)
        try:
            decoded = decoder.decompress(body, MAX_BYTES + 1)
        except zlib.error as error:
            raise ValueError(INVALID) from error
        # HTTP framing can complete while the compressed stream is incomplete.
        if (
            not 0 < len(decoded) <= MAX_BYTES
            or not decoder.eof
            or decoder.unconsumed_tail
            or decoder.unused_data
        ):
            raise ValueError(INVALID)
        return decoded


class _SourceClient(FixedOriginHttpClient):
    """Archive a bounded HTTP response without transparent decompression."""

    framing: _SourceResponse | None = None

    async def _read_bounded(self, response: httpx.Response) -> bytes:
        if response.status_code != 200:
            raise HttpPolicyError("STATUS_REJECTED", http_status=response.status_code)
        headers = response.headers
        values = headers.get_list("last-modified")
        lengths = headers.get_list("content-length")
        transfer = [value.lower() for value in headers.get_list("transfer-encoding")]
        encoding = headers.get("content-encoding", "identity").lower()
        if (
            len(values) > 1
            or len(lengths) > 1
            or any(not length.isascii() or not length.isdecimal() for length in lengths)
            or transfer not in ([], ["chunked"])
        ):
            raise ValueError(INVALID)
        try:
            framing = _SourceResponse.model_validate(
                {
                    "last_modified": values[0] if values else None,
                    "content_length": int(lengths[0]) if lengths else None,
                    "content_encoding": encoding,
                    "chunked": transfer == ["chunked"],
                }
            )
            body = bytearray()
            async for chunk in response.aiter_raw():
                if len(body) + len(chunk) > MAX_BYTES:
                    raise ValueError(INVALID)
                body.extend(chunk)
            received = bytes(body)
            framing.check_length(received)
        except ValueError as error:
            raise ValueError(INVALID) from error
        self.framing = framing
        return received


def _archive_folder(root: Path, folder: Path, *, create: bool) -> Path:
    root = root.resolve()
    archive = root / ".secrets"
    target = folder if folder.is_absolute() else root / folder
    if target != target.resolve() or not target.is_relative_to(archive) or target == archive:
        raise ValueError(INVALID)
    parent = target.parent if create else target
    if parent.is_symlink() or not parent.is_dir():
        raise ValueError(INVALID)
    info = parent.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError(INVALID)
    if create:
        target.mkdir(mode=0o700)
    return target


def _save(directory: Path, name: str, raw: bytes, parts: tuple[str, ...], at: datetime) -> None:
    write_private(directory / name, raw)
    write_private(
        directory / (name + ".receipt.json"),
        json.dumps(
            {
                "source_url": f"{SOURCE}/{'/'.join(parts)}",
                "received_at": at.isoformat(),
                "sha256": sha256(raw).hexdigest(),
                "byte_length": len(raw),
                "http_status": 200,
            },
            sort_keys=True,
        ).encode(),
    )


async def _collect(folder: Path, day: date, contact: str, scope: dict[str, object]) -> None:
    parts = _directory_parts(day)
    directory_parts = (*parts, "index.json")
    index_parts = (*parts, _index_name(day))
    headers = (("User-Agent", f"NEXUS {contact}"), ("Accept-Encoding", "gzip, deflate"))
    policy = HttpRequestPolicy(deadline_seconds=12, max_attempts=1, max_response_bytes=MAX_BYTES)
    origin = AllowedOrigin("https", "www.sec.gov")
    try:
        async with httpx.AsyncClient(follow_redirects=False) as transport:
            directory_client = _SourceClient(
                provider_name="sec", policy=policy, origin=origin, client=transport
            )
            directory_wire = await directory_client.get_bytes(directory_parts, (), headers)
            directory_at = datetime.now(UTC)
            directory_framing = directory_client.framing
            if directory_framing is None:
                raise ValueError(INVALID)
            directory = directory_framing.decode(directory_wire)
            if contact.encode() in directory_wire or contact.encode() in directory:
                raise ValueError(INVALID)
            _save(folder, "directory.json.http-body", directory_wire, directory_parts, directory_at)
            listed = _listed_modified(directory, day)
            if listed > directory_at:
                raise ValueError(INVALID)
            index_client = _SourceClient(
                provider_name="sec", policy=policy, origin=origin, client=transport
            )
            wire = await index_client.get_bytes(index_parts, (), headers)
            index_at = datetime.now(UTC)
            framing = index_client.framing
            if framing is None or framing.version() != listed:
                raise ValueError(INVALID)
            raw = framing.decode(wire)
            if contact.encode() in wire or contact.encode() in raw:
                raise ValueError(INVALID)
            _save(folder, _index_name(day) + ".http-body", wire, index_parts, index_at)
            write_private(
                folder / "index-response.json",
                json.dumps(
                    {
                        "scope": scope,
                        "directory_sha256": sha256(directory).hexdigest(),
                        "directory_framing": directory_framing.model_dump(),
                        "index_sha256": sha256(raw).hexdigest(),
                        "framing": framing.model_dump(),
                    },
                    sort_keys=True,
                ).encode(),
            )
    except HttpPolicyError:
        raise ValueError("SEC_DAY_INDEX_FETCH_FAILED") from None


def _verify(
    folder: Path,
    *,
    day: date,
    issuer_cik: str,
    forms: tuple[str, ...],
    scope: dict[str, object],
    now: datetime,
) -> dict[str, object]:
    parts = _directory_parts(day)
    name = _index_name(day)
    directory_wire = _read(folder / "directory.json.http-body", limit=MAX_BYTES)
    wire_name = name + ".http-body"
    wire = _read(folder / wire_name, limit=MAX_BYTES)
    prefix = f"{SOURCE}/{'/'.join(parts)}"
    directory_at = _receipt(
        directory_wire,
        _read(folder / "directory.json.http-body.receipt.json", limit=65536),
        source_url=f"{prefix}/index.json",
        now=now,
    )
    index_at = _receipt(
        wire,
        _read(folder / (wire_name + ".receipt.json"), limit=65536),
        source_url=f"{prefix}/{name}",
        now=now,
    )
    response = parse_identity_source(_read(folder / "index-response.json", limit=65536))
    directory_framing = _SourceResponse.model_validate(response.get("directory_framing"))
    directory = directory_framing.decode(directory_wire)
    modified = _listed_modified(directory, day)
    framing = _SourceResponse.model_validate(response.get("framing"))
    if not modified <= directory_at <= index_at or framing.version() != modified:
        raise ValueError(INVALID)
    raw = framing.decode(wire)
    index_hash = sha256(raw).hexdigest()
    directory_hash = sha256(directory).hexdigest()
    if response != {
        "scope": scope,
        "directory_sha256": directory_hash,
        "directory_framing": directory_framing.model_dump(),
        "index_sha256": index_hash,
        "framing": framing.model_dump(),
    }:
        raise ValueError(INVALID)
    row_count, filings = _matching_rows(raw, day=day, issuer_cik=issuer_cik, forms=forms)
    return {
        "status": "verified_published_index",
        "business_date": day.isoformat(),
        "issuer_cik": issuer_cik,
        "forms": list(forms),
        "source_url": f"{prefix}/{name}",
        "version_modified_at": modified.isoformat(),
        "received_at": index_at.isoformat(),
        "sha256": index_hash,
        "http_body_sha256": sha256(wire).hexdigest(),
        "directory_sha256": directory_hash,
        "rows_verified": row_count,
        "matching_rows": filings,
        "proof_scope": "published_index_version",
        "accepted_time_coverage_proven": False,
        "execution_locked": True,
        "model_requests": 0,
    }


async def verify_published_day(
    settings: EngineSettings,
    *,
    rule_id: UUID,
    day: date,
    root: Path,
    folder: Path,
    collect: bool,
    contact: str | None = None,
) -> dict[str, object]:
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            head = await session.scalar(text("SELECT version_num FROM alembic_version"))
            if head != CURRENT_MIGRATION_HEAD:
                raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
            rule = await session.get(SecMonitorRule, rule_id)
            if (
                rule is None
                or rule.account_id != settings.agent_paper_account_id
                or rule.issuer_cik != "0001045810"
                or set(rule.forms) != {"8-K", "8-K/A"}
            ):
                raise ValueError("SEC_DURABLE_SCOPE_INVALID")
            issuer_cik, forms = rule.issuer_cik, tuple(sorted(rule.forms))
            scope: dict[str, object] = {
                "rule_id": str(rule.id),
                "account_id": str(rule.account_id),
                "instrument_id": str(rule.instrument_id),
                "issuer_cik": issuer_cik,
                "forms": list(forms),
                "business_date": day.isoformat(),
            }
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime) or now.utcoffset() is None:
                raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
            if not rule.effective_at.astimezone(NY).date() <= day < now.astimezone(NY).date():
                raise ValueError(INVALID)
    finally:
        await factory.kw["bind"].dispose()
    if collect and (
        not contact
        or contact != contact.strip()
        or "@" not in contact
        or len(contact.encode()) > 254
        or any(ord(char) < 32 for char in contact)
    ):
        raise ValueError("SEC_MONITOR_CONTACT_REQUIRED")
    archive = _archive_folder(root, folder, create=collect)
    try:
        if collect:
            assert contact is not None
            await _collect(archive, day, contact, scope)
        result = _verify(
            archive,
            day=day,
            issuer_cik=issuer_cik,
            forms=forms,
            scope=scope,
            now=datetime.now(UTC) if collect else now,
        )
    except (OSError, UnicodeError) as error:
        raise ValueError(INVALID) from error
    return {**result, "rule_id": str(rule_id), "provider_requests": 2 if collect else 0}
