"""Collect bounded Nasdaq instrument risk proofs without model or trading effects."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Never
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
from pydantic import Field, SecretStr
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.ops.alpaca_actions_local import collect_actions
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
)
from nexus.providers.corporate_actions.alpaca_http import CorporateActionsRequest
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.providers.news import validate_symbol
from nexus.research.alpaca_actions_archive import load_action_collection
from nexus.risk.authority import canonical_query_authority_hash
from nexus.risk.query_authority_producer import (
    RiskSourceProof,
    inspect_query_authority_scope,
    parse_nasdaq_halt_feed,
    parse_nasdaq_listing_directory,
    persist_query_authorities,
)
from nexus.settings import EngineSettings

_NASDAQ_ORIGIN = AllowedOrigin(scheme="https", host="www.nasdaqtrader.com")
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


@dataclass(frozen=True)
class RiskCandidate:
    symbol: str
    instrument_id: UUID

    def __post_init__(self) -> None:
        validate_symbol(self.symbol)
        if not isinstance(self.instrument_id, UUID):
            raise ValueError("AGENT_RISK_CANDIDATE_INVALID")


@dataclass(frozen=True)
class _SharedNasdaqSources:
    listing: bytes
    listing_received: datetime
    halt: bytes
    halt_received: datetime


async def collect_source_proofs_batch(
    *,
    candidates: tuple[RiskCandidate, ...],
    decision_session: date,
    key_id: SecretStr,
    secret: SecretStr,
    output_dir: Path,
    cutoff_at: datetime | None,
) -> dict[UUID, tuple[RiskSourceProof, ...]]:
    """Collect shared Nasdaq bytes once and bounded actions for each exact candidate."""
    if (
        not 1 <= len(candidates) <= 5
        or len({c.instrument_id for c in candidates}) != len(candidates)
        or len({c.symbol for c in candidates}) != len(candidates)
    ):
        raise ValueError("AGENT_RISK_CANDIDATE_INVALID")
    try:
        output_dir.mkdir(mode=0o700)
    except OSError as error:
        raise ValueError("AGENT_RISK_NEW_OUTPUT_REQUIRED") from error
    try:
        async with httpx.AsyncClient(follow_redirects=False, timeout=30) as http:
            client = FixedOriginHttpClient(
                provider_name="nasdaq_trader",
                origin=_NASDAQ_ORIGIN,
                policy=HttpRequestPolicy(max_attempts=1, max_response_bytes=8 * 1024 * 1024),
                client=http,
            )
            listing = await client.get_bytes(("dynamic", "SymDir", "nasdaqlisted.txt"), (), ())
            listing_received = datetime.now(UTC)
            write_private(output_dir / "nasdaqlisted.txt", listing)
            write_private(
                output_dir / "nasdaqlisted.receipt.json",
                _receipt(
                    source="nasdaq_trader",
                    path="/dynamic/SymDir/nasdaqlisted.txt",
                    raw=listing,
                    received_at=listing_received,
                ),
            )
            halt = await client.get_bytes(("rss.aspx",), (("feed", "tradehalts"),), ())
            halt_received = datetime.now(UTC)
            write_private(output_dir / "tradehalts.xml", halt)
            write_private(
                output_dir / "tradehalts.receipt.json",
                _receipt(
                    source="nasdaq_trader",
                    path="/rss.aspx?feed=tradehalts",
                    raw=halt,
                    received_at=halt_received,
                ),
            )
        if cutoff_at is not None and max(listing_received, halt_received) > cutoff_at:
            raise ValueError("AGENT_RISK_COLLECTION_WINDOW_INVALID")
        shared = _SharedNasdaqSources(listing, listing_received, halt, halt_received)
        result = {}
        for candidate in sorted(candidates, key=lambda c: c.instrument_id.int):
            result[candidate.instrument_id] = await collect_source_proofs(
                symbol=candidate.symbol,
                instrument_id=candidate.instrument_id,
                decision_session=decision_session,
                key_id=key_id,
                secret=secret,
                output_dir=output_dir / str(candidate.instrument_id),
                cutoff_at=cutoff_at,
                _shared_sources=shared,
            )
        return result
    except (Exception, KeyboardInterrupt) as error:
        if isinstance(error, ValueError) and str(error) == "AGENT_RISK_COLLECTION_WINDOW_INVALID":
            raise
        raise ValueError("AGENT_RISK_SOURCE_COLLECTION_FAILED") from error


class AgentRiskSettings(EngineSettings):
    alpaca_key: SecretStr = Field(validation_alias="ALPACA_API_KEY_ID")
    alpaca_secret: SecretStr = Field(validation_alias="ALPACA_API_SECRET_KEY")


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise ValueError("AGENT_RISK_ARGUMENT_INVALID")


def _utc_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
            raise ValueError
        return parsed
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError from error


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _receipt(*, source: str, path: str, raw: bytes, received_at: datetime) -> bytes:
    return _json_bytes(
        {
            "schema": "nexus_agent_risk_source_receipt_v1",
            "source": source,
            "path": path,
            "sha256": sha256(raw).hexdigest(),
            "byte_length": len(raw),
            "received_at": received_at.isoformat(),
        }
    )


async def collect_source_proofs(
    *,
    symbol: str,
    instrument_id: UUID,
    decision_session: date,
    key_id: SecretStr,
    secret: SecretStr,
    output_dir: Path,
    cutoff_at: datetime | None,
    _shared_sources: _SharedNasdaqSources | None = None,
) -> tuple[RiskSourceProof, ...]:
    """Archive exact provider bytes and reject receipts after an explicit cutoff."""
    try:
        output_dir.mkdir(mode=0o700)
        actions_dir = output_dir / "actions"
        actions_dir.mkdir(mode=0o700)
    except OSError as error:
        raise ValueError("AGENT_RISK_NEW_OUTPUT_REQUIRED") from error

    proofs: list[RiskSourceProof] = []
    try:
        result = await collect_actions(
            CorporateActionsRequest(symbol, decision_session, decision_session),
            key_id=key_id,
            secret=secret,
            output_dir=actions_dir,
            max_pages=10,
        )
        if (
            result.get("status") != "collected"
            or result.get("pagination_exhausted") is not True
            or result.get("source_completeness") != "unknown"
            or result.get("execution_locked") is not True
            or result.get("historical_backtest_eligible") is not False
        ):
            raise ValueError
        action_archive = load_action_collection(
            actions_dir,
            instrument_id=instrument_id,
            symbol=symbol,
            now=datetime.now(UTC),
        )
        if cutoff_at is not None and action_archive.document.available_at > cutoff_at:
            raise ValueError("AGENT_RISK_COLLECTION_WINDOW_INVALID")
        proofs.append(
            RiskSourceProof(
                "corporate_action",
                bool(action_archive.document.events),
                action_archive.document.manifest_content_hash,
                action_archive.document.available_at,
            )
        )

        async with httpx.AsyncClient(follow_redirects=False, timeout=30) as http:
            client = FixedOriginHttpClient(
                provider_name="nasdaq_trader",
                origin=_NASDAQ_ORIGIN,
                policy=HttpRequestPolicy(max_attempts=1, max_response_bytes=8 * 1024 * 1024),
                client=http,
            )
            listing_raw = (
                _shared_sources.listing
                if _shared_sources is not None
                else await client.get_bytes(("dynamic", "SymDir", "nasdaqlisted.txt"), (), ())
            )
            listing_received = (
                _shared_sources.listing_received
                if _shared_sources is not None
                else datetime.now(UTC)
            )
            write_private(output_dir / "nasdaqlisted.txt", listing_raw)
            write_private(
                output_dir / "nasdaqlisted.receipt.json",
                _receipt(
                    source="nasdaq_trader",
                    path="/dynamic/SymDir/nasdaqlisted.txt",
                    raw=listing_raw,
                    received_at=listing_received,
                ),
            )
            listing = parse_nasdaq_listing_directory(
                listing_raw,
                symbol=symbol,
                decision_session=decision_session,
                fetched_at=listing_received,
            )
            proofs.append(
                RiskSourceProof(
                    "delisting",
                    listing.blocked,
                    sha256(listing_raw).hexdigest(),
                    listing_received,
                )
            )

            halt_raw = (
                _shared_sources.halt
                if _shared_sources is not None
                else await client.get_bytes(("rss.aspx",), (("feed", "tradehalts"),), ())
            )
            halt_received = (
                _shared_sources.halt_received if _shared_sources is not None else datetime.now(UTC)
            )
            write_private(output_dir / "tradehalts.xml", halt_raw)
            write_private(
                output_dir / "tradehalts.receipt.json",
                _receipt(
                    source="nasdaq_trader",
                    path="/rss.aspx?feed=tradehalts",
                    raw=halt_raw,
                    received_at=halt_received,
                ),
            )
            if cutoff_at is not None and halt_received > cutoff_at:
                raise ValueError("AGENT_RISK_COLLECTION_WINDOW_INVALID")
            halt = parse_nasdaq_halt_feed(halt_raw, symbol=symbol, fetched_at=halt_received)
            proofs.append(
                RiskSourceProof(
                    "suspension",
                    halt.blocked,
                    sha256(halt_raw).hexdigest(),
                    halt.published_at,
                )
            )
        ordered = tuple(sorted(proofs, key=lambda proof: proof.kind))
        write_private(
            output_dir / "manifest.json",
            _json_bytes(
                {
                    "schema": "nexus_agent_risk_collection_v1",
                    "status": "collected",
                    "symbol": symbol,
                    "decision_session": decision_session.isoformat(),
                    "proofs": [
                        {
                            "kind": proof.kind,
                            "blocked": proof.blocked,
                            "source_watermark_hash": proof.source_watermark_hash,
                            "available_at": proof.available_at.isoformat(),
                        }
                        for proof in ordered
                    ],
                    "execution_locked": True,
                    "model_called": False,
                }
            ),
        )
        return ordered
    except (Exception, KeyboardInterrupt) as error:
        try:
            write_private(
                output_dir / "manifest.json",
                _json_bytes(
                    {
                        "schema": "nexus_agent_risk_collection_v1",
                        "status": "failed",
                        "symbol": symbol,
                        "decision_session": decision_session.isoformat(),
                        "execution_locked": True,
                        "model_called": False,
                    }
                ),
            )
        except OSError:
            pass
        if isinstance(error, ValueError) and str(error) == "AGENT_RISK_COLLECTION_WINDOW_INVALID":
            raise
        raise ValueError("AGENT_RISK_SOURCE_COLLECTION_FAILED") from error


def _verified_result(
    records: tuple[Any, ...], *, instrument_id: UUID, decision_time: datetime
) -> dict[str, object]:
    kinds = {"corporate_action", "suspension", "delisting"}
    if (
        len(records) != 3
        or {record.kind for record in records} != kinds
        or any(
            not record.complete
            or record.required_instrument_ids != [instrument_id]
            or not set(record.blocked_instrument_ids) <= {instrument_id}
            or record.available_at > decision_time
            or (
                record.kind == "suspension"
                and decision_time - record.available_at > timedelta(minutes=2)
            )
            or canonical_query_authority_hash(
                source_policy_id=record.source_policy_id,
                universe_snapshot_id=record.universe_snapshot_id,
                decision_session=record.decision_session,
                kind=record.kind,
                required_instrument_ids=record.required_instrument_ids,
                blocked_instrument_ids=record.blocked_instrument_ids,
                complete=record.complete,
                source_watermark_hash=record.source_watermark_hash,
                available_at=record.available_at,
            )
            != record.content_hash
            for record in records
        )
    ):
        raise ValueError("RISK_QUERY_AUTHORITY_CONFLICT")
    return {
        "status": "verified",
        "authority_count": 3,
        "blocked_kinds": sorted(record.kind for record in records if record.blocked_instrument_ids),
        "execution_locked": True,
        "model_called": False,
    }


async def run_once(
    settings: AgentRiskSettings,
    *,
    decision_session: date,
    decision_time: datetime,
    output_dir: Path,
    now: datetime | None = None,
    source_policy_id: UUID = PAPER_DECISION_SOURCE_POLICY_ID,
) -> dict[str, object]:
    current = now or datetime.now(UTC)
    if (
        current.tzinfo is None
        or current.utcoffset() != UTC.utcoffset(current)
        or decision_time.tzinfo is None
        or decision_time.utcoffset() != UTC.utcoffset(decision_time)
        or decision_time.astimezone(ZoneInfo("America/New_York")).date() != decision_session
        or source_policy_id
        not in {PAPER_DECISION_SOURCE_POLICY_ID, PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID}
        or not settings.database_url
    ):
        raise ValueError("AGENT_RISK_SESSION_INVALID")
    database = make_url(settings.database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in _LOCAL_HOSTS
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise ValueError("AGENT_RISK_DATABASE_NOT_LOCAL")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            calendar_query = (
                select(ExchangeCalendarAuthorityRecord.id)
                .join(
                    ExchangeCalendarSessionRecord,
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == ExchangeCalendarAuthorityRecord.id,
                )
                .where(
                    ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                    ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                    ExchangeCalendarAuthorityRecord.published_at <= decision_time,
                    ExchangeCalendarSessionRecord.session == decision_session,
                )
            )
            if source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID:
                calendar_query = calendar_query.where(
                    ExchangeCalendarSessionRecord.decision_at == decision_time
                )
            else:
                local_decision = decision_time.astimezone(ZoneInfo("America/New_York"))
                if not 7 <= local_decision.hour < 16:
                    raise ValueError("AGENT_RISK_SESSION_INVALID")
            calendar_authority_id = await session.scalar(
                calendar_query.order_by(
                    ExchangeCalendarAuthorityRecord.published_at.desc(),
                    ExchangeCalendarAuthorityRecord.id.desc(),
                ).limit(1)
            )
            if calendar_authority_id is None:
                raise ValueError("AGENT_RISK_SESSION_INVALID")
            scope = await inspect_query_authority_scope(
                session,
                decision_session=decision_session,
                symbol="AAPL",
                as_of=current,
                source_policy_id=source_policy_id,
            )
            if scope.existing:
                return _verified_result(
                    scope.existing,
                    instrument_id=scope.instrument_id,
                    decision_time=decision_time,
                )
        if not current <= decision_time <= current + timedelta(minutes=2):
            raise ValueError("AGENT_RISK_COLLECTION_WINDOW_INVALID")
        proofs = await collect_source_proofs(
            symbol="AAPL",
            instrument_id=scope.instrument_id,
            decision_session=decision_session,
            key_id=settings.alpaca_key,
            secret=settings.alpaca_secret,
            output_dir=output_dir,
            cutoff_at=decision_time,
        )
        suspension = next(proof for proof in proofs if proof.kind == "suspension")
        if any(
            proof.available_at > decision_time for proof in proofs
        ) or decision_time - suspension.available_at > timedelta(minutes=2):
            raise ValueError("AGENT_RISK_COLLECTION_WINDOW_INVALID")
        async with factory.begin() as session:
            records = await persist_query_authorities(
                session,
                universe_snapshot_id=scope.universe_snapshot_id,
                decision_session=decision_session,
                symbol="AAPL",
                proofs=proofs,
                source_policy_id=source_policy_id,
            )
        return _verified_result(
            records,
            instrument_id=scope.instrument_id,
            decision_time=decision_time,
        )
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(prog="nexus-engine-agent-risk-authority")
    parser.add_argument("--decision-session", type=date.fromisoformat, required=True)
    parser.add_argument("--decision-time", type=_utc_datetime, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--same-session-schwab", action="store_true")
    try:
        args = parser.parse_args(argv)
        settings = AgentRiskSettings.model_validate(load_local_environment(Path.cwd(), {}))
        source_policy_id = (
            PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
            if args.same_session_schwab
            else PAPER_DECISION_SOURCE_POLICY_ID
        )
        result = asyncio.run(
            run_once(
                settings,
                decision_session=args.decision_session,
                decision_time=args.decision_time,
                output_dir=args.output_dir,
                source_policy_id=source_policy_id,
            )
        )
    except (Exception, KeyboardInterrupt) as error:
        allowed = {
            "AGENT_RISK_DATABASE_NOT_LOCAL",
            "AGENT_RISK_ARGUMENT_INVALID",
            "AGENT_RISK_COLLECTION_WINDOW_INVALID",
            "AGENT_RISK_NEW_OUTPUT_REQUIRED",
            "AGENT_RISK_SESSION_INVALID",
            "AGENT_RISK_SOURCE_COLLECTION_FAILED",
            "RISK_QUERY_AUTHORITY_CONFLICT",
            "RISK_QUERY_AUTHORITY_SCOPE_INVALID",
        }
        code = str(error) if str(error) in allowed else "AGENT_RISK_RESULT_UNKNOWN"
        print(json.dumps({"error_code": code}, sort_keys=True, separators=(",", ":")))
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
