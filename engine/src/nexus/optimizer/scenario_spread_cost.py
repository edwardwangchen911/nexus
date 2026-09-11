"""Frozen evidence contracts and pure spread-cost calculation for scenarios."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, localcontext
from hashlib import sha256
from importlib.metadata import version as package_version
from typing import Literal
from uuid import UUID

import exchange_calendars as exchange_calendars  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.ingestion import MarketObjectStore, object_uri_matches_key
from nexus.market.intraday_ingestion import canonical_quote_observation
from nexus.ops.alpaca_market_stream import (
    ARCHIVE_SCHEMA,
    MarketStreamArchive,
    MarketStreamBatchArchive,
    MarketStreamError,
    parse_market_stream_batch_raw_payload,
    parse_market_stream_raw_payload,
)
from nexus.optimizer.compare import TypedMetric
from nexus.optimizer.scenario_contracts import (
    PortfolioScenarioEvaluationV2,
    canonical_scenario_hash,
)
from nexus.providers.broker.base import PortfolioSnapshot

_ZERO = Decimal("0")
_TWO = Decimal("2")
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_POLICY_ID = UUID("7733d99e-1594-5f1e-87ea-e5cc23575159")
_POLICY_KEY = "portfolio_scenario_cost_analysis"
_POLICY_VERSION: Literal["scenario_spread_cost_policy_v1"] = "scenario_spread_cost_policy_v1"
_POLICY_SCHEMA_VERSION = "1"
_CALCULATION_VERSION: Literal["scenario_spread_cost_v1"] = "scenario_spread_cost_v1"
_CALENDAR_PAYLOAD = {
    "library": "exchange_calendars",
    "library_version": "4.13.2",
    "calendar": "XNYS",
    "session_window_contract": "xnys_regular_open_closed_v1",
}
_POLICY_PAYLOAD = {
    "purpose": "portfolio_scenario_cost_analysis",
    "selection": "latest_qualified_before_valuation_cutoff",
    "fallback": "disabled",
    "session_scope": "xnys_regular",
    "max_quote_age_seconds": 45,
    "calculation": "sell_half_spread_from_snapshot_mark_v1",
    "calendar_authority": _CALENDAR_PAYLOAD,
    "require_verified_evidence": True,
    "require_empty_quality_flags": True,
    "alpaca": {
        "feed": "iex",
        "delivery_mode": "real_time",
        "transport": "websocket",
        "artifact_schema": ARCHIVE_SCHEMA,
        "market_scope": "US",
        "event_kind": "quote",
    },
}
_ArchiveCache = dict[
    tuple[UUID, str], tuple[bytes, MarketStreamArchive | MarketStreamBatchArchive]
]


class ScenarioSpreadEvidenceReplayInvalid(ValueError):
    """Frozen spread authority is missing, inconsistent, or tampered."""


class _StrictSpreadModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    @model_validator(mode="before")
    @classmethod
    def _reject_binary_floats(cls, value: object) -> object:
        _reject_float(value)
        return value


def _reject_float(value: object) -> None:
    if isinstance(value, float):
        raise TypeError("scenario spread contracts reject binary floats")
    if isinstance(value, Mapping):
        for nested in value.values():
            _reject_float(nested)
    elif isinstance(value, list | tuple | set | frozenset):
        for nested in value:
            _reject_float(nested)


class ScenarioSpreadCalendarAuthorityV1(_StrictSpreadModel):
    library: Literal["exchange_calendars"]
    library_version: Literal["4.13.2"]
    calendar: Literal["XNYS"]
    session_window_contract: Literal["xnys_regular_open_closed_v1"]
    authority_hash: str = Field(pattern=_HASH_PATTERN)
    session_date: date
    session_open: datetime
    session_close: datetime

    @field_validator("session_open", "session_close")
    @classmethod
    def _canonical_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _ordered_window(self) -> ScenarioSpreadCalendarAuthorityV1:
        if self.session_close <= self.session_open:
            raise ValueError("scenario spread session close must follow open")
        return self


class SelectedSpreadQuoteV1(_StrictSpreadModel):
    instrument_id: UUID
    latest_quote_id: UUID
    content_hash: str = Field(pattern=_HASH_PATTERN)
    artifact_id: UUID
    artifact_hash: str = Field(pattern=_HASH_PATTERN)


class ScenarioSpreadEvidenceInputV1(_StrictSpreadModel):
    manifest_schema: Literal["scenario_spread_evidence_manifest_v1"]
    evidence_selected_at: datetime
    policy_status: Literal["available", "unavailable"]
    source_policy_id: UUID | None
    source_policy_version: Literal["scenario_spread_cost_policy_v1"] | None
    source_policy_payload_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    calendar_authority: ScenarioSpreadCalendarAuthorityV1 | None
    calculation_version: Literal["scenario_spread_cost_v1"]
    no_cost_candidate_hash: str = Field(pattern=_HASH_PATTERN)
    scenario_cost_basis_hash: str = Field(pattern=_HASH_PATTERN)
    regulatory_reference_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    selected_quotes: tuple[SelectedSpreadQuoteV1, ...]

    @field_validator("evidence_selected_at")
    @classmethod
    def _canonical_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @field_validator("selected_quotes")
    @classmethod
    def _canonical_quote_order(
        cls, value: tuple[SelectedSpreadQuoteV1, ...]
    ) -> tuple[SelectedSpreadQuoteV1, ...]:
        return tuple(sorted(value, key=lambda quote: quote.instrument_id.int))

    @model_validator(mode="after")
    def _policy_and_quotes_are_consistent(self) -> ScenarioSpreadEvidenceInputV1:
        quote_ids = tuple(quote.instrument_id for quote in self.selected_quotes)
        if len(set(quote_ids)) != len(quote_ids):
            raise ValueError("scenario spread quotes require unique instruments")
        policy_fields = (
            self.source_policy_id,
            self.source_policy_version,
            self.source_policy_payload_hash,
            self.calendar_authority,
        )
        if self.policy_status == "available" and any(value is None for value in policy_fields):
            raise ValueError("available scenario spread policy requires complete authority")
        if self.policy_status == "unavailable" and (
            any(value is not None for value in policy_fields) or self.selected_quotes
        ):
            raise ValueError("unavailable scenario spread policy cannot carry authority or quotes")
        return self


class ScenarioSpreadEvidenceManifestV1(ScenarioSpreadEvidenceInputV1):
    expected_result_hash: str = Field(pattern=_HASH_PATTERN)


class VerifiedSpreadQuoteV1(_StrictSpreadModel):
    instrument_id: UUID
    bid: Decimal
    ask: Decimal

    @model_validator(mode="after")
    def _valid_quote(self) -> VerifiedSpreadQuoteV1:
        if (
            not self.bid.is_finite()
            or not self.ask.is_finite()
            or self.bid <= _ZERO
            or self.ask <= _ZERO
            or self.bid > self.ask
        ):
            raise ValueError("scenario spread quote must be finite, positive, and uncrossed")
        return self


class ScenarioSpreadEvidenceV1(_StrictSpreadModel):
    input: ScenarioSpreadEvidenceInputV1
    quotes: tuple[VerifiedSpreadQuoteV1, ...]
    bundle_hash: str = Field(pattern=_HASH_PATTERN)
    available_at: datetime | None

    @field_validator("quotes")
    @classmethod
    def _canonical_quote_order(
        cls, value: tuple[VerifiedSpreadQuoteV1, ...]
    ) -> tuple[VerifiedSpreadQuoteV1, ...]:
        return tuple(sorted(value, key=lambda quote: quote.instrument_id.int))

    @field_validator("available_at")
    @classmethod
    def _canonical_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)

    @model_validator(mode="after")
    def _valid_bundle(self) -> ScenarioSpreadEvidenceV1:
        quote_ids = tuple(quote.instrument_id for quote in self.quotes)
        if len(set(quote_ids)) != len(quote_ids):
            raise ValueError("scenario spread quotes require unique instruments")
        selected_ids = tuple(quote.instrument_id for quote in self.input.selected_quotes)
        if quote_ids != selected_ids:
            raise ValueError("scenario spread runtime quotes must match the frozen manifest")
        if self.bundle_hash != scenario_spread_bundle_hash(self.input):
            raise ValueError("scenario spread bundle hash mismatch")
        if self.input.policy_status == "available" and self.available_at is None:
            raise ValueError("available scenario spread evidence requires available_at")
        if self.input.policy_status == "unavailable" and (
            self.quotes or self.available_at is not None
        ):
            raise ValueError("unavailable scenario spread evidence cannot carry quotes")
        return self


def canonical_scenario_spread_manifest_hash(
    manifest: ScenarioSpreadEvidenceManifestV1,
) -> str:
    """Hash the complete persisted manifest, including its expected result hash."""
    return canonical_scenario_hash(_spread_hash_payload(manifest))


def finalize_scenario_spread_manifest(
    evidence_input: ScenarioSpreadEvidenceInputV1,
    *,
    expected_result_hash: str,
) -> ScenarioSpreadEvidenceManifestV1:
    """Add the first calculation's result identity to the persisted manifest."""
    return ScenarioSpreadEvidenceManifestV1.model_validate(
        {**evidence_input.model_dump(mode="python"), "expected_result_hash": expected_result_hash}
    )


def scenario_spread_bundle_hash(evidence_input: ScenarioSpreadEvidenceInputV1) -> str:
    """Hash only frozen calculation inputs, avoiding the final-result hash cycle."""
    return canonical_scenario_hash(_spread_hash_payload(evidence_input))


async def select_scenario_spread_evidence(
    session: AsyncSession,
    store: MarketObjectStore,
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
) -> ScenarioSpreadEvidenceV1:
    """Freeze one policy and latest qualified quote set under one SQL snapshot."""
    _require_base_binding(snapshot, base_result)
    authority = _calendar_authority(base_result.valuation_time)
    reduced_ids = tuple(
        position.instrument_id
        for position in base_result.positions
        if position.target_weight < position.current_weight
    )
    rows = (
        (
            await session.execute(
                text(_SELECTION_SQL if reduced_ids else _POLICY_ONLY_SQL),
                {
                    "instrument_ids": list(reduced_ids),
                    "policy_id": _POLICY_ID,
                    "valuation_time": base_result.valuation_time,
                    "session_open": (
                        base_result.valuation_time if authority is None else authority.session_open
                    ),
                },
            )
        )
        .mappings()
        .all()
    )
    if not rows:
        raise ScenarioSpreadEvidenceReplayInvalid("SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID")
    evidence_selected_at = require_utc(rows[0]["evidence_selected_at"])
    if authority is None or not _selected_policy_is_exact(rows[0]):
        return _unavailable_evidence(
            snapshot=snapshot,
            base_result=base_result,
            evidence_selected_at=evidence_selected_at,
        )

    selected: list[SelectedSpreadQuoteV1] = []
    quotes: list[VerifiedSpreadQuoteV1] = []
    archive_cache: _ArchiveCache = {}
    available_times = [require_utc(rows[0]["policy_created_at"])]
    regulatory_available_at = _regulatory_reference_available_at(base_result)
    if regulatory_available_at is not None:
        available_times.append(regulatory_available_at)
    for row in rows:
        if row["quote_id"] is None:
            continue
        identity, quote, row_available_times = await _verify_quote_row(
            store, row, archive_cache=archive_cache
        )
        selected.append(identity)
        quotes.append(quote)
        available_times.extend(row_available_times)

    evidence_input = ScenarioSpreadEvidenceInputV1(
        manifest_schema="scenario_spread_evidence_manifest_v1",
        evidence_selected_at=evidence_selected_at,
        policy_status="available",
        source_policy_id=_POLICY_ID,
        source_policy_version=_POLICY_VERSION,
        source_policy_payload_hash=_canonical_json_hash(_POLICY_PAYLOAD),
        calendar_authority=authority,
        calculation_version=_CALCULATION_VERSION,
        no_cost_candidate_hash=canonical_scenario_hash(base_result),
        scenario_cost_basis_hash=scenario_cost_basis_hash(snapshot, base_result),
        regulatory_reference_hash=_regulatory_reference_hash(base_result),
        selected_quotes=tuple(selected),
    )
    available_at = max(available_times)
    if available_at > base_result.valuation_time:
        raise ScenarioSpreadEvidenceReplayInvalid("SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID")
    return ScenarioSpreadEvidenceV1(
        input=evidence_input,
        quotes=tuple(quotes),
        bundle_hash=scenario_spread_bundle_hash(evidence_input),
        available_at=available_at,
    )


async def replay_scenario_spread_evidence(
    session: AsyncSession,
    store: MarketObjectStore,
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
    manifest: ScenarioSpreadEvidenceManifestV1,
) -> ScenarioSpreadEvidenceV1:
    """Reload only exact manifest dependencies and prove the original result hash."""
    try:
        _require_base_binding(snapshot, base_result)
        evidence_input = ScenarioSpreadEvidenceInputV1.model_validate(
            manifest.model_dump(mode="python", exclude={"expected_result_hash"})
        )
        if (
            evidence_input.no_cost_candidate_hash != canonical_scenario_hash(base_result)
            or evidence_input.scenario_cost_basis_hash
            != scenario_cost_basis_hash(snapshot, base_result)
            or evidence_input.regulatory_reference_hash != _regulatory_reference_hash(base_result)
        ):
            raise ValueError
        if evidence_input.policy_status == "unavailable":
            evidence = ScenarioSpreadEvidenceV1(
                input=evidence_input,
                quotes=(),
                bundle_hash=scenario_spread_bundle_hash(evidence_input),
                available_at=None,
            )
        else:
            evidence = await _replay_available_evidence(
                session=session,
                store=store,
                snapshot=snapshot,
                base_result=base_result,
                manifest=manifest,
                evidence_input=evidence_input,
            )
        result = apply_scenario_spread_cost(
            snapshot=snapshot,
            base_result=base_result,
            evidence=evidence,
        )
        if result.result_hash != manifest.expected_result_hash:
            raise ValueError
        return evidence
    except ScenarioSpreadEvidenceReplayInvalid:
        raise
    except Exception:
        raise ScenarioSpreadEvidenceReplayInvalid(
            "SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID"
        ) from None


async def _replay_available_evidence(
    *,
    session: AsyncSession,
    store: MarketObjectStore,
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
    manifest: ScenarioSpreadEvidenceManifestV1,
    evidence_input: ScenarioSpreadEvidenceInputV1,
) -> ScenarioSpreadEvidenceV1:
    authority = manifest.calendar_authority
    if (
        manifest.source_policy_id != _POLICY_ID
        or manifest.source_policy_version != _POLICY_VERSION
        or manifest.source_policy_payload_hash != _canonical_json_hash(_POLICY_PAYLOAD)
        or manifest.calculation_version != _CALCULATION_VERSION
        or authority is None
        or authority.authority_hash != _canonical_json_hash(_CALENDAR_PAYLOAD)
        or authority.session_date != base_result.valuation_time.date()
        or not authority.session_open <= base_result.valuation_time <= authority.session_close
    ):
        raise ValueError
    rows = (
        (
            await session.execute(
                text(_REPLAY_SQL),
                {
                    "policy_id": _POLICY_ID,
                    "quote_ids": [quote.latest_quote_id for quote in manifest.selected_quotes],
                    "valuation_time": base_result.valuation_time,
                },
            )
        )
        .mappings()
        .all()
    )
    if not rows or not _selected_policy_is_exact(rows[0]):
        raise ValueError
    quote_rows = tuple(row for row in rows if row["quote_id"] is not None)
    if len(quote_rows) != len(manifest.selected_quotes):
        raise ValueError
    expected_by_id = {quote.latest_quote_id: quote for quote in manifest.selected_quotes}
    selected: list[SelectedSpreadQuoteV1] = []
    quotes: list[VerifiedSpreadQuoteV1] = []
    archive_cache: _ArchiveCache = {}
    available_times = [require_utc(rows[0]["policy_created_at"])]
    regulatory_available_at = _regulatory_reference_available_at(base_result)
    if regulatory_available_at is not None:
        available_times.append(regulatory_available_at)
    for row in quote_rows:
        identity, quote, row_available_times = await _verify_quote_row(
            store,
            row,
            expected=expected_by_id[row["quote_id"]],
            archive_cache=archive_cache,
        )
        selected.append(identity)
        quotes.append(quote)
        available_times.extend(row_available_times)
    if tuple(sorted(selected, key=lambda item: item.instrument_id.int)) != manifest.selected_quotes:
        raise ValueError
    available_at = max(available_times)
    if available_at > base_result.valuation_time:
        raise ValueError
    return ScenarioSpreadEvidenceV1(
        input=evidence_input,
        quotes=tuple(quotes),
        bundle_hash=scenario_spread_bundle_hash(evidence_input),
        available_at=available_at,
    )


async def _verify_quote_row(
    store: MarketObjectStore,
    row: RowMapping,
    *,
    expected: SelectedSpreadQuoteV1 | None = None,
    archive_cache: _ArchiveCache | None = None,
) -> tuple[SelectedSpreadQuoteV1, VerifiedSpreadQuoteV1, tuple[datetime, ...]]:
    try:
        instrument_id = _uuid(row["quote_instrument_id"])
        quote_id = _uuid(row["quote_id"])
        artifact_id = _uuid(row["artifact_id"])
        bid = _decimal(row["bid"])
        ask = _decimal(row["ask"])
        observed_at = require_utc(_datetime(row["observed_at"]))
        available_at = require_utc(_datetime(row["available_at"]))
        received_at = require_utc(_datetime(row["received_at"]))
        artifact_observed_at = require_utc(_datetime(row["artifact_observed_at"]))
        artifact_available_at = require_utc(_datetime(row["artifact_available_at"]))
        artifact_received_at = require_utc(_datetime(row["artifact_received_at"]))
        artifact_created_at = require_utc(_datetime(row["artifact_created_at"]))
        quote_created_at = require_utc(_datetime(row["quote_created_at"]))
        quality_flags = row["quality_flags"]
        if not isinstance(quality_flags, list) or any(
            not isinstance(value, str) for value in quality_flags
        ):
            raise ValueError
        artifact_hash = _hash(row["artifact_hash"])
        content_hash = _hash(row["content_hash"])
        revision = _string(row["revision"])
        if (
            row["source"] != "alpaca"
            or row["feed"] != "iex"
            or row["delivery_mode"] != "real_time"
            or row["event_kind"] != "quote"
            or row["evidence_status"] != "verified"
            or quality_flags
            or revision != artifact_hash
            or row["artifact_source"] != "alpaca"
            or row["artifact_schema"] != ARCHIVE_SCHEMA
            or row["mime_type"] != "application/json"
        ):
            raise ValueError
        identity = SelectedSpreadQuoteV1(
            instrument_id=instrument_id,
            latest_quote_id=quote_id,
            content_hash=content_hash,
            artifact_id=artifact_id,
            artifact_hash=artifact_hash,
        )
        if expected is not None and identity != expected:
            raise ValueError
        raw_key = f"raw/market/alpaca/stream/{artifact_received_at:%Y/%m/%d}/{artifact_hash}.json"
        uri = _string(row["uri"])
        if not object_uri_matches_key(uri, raw_key):
            raise ValueError
        cache_key = (artifact_id, artifact_hash)
        cached = None if archive_cache is None else archive_cache.get(cache_key)
        if cached is None:
            raw = await store.read_raw(raw_key, uri=uri)
            try:
                parsed: MarketStreamArchive | MarketStreamBatchArchive = (
                    parse_market_stream_raw_payload(raw)
                )
            except MarketStreamError:
                parsed = parse_market_stream_batch_raw_payload(raw)
            if archive_cache is not None:
                archive_cache[cache_key] = (raw, parsed)
        else:
            raw, parsed = cached
        if len(raw) != row["byte_size"] or sha256(raw).hexdigest() != artifact_hash:
            raise ValueError
        if isinstance(parsed, MarketStreamArchive):
            archive = parsed
            archive_symbol = archive.symbol
            archive_quote = archive.quote
            events = tuple(
                value for value in (archive.trade, archive.quote) if value is not None
            )
            archive_feed = archive.feed
            archive_completed_at = archive.completed_at
            archive_raw_payload = archive.raw_payload
        else:
            batch = parsed
            matching = tuple(item for item in batch.items if item.symbol == row["symbol"])
            if len(matching) != 1:
                raise ValueError
            archive_symbol = matching[0].symbol
            archive_quote = matching[0].quote
            events = tuple(
                value
                for item in batch.items
                for value in (item.trade, item.quote)
                if value is not None
            )
            archive_feed = batch.feed
            archive_completed_at = batch.completed_at
            archive_raw_payload = batch.raw_payload
        if (
            archive_raw_payload != raw
            or archive_feed != "iex"
            or archive_symbol != row["symbol"]
            or archive_quote is None
            or archive_quote.bid != bid
            or archive_quote.ask != ask
            or archive_quote.observed_at != observed_at
            or archive_completed_at != artifact_available_at
            or artifact_available_at != artifact_received_at
            or max(value.observed_at for value in events) != artifact_observed_at
        ):
            raise ValueError
        canonical_hash = sha256(
            canonical_quote_observation(
                instrument_id=instrument_id,
                source="alpaca",
                feed="iex",
                delivery_mode="real_time",
                event_kind="quote",
                bid=archive_quote.bid,
                ask=archive_quote.ask,
                last=None,
                observed_at=observed_at,
                available_at=available_at,
                received_at=received_at,
                revision=revision,
                quality_flags=tuple(quality_flags),
            )
        ).hexdigest()
        if canonical_hash != content_hash:
            raise ValueError
        return (
            identity,
            VerifiedSpreadQuoteV1(instrument_id=instrument_id, bid=bid, ask=ask),
            (
                available_at,
                received_at,
                quote_created_at,
                artifact_available_at,
                artifact_received_at,
                artifact_created_at,
            ),
        )
    except ScenarioSpreadEvidenceReplayInvalid:
        raise
    except Exception:
        raise ScenarioSpreadEvidenceReplayInvalid(
            "SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID"
        ) from None


def _calendar_authority(valuation_time: datetime) -> ScenarioSpreadCalendarAuthorityV1 | None:
    if package_version("exchange-calendars") != _CALENDAR_PAYLOAD["library_version"]:
        return None
    calendar = exchange_calendars.get_calendar("XNYS")
    try:
        session = calendar.date_to_session(valuation_time.date(), direction="none")
    except ValueError:
        return None
    session_open = calendar.session_open(session).to_pydatetime().astimezone(valuation_time.tzinfo)
    session_close = (
        calendar.session_close(session).to_pydatetime().astimezone(valuation_time.tzinfo)
    )
    if not session_open <= valuation_time <= session_close:
        return None
    return ScenarioSpreadCalendarAuthorityV1.model_validate(
        {
            **_CALENDAR_PAYLOAD,
            "authority_hash": _canonical_json_hash(_CALENDAR_PAYLOAD),
            "session_date": valuation_time.date(),
            "session_open": session_open,
            "session_close": session_close,
        }
    )


def _unavailable_evidence(
    *,
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
    evidence_selected_at: datetime,
) -> ScenarioSpreadEvidenceV1:
    evidence_input = ScenarioSpreadEvidenceInputV1(
        manifest_schema="scenario_spread_evidence_manifest_v1",
        evidence_selected_at=evidence_selected_at,
        policy_status="unavailable",
        source_policy_id=None,
        source_policy_version=None,
        source_policy_payload_hash=None,
        calendar_authority=None,
        calculation_version=_CALCULATION_VERSION,
        no_cost_candidate_hash=canonical_scenario_hash(base_result),
        scenario_cost_basis_hash=scenario_cost_basis_hash(snapshot, base_result),
        regulatory_reference_hash=_regulatory_reference_hash(base_result),
        selected_quotes=(),
    )
    return ScenarioSpreadEvidenceV1(
        input=evidence_input,
        quotes=(),
        bundle_hash=scenario_spread_bundle_hash(evidence_input),
        available_at=None,
    )


def _require_base_binding(
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
) -> None:
    if snapshot.id != base_result.portfolio_snapshot_id:
        raise ScenarioSpreadEvidenceReplayInvalid("SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID")
    scenario_cost_basis_hash(snapshot, base_result)


def _selected_policy_is_exact(row: RowMapping) -> bool:
    return bool(
        row["policy_id"] == _POLICY_ID
        and row["policy_key"] == _POLICY_KEY
        and row["policy_version"] == _POLICY_VERSION
        and row["policy_schema_version"] == _POLICY_SCHEMA_VERSION
        and row["policy_payload"] == _POLICY_PAYLOAD
    )


def _regulatory_reference_hash(base_result: PortfolioScenarioEvaluationV2) -> str | None:
    if not (base_result.cost_breakdown.evidence_version or "").startswith(
        "us_regulatory_fee_reference_"
    ):
        return None
    return base_result.cost_breakdown.evidence_hash


def _regulatory_reference_available_at(
    base_result: PortfolioScenarioEvaluationV2,
) -> datetime | None:
    if _regulatory_reference_hash(base_result) is None:
        return None
    return base_result.cost_breakdown.available_at


def _canonical_json_hash(value: Mapping[str, object]) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _uuid(value: object) -> UUID:
    if not isinstance(value, UUID):
        raise TypeError
    return value


def _datetime(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError
    return value


def _decimal(value: object) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError
    return value


def _string(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError
    return value


def _hash(value: object) -> str:
    result = _string(value)
    if len(result) != 64 or any(character not in "0123456789abcdef" for character in result):
        raise ValueError
    return result


_SELECTION_SQL = """
WITH requested AS (
    SELECT unnest(CAST(:instrument_ids AS uuid[])) AS instrument_id
), policy AS (
    SELECT id, key, version, schema_version, payload, created_at
    FROM source_policies
    WHERE id = CAST(:policy_id AS uuid) AND created_at <= :valuation_time
), ranked AS (
    SELECT
        q.id AS quote_id,
        q.instrument_id AS quote_instrument_id,
        q.source,
        q.feed,
        q.delivery_mode,
        q.bid,
        q.ask,
        q.last,
        q.observed_at,
        q.available_at,
        q.received_at,
        q.revision,
        q.quality_flags,
        q.event_kind,
        q.content_hash,
        q.evidence_status,
        q.created_at AS quote_created_at,
        a.id AS artifact_id,
        a.uri,
        a.sha256 AS artifact_hash,
        a.byte_size,
        a.mime_type,
        a.schema_version AS artifact_schema,
        a.source AS artifact_source,
        a.observed_at AS artifact_observed_at,
        a.available_at AS artifact_available_at,
        a.received_at AS artifact_received_at,
        a.created_at AS artifact_created_at,
        (
            SELECT ia.symbol
            FROM instrument_aliases ia
            WHERE ia.instrument_id = q.instrument_id
              AND ia.alias_type = 'ticker'
              AND ia.valid_from <= CAST(q.observed_at AS date)
              AND (ia.valid_to IS NULL OR ia.valid_to >= CAST(q.observed_at AS date))
              AND ia.created_at <= :valuation_time
        ) AS symbol,
        row_number() OVER (
            PARTITION BY q.instrument_id
            ORDER BY q.observed_at DESC, q.available_at DESC, q.received_at DESC, q.id DESC
        ) AS quote_rank
    FROM latest_quotes q
    JOIN requested r ON r.instrument_id = q.instrument_id
    JOIN data_artifacts a ON a.id = q.data_artifact_id
    WHERE q.source = 'alpaca'
      AND q.feed = 'iex'
      AND q.delivery_mode = 'real_time'
      AND q.event_kind = 'quote'
      AND q.evidence_status = 'verified'
      AND q.quality_flags = '[]'::jsonb
      AND q.observed_at BETWEEN :session_open AND :valuation_time
      AND q.available_at <= :valuation_time
      AND q.received_at <= :valuation_time
      AND q.created_at <= :valuation_time
      AND a.created_at <= :valuation_time
      AND a.observed_at <= :valuation_time
      AND a.available_at <= :valuation_time
      AND a.received_at <= :valuation_time
      AND a.source = 'alpaca'
      AND a.schema_version = 'alpaca_stock_stream_v1'
      AND a.mime_type = 'application/json'
      AND :valuation_time - q.observed_at <= interval '45 seconds'
)
SELECT
    statement_timestamp() AS evidence_selected_at,
    r.instrument_id AS requested_instrument_id,
    p.id AS policy_id,
    p.key AS policy_key,
    p.version AS policy_version,
    p.schema_version AS policy_schema_version,
    p.payload AS policy_payload,
    p.created_at AS policy_created_at,
    ranked.*
FROM requested r
LEFT JOIN policy p ON true
LEFT JOIN ranked
  ON ranked.quote_instrument_id = r.instrument_id AND ranked.quote_rank = 1
ORDER BY r.instrument_id
"""

_POLICY_ONLY_SQL = """
WITH policy AS (
    SELECT id, key, version, schema_version, payload, created_at
    FROM source_policies
    WHERE id = CAST(:policy_id AS uuid) AND created_at <= :valuation_time
)
SELECT
    statement_timestamp() AS evidence_selected_at,
    NULL::uuid AS requested_instrument_id,
    p.id AS policy_id,
    p.key AS policy_key,
    p.version AS policy_version,
    p.schema_version AS policy_schema_version,
    p.payload AS policy_payload,
    p.created_at AS policy_created_at,
    NULL::uuid AS quote_id
FROM (SELECT 1) AS anchor
LEFT JOIN policy p ON true
"""

_REPLAY_SQL = """
WITH policy AS (
    SELECT id, key, version, schema_version, payload, created_at
    FROM source_policies
    WHERE id = CAST(:policy_id AS uuid) AND created_at <= :valuation_time
), requested AS (
    SELECT unnest(CAST(:quote_ids AS uuid[])) AS quote_id
), exact_quotes AS (
    SELECT
        q.id AS quote_id,
        q.instrument_id AS quote_instrument_id,
        q.source,
        q.feed,
        q.delivery_mode,
        q.bid,
        q.ask,
        q.last,
        q.observed_at,
        q.available_at,
        q.received_at,
        q.revision,
        q.quality_flags,
        q.event_kind,
        q.content_hash,
        q.evidence_status,
        q.created_at AS quote_created_at,
        a.id AS artifact_id,
        a.uri,
        a.sha256 AS artifact_hash,
        a.byte_size,
        a.mime_type,
        a.schema_version AS artifact_schema,
        a.source AS artifact_source,
        a.observed_at AS artifact_observed_at,
        a.available_at AS artifact_available_at,
        a.received_at AS artifact_received_at,
        a.created_at AS artifact_created_at,
        (
            SELECT ia.symbol
            FROM instrument_aliases ia
            WHERE ia.instrument_id = q.instrument_id
              AND ia.alias_type = 'ticker'
              AND ia.valid_from <= CAST(q.observed_at AS date)
              AND (ia.valid_to IS NULL OR ia.valid_to >= CAST(q.observed_at AS date))
              AND ia.created_at <= :valuation_time
        ) AS symbol
    FROM requested r
    JOIN latest_quotes q ON q.id = r.quote_id
    JOIN data_artifacts a ON a.id = q.data_artifact_id
)
SELECT
    statement_timestamp() AS evidence_selected_at,
    p.id AS policy_id,
    p.key AS policy_key,
    p.version AS policy_version,
    p.schema_version AS policy_schema_version,
    p.payload AS policy_payload,
    p.created_at AS policy_created_at,
    exact_quotes.*
FROM (SELECT 1) AS anchor
LEFT JOIN policy p ON true
LEFT JOIN exact_quotes ON true
ORDER BY exact_quotes.quote_instrument_id
"""


def _spread_hash_payload(value: BaseModel) -> Mapping[str, object]:
    payload = value.model_dump(mode="python")
    calendar = payload.get("calendar_authority")
    if isinstance(calendar, dict) and isinstance(calendar.get("session_date"), date):
        calendar["session_date"] = calendar["session_date"].isoformat()
    return payload


def scenario_cost_basis_hash(
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
) -> str:
    """Bind spread calculation to snapshot marks, portfolio value, and target weights."""
    if (
        snapshot.id != base_result.portfolio_snapshot_id
        or snapshot.as_of is None
        or snapshot.as_of != base_result.valuation_time
    ):
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")

    snapshot_positions = {
        position.instrument_id: position
        for position in snapshot.positions
        if position.currency == "USD"
    }
    if len(snapshot_positions) != len(
        tuple(position for position in snapshot.positions if position.currency == "USD")
    ):
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")

    position_values: dict[UUID, Decimal] = {}
    basis_positions: list[dict[str, object]] = []
    for position in sorted(base_result.positions, key=lambda item: item.instrument_id.int):
        snapshot_position = snapshot_positions.get(position.instrument_id)
        if snapshot_position is None or snapshot_position.current_price is None:
            raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")
        price = snapshot_position.current_price
        value = snapshot_position.quantity * price
        if not price.is_finite() or price <= _ZERO or not value.is_finite():
            raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")
        position_values[position.instrument_id] = value
        basis_positions.append(
            {
                "instrument_id": position.instrument_id,
                "snapshot_current_price": price,
                "current_weight": position.current_weight,
                "target_weight": position.target_weight,
            }
        )

    cash = sum(
        (
            item.cash_balance
            for item in snapshot.cash
            if item.currency == "USD" and item.cash_balance.is_finite()
        ),
        _ZERO,
    )
    portfolio_value = cash + sum(position_values.values(), _ZERO)
    if not portfolio_value.is_finite() or portfolio_value <= _ZERO:
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")
    return canonical_scenario_hash(
        {
            "schema_version": "scenario_cost_basis_hash_v1",
            "portfolio_snapshot_id": snapshot.id,
            "valuation_time": base_result.valuation_time,
            "portfolio_value": portfolio_value,
            "positions": tuple(basis_positions),
        }
    )


def _instrument_spread_ratio(
    *,
    sold_weight: Decimal,
    snapshot_current_price: Decimal,
    bid: Decimal,
    ask: Decimal,
) -> Decimal:
    midpoint = (bid + ask) / _TWO
    return sold_weight * (midpoint - bid) / snapshot_current_price


def apply_scenario_spread_cost(
    *,
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
    evidence: ScenarioSpreadEvidenceV1,
) -> PortfolioScenarioEvaluationV2:
    """Apply an all-or-nothing frozen half-spread estimate to a no-cost scenario."""
    if snapshot.id != base_result.portfolio_snapshot_id or (
        scenario_cost_basis_hash(snapshot, base_result) != evidence.input.scenario_cost_basis_hash
    ):
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")
    if canonical_scenario_hash(base_result) != evidence.input.no_cost_candidate_hash:
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")

    reduced = tuple(
        position
        for position in base_result.positions
        if position.target_weight < position.current_weight
    )
    quotes = {quote.instrument_id: quote for quote in evidence.quotes}
    spread = TypedMetric.unknown("BID_ASK_EVIDENCE_UNAVAILABLE")
    if (
        evidence.input.policy_status == "available"
        and reduced
        and set(quotes) == {position.instrument_id for position in reduced}
    ):
        snapshot_prices = {
            position.instrument_id: position.current_price
            for position in snapshot.positions
            if position.current_price is not None
        }
        with localcontext() as context:
            context.prec = 28
            context.rounding = ROUND_HALF_UP
            spread = TypedMetric.known(
                sum(
                    (
                        _instrument_spread_ratio(
                            sold_weight=position.current_weight - position.target_weight,
                            snapshot_current_price=snapshot_prices[position.instrument_id],
                            bid=quotes[position.instrument_id].bid,
                            ask=quotes[position.instrument_id].ask,
                        )
                        for position in reduced
                    ),
                    _ZERO,
                )
            )

    breakdown = base_result.cost_breakdown.model_copy(
        update={
            "evidence_version": (
                "scenario_cost_evidence_bundle_v1"
                if spread.status == "known"
                else base_result.cost_breakdown.evidence_version
            ),
            "evidence_hash": (
                evidence.bundle_hash
                if spread.status == "known"
                else base_result.cost_breakdown.evidence_hash
            ),
            "available_at": (
                evidence.available_at
                if spread.status == "known"
                else base_result.cost_breakdown.available_at
            ),
            "spread": spread,
        }
    )
    candidate = base_result.candidate.model_copy(
        update={"estimated_cost": TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE")}
    )
    updated = base_result.model_copy(update={"cost_breakdown": breakdown, "candidate": candidate})
    return updated.model_copy(update={"result_hash": canonical_scenario_hash(updated)})
