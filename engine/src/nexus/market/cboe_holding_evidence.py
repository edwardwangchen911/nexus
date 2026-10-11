"""Versioned held-only evidence for the narrow Cboe single-stock leveraged ETF branch.

This is the storage contract, not a registration API or execution permission.
Raw source artifacts must remain available for strict readback. Broker account
numbers, quantities and financial values are never copied into this payload.
"""

from datetime import datetime
from hashlib import sha256
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict

from nexus.domain.time import require_utc
from nexus.market.cboe_fund_sources import CboeFundIdentitySources, validate_cboe_fund_sources
from nexus.market.current_etf_identity_storage import (
    CurrentHoldingBinding,
    _validate_frozen_holding_binding,
    current_holding_binding,
)
from nexus.providers.broker.gateway_contracts import GatewayResponsePage

SCHEMA: Literal["cboe_held_fund_evidence_v1"] = "cboe_held_fund_evidence_v1"


class CboeHeldFundIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    symbol: str
    cik: str
    series_id: str
    class_id: str
    legal_name: str
    underlying_symbol: str
    exchange: Literal["BATS"] = "BATS"
    currency: Literal["USD"] = "USD"
    asset_type: Literal["equity_etf"] = "equity_etf"
    strategy_kind: Literal["daily_2x_long_single_stock"] = "daily_2x_long_single_stock"
    leverage_factor: Literal[2] = 2


class CboeHoldingEvidence(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    schema_version: Literal["cboe_held_fund_evidence_v1"] = SCHEMA
    identity: CboeHeldFundIdentity
    checked_at: AwareDatetime
    holding: CurrentHoldingBinding
    source_revision: str
    artifact_sha256: tuple[str, ...]
    company_directory_status: Literal["absent"]
    fund_directory_status: Literal["absent", "exact_match"]
    scope: Literal["fosun_current_holding"] = "fosun_current_holding"
    universe_published: Literal[False] = False
    new_position_eligible: Literal[False] = False
    historical_backtest_eligible: Literal[False] = False
    execution_locked: Literal[True] = True

    @property
    def revision(self) -> str:
        return sha256(self.model_dump_json().encode()).hexdigest()


def _evidence(
    sources: CboeFundIdentitySources, holding: CurrentHoldingBinding, checked_at: datetime
) -> CboeHoldingEvidence:
    filing = sources.source_chain.submission
    _validate_frozen_holding_binding(holding, symbol=filing.symbol, now=checked_at)
    return CboeHoldingEvidence(
        identity=CboeHeldFundIdentity(
            symbol=filing.symbol,
            cik=filing.cik,
            series_id=filing.series_id,
            class_id=filing.class_id,
            legal_name=filing.class_name,
            underlying_symbol=sources.underlying_symbol,
        ),
        checked_at=checked_at,
        holding=holding,
        source_revision=sources.revision,
        artifact_sha256=tuple(
            sha256(raw).hexdigest()
            for raw in sources.source_chain.contents + sources.directory_contents
        ),
        company_directory_status=sources.company_directory_status,
        fund_directory_status=sources.fund_directory_status,
    )


def freeze_cboe_holding_evidence(
    *,
    symbol: str,
    contents: tuple[bytes, ...],
    directory_contents: tuple[bytes, ...],
    accounts_page: GatewayResponsePage,
    holdings_page: GatewayResponsePage,
    now: datetime,
) -> CboeHoldingEvidence:
    try:
        checked_at = require_utc(now)
        holding = current_holding_binding(
            symbol=symbol, accounts_page=accounts_page, holdings_page=holdings_page, now=checked_at
        )
        sources = validate_cboe_fund_sources(
            symbol=symbol, contents=contents, directory_contents=directory_contents, now=checked_at
        )
        return _evidence(sources, holding, checked_at)
    except (ValueError, TypeError, KeyError):
        raise ValueError("CBOE_HOLDING_EVIDENCE_INVALID") from None


def read_cboe_holding_evidence(
    payload: object,
    *,
    contents: tuple[bytes, ...],
    directory_contents: tuple[bytes, ...],
    now: datetime,
) -> CboeHoldingEvidence:
    """Recheck frozen source/exposure facts at registration time, never re-age them.

    A later read proves the recorded identity only; it supplies no current broker
    reconciliation or permission to create a new position.
    """
    try:
        proof = CboeHoldingEvidence.model_validate(payload)
        if proof.checked_at > require_utc(now):
            raise ValueError
        sources = validate_cboe_fund_sources(
            symbol=proof.identity.symbol,
            contents=contents,
            directory_contents=directory_contents,
            now=proof.checked_at,
        )
        expected = _evidence(sources, proof.holding, proof.checked_at)
        if proof != expected:
            raise ValueError
        return proof
    except (ValueError, TypeError, KeyError):
        raise ValueError("CBOE_HOLDING_EVIDENCE_INVALID") from None
