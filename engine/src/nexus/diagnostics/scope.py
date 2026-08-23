"""Instrument-specific deterministic diagnostic scope guards."""

from uuid import UUID

from nexus.diagnostics.models import DiagnosticItem, MarketEvidence
from nexus.providers.broker.base import PortfolioSnapshot

_ALLOWED_EXACT_CODES = frozenset(
    {
        "CASH_RATIO",
        "CONCENTRATION_HHI",
        "CONCENTRATION_TOP1",
        "CONCENTRATION_TOP3",
        "CORRELATION_CLUSTER",
        "CURRENCY_EXPOSURE",
        "DATA_COMPLETENESS_TRADING_CALENDAR",
        "MARKET_EVIDENCE",
        "POSITION_MARKET_VALUE",
        "SNAPSHOT_ADMISSION",
        "SNAPSHOT_EVIDENCE",
    }
)
_ALLOWED_INSTRUMENT_FAMILIES = frozenset(
    {
        "BETA_126",
        "CORRELATION_126",
        "DRAWDOWN_252",
        "LIQUIDITY_DAYS_TO_EXIT",
        "MARKET_RISK",
        "POSITION_MARKET_VALUE",
        "VOLATILITY_20",
        "VOLATILITY_63",
    }
)


def assert_fosun_read_only_scope(
    snapshot: PortfolioSnapshot,
    market: MarketEvidence,
    items: list[DiagnosticItem],
) -> None:
    """Forbid fundamental recommendation domains for the read-only 00656.HK holding."""
    restricted_ids = {
        position.instrument_id
        for position in snapshot.positions
        if market.identities.get(
            position.instrument_id,
        ) is None
        or market.identities[position.instrument_id].restriction != "STANDARD"
    }
    if not restricted_ids:
        return
    if any(not _allowed_code(item.diagnostic_code, restricted_ids) for item in items):
        raise ValueError("00656.HK diagnostics cannot contain fundamental buy or sell output")


def _allowed_code(code: str, restricted_ids: set[UUID]) -> bool:
    if code in _ALLOWED_EXACT_CODES:
        return True
    family, separator, suffix = code.partition(":")
    if not separator or family not in _ALLOWED_INSTRUMENT_FAMILIES:
        return False
    try:
        instrument_id = UUID(suffix)
    except ValueError:
        return False
    return instrument_id in restricted_ids or family in {
        "BETA_126",
        "CORRELATION_126",
        "DRAWDOWN_252",
        "MARKET_RISK",
        "VOLATILITY_20",
        "VOLATILITY_63",
    }


def has_future_session_conflict(snapshot: PortfolioSnapshot, market: MarketEvidence) -> bool:
    """A bar for a future trading session cannot be known at the snapshot instant."""
    if snapshot.as_of is None:
        return False
    series = (*market.prices.values(),) + (() if market.benchmark is None else (market.benchmark,))
    return any(
        bar.session > snapshot.as_of.date() and bar.available_at <= snapshot.as_of
        for price_series in series
        for bar in price_series.bars
    )
