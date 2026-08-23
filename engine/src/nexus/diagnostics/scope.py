"""Instrument-specific deterministic diagnostic scope guards."""

from nexus.diagnostics.models import DiagnosticItem, MarketEvidence
from nexus.providers.broker.base import PortfolioSnapshot

_FOSUN_ALLOWED_PREFIXES = (
    "CONCENTRATION",
    "RISK_BUCKET",
    "CURRENCY",
    "CASH",
    "LIQUIDITY",
    "VOLATILITY",
    "DRAWDOWN",
    "BETA",
    "CORRELATION",
    "MARKET_RISK",
    "POSITION_MARKET_VALUE",
)


def assert_fosun_read_only_scope(
    snapshot: PortfolioSnapshot,
    market: MarketEvidence,
    items: list[DiagnosticItem],
) -> None:
    """Forbid fundamental recommendation domains for the read-only 00656.HK holding."""
    if not any(
        market.symbols.get(position.instrument_id) == "00656.HK" for position in snapshot.positions
    ):
        return
    if any(not item.diagnostic_code.startswith(_FOSUN_ALLOWED_PREFIXES) for item in items):
        raise ValueError("00656.HK diagnostics cannot contain fundamental buy or sell output")


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
