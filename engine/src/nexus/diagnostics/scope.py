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
