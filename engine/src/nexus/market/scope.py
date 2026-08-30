"""Canonical US-equity instrument-master scope shared by every consumer."""

from __future__ import annotations

# The immutable instrument master contains both MIC values and retained legacy aliases.
# Collection, diagnostics, risk, market reads, backtests, and Provider resolvers must
# accept the same closed set so a collected authority cannot fail later only because a
# downstream component recognizes a different spelling.
US_EQUITY_EXCHANGES: tuple[str, ...] = (
    "AMEX",
    "ARCA",
    "ARCX",
    "NASDAQ",
    "NYSE",
    "NYSEARCA",
    "NYSE_ARCA",
    "XNAS",
    "XNYS",
)
