"""Synthetic original bytes through normal identity and SEC index import entry points."""

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256

from helpers.current_identity import CIK, archives, filing, profile
from nexus.ops.instrument_identity_local import import_identity
from nexus.ops.sec_index_local import import_index


async def import_baseline_identity(
    settings, store, root, *, symbol="AAPL", exchange="NYSE", received=None
):
    assert exchange in {"NYSE", "NASDAQ"}
    raw = filing().replace(b"AAPL", symbol.encode())
    if exchange == "NASDAQ":
        raw = raw.replace(b"exch:XNYS", b"exch:XNAS").replace(
            b"New York Stock Exchange", b"The Nasdaq Stock Market LLC"
        )
    fmp, sec = archives(
        root,
        symbol=symbol,
        fmp=profile(symbol=symbol, exchange=exchange),
        sec=raw,
        received=received.isoformat() if received else None,
    )
    return await import_identity(
        settings, symbol=symbol, fmp_archive=fmp, sec_archive=sec, store=store
    )


async def import_baseline_capture(
    settings,
    store,
    root,
    *,
    identity,
    symbol="AAPL",
    exchange="NYSE",
    received=None,
    sibling_symbols=(),
):
    """No collector/HTTP: import complete synthetic archives and preserve local custody times."""
    received = received or datetime.now(UTC) - timedelta(hours=1)
    accepted = received - timedelta(days=1)
    source_exchange = "Nasdaq" if exchange == "NASDAQ" else exchange
    root.mkdir(parents=True)
    directory = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [int(CIK), "Synthetic", ticker, source_exchange]
            for ticker in (symbol, *sibling_symbols)
        ],
    }
    submissions = {
        "cik": int(CIK),
        "tickers": [symbol, *sibling_symbols],
        "exchanges": [source_exchange] * (1 + len(sibling_symbols)),
        "filings": {
            "files": [],
            "recent": {
                "accessionNumber": [f"{CIK}-26-000001"],
                "form": ["8-K"],
                "acceptanceDateTime": [accepted.isoformat()],
                "filingDate": [accepted.date().isoformat()],
                "primaryDocument": ["first.htm"],
            },
        },
    }
    for name, document, url in (
        ("directory", directory, "https://www.sec.gov/files/company_tickers_exchange.json"),
        (f"{symbol}-submissions", submissions, f"https://data.sec.gov/submissions/CIK{CIK}.json"),
    ):
        raw = json.dumps(document, separators=(",", ":")).encode()
        (root / f"{name}.json").write_bytes(raw)
        (root / f"{name}-receipt.json").write_text(
            json.dumps(
                {
                    "source_url": url,
                    "received_at": received.isoformat(),
                    "sha256": sha256(raw).hexdigest(),
                    "byte_length": len(raw),
                    "http_status": 200,
                }
            )
        )
    return await import_index(
        settings, identity_snapshot_id=identity.snapshot_id, archive_dir=root, store=store
    )
