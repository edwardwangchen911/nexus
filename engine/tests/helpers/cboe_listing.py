"""Handwritten synthetic fixtures; no captured source or account data."""

from datetime import UTC, datetime

NOW = datetime(2026, 10, 11, 14, tzinfo=UTC)

HTML = b"""<html><body><h1>Cboe Listed Symbols</h1>
<p>The following symbols are listed on Cboe. This list was last updated as of
2026-10-10 20:00:04 EDT.</p>
<table class="data-table data-table--zebra"><thead><tr>
<th>Symbol</th><th>Volume</th><th>Matched</th><th>Routed</th><th>Bid Size</th>
<th>Bid Price</th><th>Ask Size</th><th>Ask Price</th><th>Last Price</th></tr></thead><tbody>
<tr><td><a href="/us/equities/listings/listed_products/symbols/TEST/">TEST</a></td>
<td>0</td><td>0</td><td>0</td><td>0</td><td>$0.00</td><td>0</td><td>$0.00</td><td>$0.00</td></tr>
</tbody></table></body></html>"""

CSV = (
    b"Name,Volume,Ask Size,Ask Price,Bid Size,Bid Price,Last Price,Shares Matched,Shares Routed\n"
    b"TEST,0,0,0.00,0,0.00,0.00,0,0\n"
)

PRODUCT = b"""<html><main><h1 class="h2">Synthetic Daily 2x ETF</h1>
<h2>Cboe: TEST</h2><p>Listed April 23, 2026</p></main></html>"""
