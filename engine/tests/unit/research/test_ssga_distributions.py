"""Synthetic issuer workbook and identity cases; no account data or real archives."""

import io
import json
from datetime import UTC, datetime
from html import escape
from uuid import UUID
from zipfile import ZipFile

import pytest
from nexus.research.ssga_distributions import parse_distribution

HEADERS = (
    "FUND NAME",
    "TICKER",
    "CUSIP",
    "EX-DATE",
    "RECORD DATE",
    "PAYABLE DATE",
    "DIVIDEND ($)",
    "SHORT TERM CAPITAL GAIN ($)",
    "LONG TERM CAPITAL GAIN ($)",
    "FREQUENCY",
)


def workbook(*, changes=None, duplicate=False):
    values = [
        "Synthetic Fund",
        "TEST",
        "000000001",
        "01/10/2026",
        "01/10/2026",
        "01/15/2026",
        "0.125000",
        "",
        "",
        "Quarterly",
    ]
    for index, value in (changes or {}).items():
        values[index] = value
    strings = ["", *HEADERS, *values]
    namespace = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
    rows = []
    for number, cells in enumerate([list(HEADERS), values, *([values] if duplicate else [])], 1):
        content = "".join(
            f'<c r="{chr(65 + i)}{number}" t="s"><v>{strings.index(v)}</v></c>'
            for i, v in enumerate(cells)
        )
        rows.append(f'<row r="{number}">{content}</row>')
    output = io.BytesIO()
    with ZipFile(output, "w") as z:
        z.writestr(
            "xl/sharedStrings.xml",
            f"<sst {namespace}>"
            + "".join(f"<si><t>{escape(s)}</t></si>" for s in strings)
            + "</sst>",
        )
        z.writestr(
            "xl/worksheets/sheet1.xml",
            f"<worksheet {namespace}><sheetData>" + "".join(rows) + "</sheetData></worksheet>",
        )
        z.writestr(
            "xl/workbook.xml",
            f"<workbook {namespace}><sheets>"
            '<sheet name="dividend" sheetId="1"/></sheets></workbook>',
        )
    return output.getvalue()


def identity(symbol="TEST", isin="US0000000010"):
    info = {"attrs": {"fund-ticker": {"value": symbol}, "isin": {"value": isin}}}
    return (
        f'<input id="fund-info-ticker" value="{symbol}">'
        f'<input id="fund-quick-info" value="{escape(json.dumps(info), quote=True)}">'
    ).encode()


def parse(**changes):
    args = dict(
        raw=workbook(),
        identity_html=identity(),
        instrument_id=UUID(int=31),
        symbol="TEST",
        identity_url="https://www.ssga.com/us/en/individual/etfs/synthetic-fund-test",
        received_at=datetime(2026, 1, 20, tzinfo=UTC),
    )
    return parse_distribution(**(args | changes))


def test_exact_dividend_dates_and_empty_shared_string_are_preserved():
    document = parse()
    assert str(document.dividend_per_share) == "0.125000"
    assert document.short_term_capital_gain is None and document.long_term_capital_gain is None
    assert document.ex_date.isoformat() == "2026-01-10"
    assert document.payable_date.isoformat() == "2026-01-15"
    assert document.available_at == datetime(2026, 1, 20, tzinfo=UTC)
    assert document.historical_backtest_eligible is False


@pytest.mark.parametrize(
    "changes",
    [
        {"identity_html": identity("OTHER")},
        {"identity_html": identity(isin="US0000000028")},
        {"identity_url": "https://unrelated.invalid/fund"},
        {"raw": workbook(changes={5: "01/01/2026"})},
        {"raw": workbook(changes={6: "NaN"})},
        {"raw": workbook(changes={6: "-1"})},
        {"raw": workbook(duplicate=True)},
        {"raw": b"not an xlsx"},
        {"received_at": datetime(2026, 1, 20)},
    ],
)
def test_wrong_identity_dates_amount_or_archive_fails_closed(changes):
    with pytest.raises(ValueError):
        parse(**changes)
