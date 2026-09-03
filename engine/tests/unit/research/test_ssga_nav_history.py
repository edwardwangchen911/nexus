"""Synthetic OOXML only; no archived issuer identity or observations."""

import io
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from html import escape
from uuid import UUID
from zipfile import ZipFile

import pytest
from nexus.research.ssga_nav_history import parse_nav_history


def originals(*, rows=None, ticker="TEST", currency="USD", formula=False, footer=True):
    info = {"attrs": {"fund-ticker": {"value": "TEST"}, "isin": {"value": "US0000000010"}}}
    html = (
        f'<input id="fund-info-ticker" value="TEST">'
        f'<input id="fund-quick-info" value="{escape(json.dumps(info), quote=True)}">'
        f'<div><div class="label">Base Currency</div><div class="data">{currency}</div></div>'
        f'<table><tr><th class="label">Base Currency</th>'
        f'<td class="data">{currency}</td></tr></table>'
    ).encode()
    rows = (
        rows
        if rows is not None
        else [
            ["11-Jan-2026", "12.345678901234567", "2.5E+6", "3.08641972530864175E+7"],
            ["10-Jan-2026", "12.3", "", ""],
        ]
    )
    table = {
        1: ["Fund Name:", "Synthetic Fund"],
        2: ["Ticker Symbol:", ticker],
        4: ["Date", "NAV", "Shares Outstanding", "Total Net Assets"],
        **{5 + i: r for i, r in enumerate(rows)},
    }
    if footer:
        table[8 + len(rows)] = ["Synthetic footer"]
        table[10 + len(rows)] = [
            "Synthetic legal text",
            "",
            "",
            "",
            "",
            "label",
            "",
            "",
            "",
            "",
            "",
        ]
    strings = []
    sheet = []
    for n, row in table.items():
        cells = []
        for col, v in enumerate(row):
            strings.append(v)
            f = "<f>1+1</f>" if formula and n == 5 and col == 1 else ""
            cells.append(f'<c r="{chr(65 + col)}{n}" t="s">{f}<v>{len(strings) - 1}</v></c>')
        sheet.append(f'<row r="{n}">' + "".join(cells) + "</row>")
    ns = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
    b = io.BytesIO()
    with ZipFile(b, "w") as z:
        z.writestr(
            "xl/workbook.xml",
            f'<workbook {ns}><sheets><sheet name="navhist" sheetId="1"/></sheets></workbook>',
        )
        z.writestr(
            "xl/worksheets/sheet1.xml",
            f"<worksheet {ns}><sheetData>{''.join(sheet)}</sheetData></worksheet>",
        )
        z.writestr(
            "xl/sharedStrings.xml",
            f"<sst {ns}>" + "".join(f"<si><t>{escape(v)}</t></si>" for v in strings) + "</sst>",
        )
    return html, b.getvalue()


def parse(**options):
    html, workbook = originals(**options)
    return parse_nav_history(
        html=html,
        workbook=workbook,
        instrument_id=UUID(int=1),
        symbol="TEST",
        html_url="https://www.ssga.com/us/en/individual/etfs/synthetic-test",
        workbook_url="https://www.ssga.com/library-content/products/fund-data/etfs/us/navhist-test.xlsx",
        html_received_at=datetime(2026, 1, 12, tzinfo=UTC),
        workbook_received_at=datetime(2026, 1, 13, tzinfo=UTC),
    )


def test_all_dates_and_exact_decimal_values_survive_with_conservative_semantics():
    result = parse()
    assert [x.date for x in result.observations] == [date(2026, 1, 11), date(2026, 1, 10)]
    assert result.observations[0].nav == Decimal("12.345678901234567")
    assert result.observations[0].shares_outstanding == Decimal("2.5E+6")
    assert result.observations[1].shares_outstanding is None
    assert result.observations[1].total_net_assets is None
    assert result.currency == "USD" and result.currency_basis == "issuer_page_base_currency"
    assert result.nav_unit == "currency_per_share"
    assert result.available_at == datetime(2026, 1, 13, tzinfo=UTC)
    assert not result.historical_backtest_eligible and not result.market_price_eligible
    assert not result.total_return_eligible
    assert result.source_published_at is result.valid_until is result.revision_method is None
    assert result.split_adjustment is result.reinvestment_method is None
    assert type(result).model_validate_json(result.canonical_bytes()) == result
    assert result.canonical_bytes() == parse().canonical_bytes()


@pytest.mark.parametrize(
    "rows",
    [
        [["11-Jan-2026", "0", "1", "2"]],
        [["11-Jan-2026", "-1", "1", "2"]],
        [["11-Jan-2026", "", "1", "2"]],
        [["11-Jan-2026", "NaN", "1", "2"]],
        [["11-Jan-2026", "1", "-1", "2"]],
        [["11-Jan-2026", "1", "1", "Infinity"]],
        [["11-Jan-2026", "1", "1", "2"], ["11-Jan-2026", "2", "1", "2"]],
        [["14-Jan-2026", "1", "1", "2"]],
        [["bad-date", "1", "1", "2"]],
        [["11-Jan-2026", "1", "1", "2"], ["bad-date", "1", "1", "2"]],
        [],
    ],
)
def test_invalid_rows_are_rejected_without_source_values(rows):
    with pytest.raises(ValueError, match="^ISSUER_NAV_"):
        parse(rows=rows)


@pytest.mark.parametrize("options", [{"ticker": "OTHER"}, {"currency": "EUR"}, {"formula": True}])
def test_identity_currency_and_formulas_fail_closed(options):
    with pytest.raises(ValueError, match="^ISSUER_NAV_"):
        parse(**options)


def test_optional_dash_means_unknown_but_nav_dash_is_rejected():
    result = parse(rows=[["11-Jan-2026", "1", "-", "-"]])
    assert result.observations[0].shares_outstanding is None
    assert result.observations[0].total_net_assets is None
    with pytest.raises(ValueError, match="^ISSUER_NAV_"):
        parse(rows=[["11-Jan-2026", "-", "1", "2"]])


@pytest.mark.parametrize(
    "damage",
    ["formula", "duplicate_cell", "shared_index", "footer_numeric", "footer_date", "huge_row"],
)
def test_corrupted_cells_or_rows_cannot_hide_in_footer(damage, monkeypatch):
    html, workbook = originals()
    with ZipFile(io.BytesIO(workbook)) as archive:
        contents = {name: archive.read(name) for name in archive.namelist()}
    sheet = contents["xl/worksheets/sheet1.xml"].decode()
    if damage == "formula":
        sheet = sheet.replace('<c r="A10" t="s">', '<c r="A10" t="s"><f>1+1</f>')
    elif damage == "duplicate_cell":
        sheet = sheet.replace('<row r="5">', '<row r="5"><c r="A5"><v>1</v></c>')
    elif damage == "shared_index":
        sheet = sheet.replace('<c r="A5" t="s"><v>', '<c r="A5" t="s"><v>99999')
    elif damage == "footer_numeric":
        sheet = sheet.replace(
            "</sheetData>",
            '<row r="13"><c r="A13" t="inlineStr"><is><t>Not a NAV</t></is></c>'
            '<c r="B13"><v>1</v></c></row></sheetData>',
        )
    elif damage == "footer_date":
        sheet = sheet.replace(
            "</sheetData>",
            '<row r="13"><c r="A13" t="inlineStr"><is><t>09-Jan-2026</t></is></c>'
            '<c r="B13"><v>1</v></c></row></sheetData>',
        )
    else:
        sheet = sheet.replace('<row r="5">', '<row r="9999999">')
    contents["xl/worksheets/sheet1.xml"] = sheet.encode()
    buffer = io.BytesIO()
    with ZipFile(buffer, "w") as archive:
        for name, value in contents.items():
            archive.writestr(name, value)
    monkeypatch.setattr(
        __import__(__name__, fromlist=["originals"]),
        "originals",
        lambda **kwargs: (html, buffer.getvalue()),
    )
    with pytest.raises(ValueError, match="^ISSUER_NAV_"):
        parse()


@pytest.mark.parametrize(
    "damage",
    ["isin", "currency_conflict", "currency_missing", "duplicate_identity", "oversize", "bad_zip"],
)
def test_identity_and_archive_bounds_are_checked(damage, monkeypatch):
    html, workbook = originals()
    if damage == "isin":
        html = html.replace(b"US0000000010", b"not-an-isin")
    elif damage == "currency_conflict":
        html = html.replace(b">USD<", b">EUR<", 1)
    elif damage == "currency_missing":
        html = html.replace(b"Base Currency", b"Other Label")
    elif damage == "duplicate_identity":
        html += b'<input id="fund-info-ticker" value="TEST">'
    elif damage == "oversize":
        workbook = b"X" * (8 * 1024 * 1024 + 1)
    else:
        workbook = b"not a zip"
    monkeypatch.setattr(
        __import__(__name__, fromlist=["originals"]), "originals", lambda **kwargs: (html, workbook)
    )
    with pytest.raises(ValueError, match="^ISSUER_NAV_"):
        parse()


def test_model_rejects_binary_float_and_preserves_unknown_defaults():
    from nexus.research.ssga_nav_history import NavObservation

    row = NavObservation(date=date(2026, 1, 11), nav=Decimal("1.23"))
    assert row.shares_outstanding is row.total_net_assets is None
    with pytest.raises(ValueError, match="ISSUER_NAV_NUMBER_INVALID"):
        NavObservation(date=date(2026, 1, 11), nav=1.23)


@pytest.mark.parametrize("damage", ["footer_iso_date", "utf16_entity"])
def test_footer_date_damage_and_encoded_entities_fail_closed(damage, monkeypatch):
    html, workbook = originals()
    with ZipFile(io.BytesIO(workbook)) as archive:
        contents = {name: archive.read(name) for name in archive.namelist()}
    if damage == "footer_iso_date":
        contents["xl/sharedStrings.xml"] = contents["xl/sharedStrings.xml"].replace(
            b"Synthetic footer", b"2026-01-10"
        )
    else:
        body = contents["xl/sharedStrings.xml"].decode().replace("Synthetic footer", "&expanded;")
        contents["xl/sharedStrings.xml"] = (
            '<?xml version="1.0" encoding="UTF-16"?>'
            '<!DOCTYPE sst [<!ENTITY expanded "Synthetic footer">]>' + body
        ).encode("utf-16")
    buffer = io.BytesIO()
    with ZipFile(buffer, "w") as archive:
        for name, value in contents.items():
            archive.writestr(name, value)
    monkeypatch.setattr(
        __import__(__name__, fromlist=["originals"]),
        "originals",
        lambda **kwargs: (html, buffer.getvalue()),
    )
    with pytest.raises(ValueError, match="^ISSUER_NAV_"):
        parse()
