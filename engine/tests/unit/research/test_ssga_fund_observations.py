"""Synthetic industry and holdings shapes; never real portfolio fixtures."""

import io
import json
from datetime import UTC, datetime
from decimal import Decimal
from html import escape
from uuid import UUID
from zipfile import ZipFile

import pytest
from nexus.research.ssga_fund_observations import parse_fund_observation


def originals(
    *,
    symbol="TEST",
    heading="Fund Industry Allocation",
    weight="-0.125",
    displayed="100.00%",
    original="100.00",
):
    allocation = {
        "label": "Fund Industry Allocation",
        "asOfDateSimple": "Jan 15 2026",
        "attrArray": [
            {
                "name": {"value": "Synthetic industry"},
                "weight": {"value": displayed, "originalValue": original},
            }
        ],
    }
    info = {"attrs": {"fund-ticker": {"value": "TEST"}, "isin": {"value": "US0000000010"}}}
    html = (
        f'<input id="fund-info-ticker" value="TEST">'
        f'<input id="fund-quick-info" value="{escape(json.dumps(info), quote=True)}">'
        f"<h3>{heading} as of Jan 15 2026</h3>"
        f'<input id="fund-sector-breakdown" value="{escape(json.dumps(allocation), quote=True)}">'
    ).encode()
    rows = {
        1: ["Fund Name:", "Synthetic Fund"],
        2: ["Ticker Symbol:", symbol],
        3: ["Holdings:", "As of 15-Jan-2026"],
        5: [
            "Name",
            "Ticker",
            "Identifier",
            "SEDOL",
            "Weight",
            "Sector",
            "Shares Held",
            "Local Currency",
        ],
        6: ["Synthetic holding", "SYN", "ID1", "", weight, "", "-2", "USD"],
        7: ["Other holding", "OTR", "ID2", "", "0", "", "0", "USD"],
    }
    ns = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
    sheet = "".join(
        f'<row r="{n}">'
        + "".join(
            f'<c r="{chr(65 + i)}{n}" t="inlineStr"><is><t>{escape(v)}</t></is></c>'
            for i, v in enumerate(values)
        )
        + "</row>"
        for n, values in rows.items()
    )
    buffer = io.BytesIO()
    with ZipFile(buffer, "w") as z:
        z.writestr(
            "xl/workbook.xml",
            f'<workbook {ns}><sheets><sheet name="holdings" sheetId="1"/></sheets></workbook>',
        )
        z.writestr(
            "xl/worksheets/sheet1.xml",
            f"<worksheet {ns}><sheetData>{sheet}</sheetData></worksheet>",
        )
    return html, buffer.getvalue()


def parse(original_bytes=None, **changes):
    html, workbook = original_bytes or originals(**changes)
    return parse_fund_observation(
        html=html,
        workbook=workbook,
        instrument_id=UUID(int=31),
        symbol="TEST",
        html_url="https://www.ssga.com/us/en/individual/etfs/synthetic-fund-test",
        workbook_url="https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-test.xlsx",
        html_received_at=datetime(2026, 1, 20, tzinfo=UTC),
        workbook_received_at=datetime(2026, 1, 21, tzinfo=UTC),
    )


def test_negative_weights_unknowns_and_both_receipts_survive():
    result = parse()
    assert result.holdings[0].weight_percent == Decimal("-0.125")
    assert result.holdings[0].classification is None
    assert result.industries[0].weight_percent == Decimal("100.00")
    assert result.classification_level == "industry"
    assert result.denominator is None and result.source_published_at is None
    assert result.valid_until is None and result.residual_classification is None
    assert result.available_at == result.workbook_received_at > result.html_received_at
    assert not result.historical_backtest_eligible and not result.sector_risk_eligible


@pytest.mark.parametrize(
    "changes",
    [
        {"symbol": "OTHER"},
        {"heading": "Index Industry Allocation"},
        {"weight": "NaN"},
        {"weight": ""},
        {"displayed": "1.00%"},
        {"displayed": "100.00"},
    ],
)
def test_identity_heading_and_nonfinite_weights_rejected(changes):
    with pytest.raises(ValueError):
        parse(**changes)


@pytest.mark.asyncio
async def test_invalid_optional_observation_does_not_hide_other_evidence():
    from contextlib import nullcontext
    from types import SimpleNamespace

    from nexus.research.ssga_fund_observations import read_fund_observations

    class Session:
        no_autoflush = nullcontext()

        async def execute(self, query):
            return [
                (SimpleNamespace(payload={"instrument_id": str(UUID(int=31))}), SimpleNamespace())
            ]

    assert (
        await read_fund_observations(
            Session(), instrument_id=UUID(int=31), as_of=datetime(2026, 1, 21, tzinfo=UTC)
        )
        == {}
    )


def test_json_numeric_weight_is_decimal_exact():
    assert parse(original=0.1, displayed="0.1%").industries[0].weight_percent == Decimal("0.1")


@pytest.mark.parametrize(
    "shape", ["name_only", "name_and_classification", "footer", "after_footer"]
)
def test_xml_data_rows_and_explicit_footer_boundary(shape):
    from xml.etree import ElementTree as ET

    html, workbook = originals()
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    with ZipFile(io.BytesIO(workbook)) as archive:
        entries = {name: archive.read(name) for name in archive.namelist()}
    sheet = ET.fromstring(entries["xl/worksheets/sheet1.xml"])
    data = sheet.find(ns + "sheetData")

    def row(number, cells):
        node = ET.Element(ns + "row", {"r": str(number)})
        for column, value in cells.items():
            cell = ET.SubElement(node, ns + "c", {"r": f"{column}{number}", "t": "inlineStr"})
            ET.SubElement(ET.SubElement(cell, ns + "is"), ns + "t").text = value
        return node

    if shape in {"name_only", "name_and_classification"}:
        data.remove(next(x for x in data if x.get("r") == "6"))
        cells = {"A": "Synthetic missing weight"}
        if shape == "name_and_classification":
            cells["F"] = "Synthetic category"
        data.append(row(6, cells))
    else:
        data.append(row(8, {"A": "Synthetic explanatory footer"}))
        ET.SubElement(ET.SubElement(sheet, ns + "mergeCells"), ns + "mergeCell", {"ref": "A8:I8"})
        if shape == "after_footer":
            data.append(row(9, {"A": "Synthetic unexpected holding", "E": "1"}))
    entries["xl/worksheets/sheet1.xml"] = ET.tostring(sheet)
    result = io.BytesIO()
    with ZipFile(result, "w") as archive:
        for name, raw in entries.items():
            archive.writestr(name, raw)
    if shape == "footer":
        assert len(parse(original_bytes=(html, result.getvalue())).holdings) == 2
    else:
        with pytest.raises(ValueError, match="WEIGHT_MISSING"):
            parse(original_bytes=(html, result.getvalue()))
