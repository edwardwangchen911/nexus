"""Narrow, deterministic projection of one evidenced issuer exhibit layout."""

from hashlib import sha256

import pytest


def exhibit_html(*, summary_revenue: str = "1,234", diluted_shares: str = "999") -> bytes:
    def values(current: str, prior: str, *, dollar: bool = False) -> str:
        items = (current, "700", prior, "2,100", "1,900")
        return "".join(
            f"<td>{'$' if dollar else ''}</td><td>{item}</td><td></td><td colspan=3></td>"
            for item in items
        )

    return f"""
    <html><body>
      <p>CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS — UNAUDITED</p>
      <p>(IN MILLIONS, EXCEPT PER SHARE DATA)</p>
      <table id="operations">
        <tr><td colspan=3></td><td colspan=3></td><td colspan=15>Fiscal Quarter Ended</td>
            <td colspan=3></td><td colspan=9>Three Fiscal Quarters Ended</td></tr>
        <tr><td colspan=3></td><td colspan=3></td><td colspan=3>August 2, 2026</td>
            <td colspan=3></td><td colspan=3>May 3, 2026</td><td colspan=3></td>
            <td colspan=3>August 3, 2025</td><td colspan=3></td>
            <td colspan=3>August 2, 2026</td><td colspan=3></td>
            <td colspan=3>August 3, 2025</td></tr>
        <tr><td colspan=3>Net revenue</td><td colspan=3></td>
            {values("1,234", "987", dollar=True)}</tr>
        <tr><td colspan=3>Net income</td><td colspan=3></td>{values("123", "98", dollar=True)}</tr>
        <tr><td colspan=3>Net income per share:</td></tr>
        <tr><td colspan=3>Diluted</td><td colspan=3></td>{values("1.25", "0.98", dollar=True)}</tr>
        <tr><td colspan=3>Weighted-average shares used in per share calculations:</td></tr>
        <tr><td colspan=3>Diluted</td><td colspan=3></td>{values(diluted_shares, "888")}</tr>
      </table>
      <table id="summary">
        <tr><td colspan=3></td><td colspan=3></td><td colspan=18>GAAP</td>
            <td colspan=3></td><td colspan=18>Non-GAAP</td></tr>
        <tr><td colspan=3>(Dollars in millions, except per share data)</td><td colspan=3></td>
            <td colspan=3>Q3 26</td><td colspan=3></td>
            <td colspan=3>Q3 25</td><td colspan=3></td><td colspan=6>Change</td>
            <td colspan=3></td><td colspan=3>Q3 26</td><td colspan=3></td>
            <td colspan=3>Q3 25</td><td colspan=3></td><td colspan=6>Change</td></tr>
        <tr><td colspan=3>Net revenue</td><td colspan=3></td><td>$</td><td>{summary_revenue}</td>
            <td></td><td colspan=3></td><td>$</td><td>987</td><td></td><td colspan=3></td>
            <td colspan=3>25</td><td colspan=3>%</td><td colspan=3></td>
            <td>$</td><td>1,300</td><td></td><td colspan=3></td><td>$</td><td>1,000</td>
            <td colspan=3>30</td><td colspan=3>%</td></tr>
        <tr><td colspan=3>Net income</td><td colspan=3></td><td>$</td><td>123</td>
            <td></td><td colspan=3></td><td>$</td><td>98</td><td></td><td colspan=3></td>
            <td colspan=3>26</td><td colspan=3>%</td><td colspan=3></td>
            <td>$</td><td>150</td><td></td><td colspan=3></td><td>$</td><td>110</td>
            <td colspan=3>36</td><td colspan=3>%</td></tr>
        <tr><td colspan=3>Earnings per common share - diluted</td><td colspan=3></td>
            <td>$</td><td>1.25</td><td></td><td colspan=3></td><td>$</td><td>0.98</td>
            <td></td><td colspan=3></td><td colspan=3>28</td><td colspan=3>%</td><td colspan=3></td>
            <td>$</td><td>1.40</td><td></td><td colspan=3></td><td>$</td><td>1.05</td>
            <td colspan=3>33</td><td colspan=3>%</td></tr>
      </table>
    </body></html>
    """.encode()


def parse(raw: bytes):
    from nexus.research.sec_exhibit_quarter_metrics import parse_exhibit_quarter_metrics

    return parse_exhibit_quarter_metrics(raw)


def test_extracts_only_current_and_comparable_gaap_quarter_columns():
    raw = exhibit_html()
    result = parse(raw)

    assert result.source_kind == "issuer_exhibit_table"
    assert result.accounting_basis == "gaap"
    assert result.audit_status == "unaudited"
    assert result.filing_status == "furnished_not_filed"
    assert result.period_scope == "fiscal_quarter_ended"
    assert result.current_period_end.isoformat() == "2026-08-02"
    assert result.comparable_period_end.isoformat() == "2025-08-03"
    assert result.current_column_label == "August 2, 2026"
    assert result.comparable_column_label == "August 3, 2025"
    assert result.raw_content_hash == sha256(raw).hexdigest()
    projected = [
        (item.metric, item.row_path, item.current_value, item.comparable_value)
        for item in result.metrics
    ]
    assert projected == [
        ("net_revenue", ("Net revenue",), "1234", "987"),
        ("gaap_net_income", ("Net income",), "123", "98"),
        ("gaap_diluted_eps", ("Net income per share:", "Diluted"), "1.25", "0.98"),
    ]
    assert [item.scale for item in result.metrics] == [0, 0, 2]
    assert [item.unit for item in result.metrics] == ["millions", "millions", "per_share"]
    assert all(item.currency is None for item in result.metrics)
    assert result.original_unit_label == "(IN MILLIONS, EXCEPT PER SHARE DATA)"


@pytest.mark.parametrize(
    "damage",
    [
        "summary_conflict",
        "duplicate_statement",
        "duplicate_revenue_row",
        "cumulative_header_missing",
        "unit_missing",
        "gaap_missing",
        "value_missing",
        "nested_table",
        "hidden_required_row",
    ],
)
def test_rejects_ambiguous_or_semantically_unsupported_tables(damage: str):
    raw = exhibit_html(summary_revenue="1,235" if damage == "summary_conflict" else "1,234")
    replacements = {
        "duplicate_statement": (
            b"</body>",
            "<p>CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS — UNAUDITED</p></body>".encode(),
        ),
        "duplicate_revenue_row": (
            b"<tr><td colspan=3>Net income</td>",
            b"<tr><td colspan=3>Net revenue</td></tr><tr><td colspan=3>Net income</td>",
        ),
        "cumulative_header_missing": (b"Three Fiscal Quarters Ended", b"Nine Months"),
        "unit_missing": (b"(IN MILLIONS, EXCEPT PER SHARE DATA)", b"(UNITS UNKNOWN)"),
        "gaap_missing": (b"<td colspan=18>GAAP</td>", b"<td colspan=18>Adjusted</td>"),
        "value_missing": (b"<td>1.25</td>", b"<td></td>", 1),
        "nested_table": (
            b'<table id="operations">',
            b'<table id="operations"><tr><td><table><tr><td>noise</td></tr></table></td></tr>',
        ),
        "hidden_required_row": (
            b"<tr><td colspan=3>Net revenue</td>",
            b"<tr hidden><td colspan=3>Net revenue</td>",
        ),
    }
    if damage in replacements:
        old, new, *count = replacements[damage]
        raw = raw.replace(old, new, count[0] if count else -1)
    with pytest.raises(ValueError, match="SEC_EXHIBIT_QUARTER_METRICS"):
        parse(raw)


def test_hidden_and_share_count_diluted_values_cannot_satisfy_eps_row():
    raw = exhibit_html().replace(
        b"<tr><td colspan=3>Diluted</td><td colspan=3></td><td>$</td><td>1.25</td>",
        b"<tr hidden><td colspan=3>Diluted</td><td colspan=3></td><td>$</td><td>1.25</td>",
        1,
    )
    with pytest.raises(ValueError, match="SEC_EXHIBIT_QUARTER_METRICS"):
        parse(raw)


def test_other_statements_with_the_same_period_headers_are_not_candidates():
    distractor = b"""
    <table><tr><td>Fiscal Quarter Ended</td><td>Three Fiscal Quarters Ended</td></tr>
      <tr><td>Cash and cash equivalents</td><td>10</td><td>20</td></tr></table>
    """
    result = parse(exhibit_html().replace(b"</body>", distractor + b"</body>"))
    assert [item.metric for item in result.metrics] == [
        "net_revenue",
        "gaap_net_income",
        "gaap_diluted_eps",
    ]


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (
            b"<td colspan=18>GAAP</td>\n            <td colspan=3></td>"
            b"<td colspan=18>Non-GAAP</td>",
            b"<td colspan=18>Non-GAAP</td>\n            <td colspan=3></td>"
            b"<td colspan=18>GAAP</td>",
        ),
        (
            b"<td colspan=15>Fiscal Quarter Ended</td>\n            <td colspan=3></td>"
            b"<td colspan=9>Three Fiscal Quarters Ended</td>",
            b"<td colspan=15>Three Fiscal Quarters Ended</td>\n            <td colspan=3></td>"
            b"<td colspan=9>Fiscal Quarter Ended</td>",
        ),
        (
            b"<td colspan=3>Q3 26</td><td colspan=3></td>\n            <td colspan=3>Q3 25</td>",
            b"<td colspan=3>Q3 24</td><td colspan=3></td>\n            <td colspan=3>Q3 23</td>",
        ),
    ],
)
def test_rejects_changed_group_or_summary_date_column_binding(old: bytes, new: bytes):
    with pytest.raises(ValueError, match="SEC_EXHIBIT_QUARTER_METRICS"):
        parse(exhibit_html().replace(old, new, 1))


def test_title_and_unit_must_immediately_describe_the_operations_table():
    raw = exhibit_html()
    title = "<p>CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS — UNAUDITED</p>".encode()
    unit = b"<p>(IN MILLIONS, EXCEPT PER SHARE DATA)</p>"
    raw = raw.replace(title, b"").replace(unit, b"").replace(b"</body>", title + unit + b"</body>")
    with pytest.raises(ValueError, match="SEC_EXHIBIT_QUARTER_METRICS"):
        parse(raw)


@pytest.mark.parametrize("bad", [b"", b"not-a-number", b"12,34", b"1,,234"])
def test_value_cell_cannot_be_filtered_out_or_shift_the_following_columns(bad: bytes):
    raw = exhibit_html().replace(b"<td>1,234</td>", b"<td>" + bad + b"</td>", 1)
    with pytest.raises(ValueError, match="SEC_EXHIBIT_QUARTER_METRICS"):
        parse(raw)


def test_rejects_reversed_quarter_dates():
    raw = exhibit_html().replace(b"August 2, 2026", b"August 2, 2024")
    with pytest.raises(ValueError, match="SEC_EXHIBIT_QUARTER_METRICS"):
        parse(raw)


def test_visibility_hidden_and_void_tags_cannot_supply_values():
    raw = exhibit_html().replace(
        b"<td>1,234</td>",
        b'<td><span style="visibility:hidden">1,234</span><br></td>',
        1,
    )
    with pytest.raises(ValueError, match="SEC_EXHIBIT_QUARTER_METRICS"):
        parse(raw)


def test_summary_quarter_must_match_three_fiscal_quarters_ended():
    raw = exhibit_html().replace(b"Q3 26", b"Q1 26").replace(b"Q3 25", b"Q1 25")
    with pytest.raises(ValueError, match="SEC_EXHIBIT_QUARTER_METRICS"):
        parse(raw)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (b"<td>1,234</td><td></td>", b"<td colspan=2>1,234</td>"),
        (b"<td>987</td><td></td>", b"<td colspan=2>987</td>"),
    ],
)
def test_metric_value_cell_cannot_span_into_an_adjacent_column(old: bytes, new: bytes):
    with pytest.raises(ValueError, match="SEC_EXHIBIT_QUARTER_METRICS"):
        parse(exhibit_html().replace(old, new, 1))
