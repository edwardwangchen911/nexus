"""Narrow, deterministic projection of one evidenced issuer exhibit layout."""

from hashlib import sha256

import pytest


def exhibit_html(*, summary_revenue: str = "1,234", diluted_shares: str = "999") -> bytes:
    def values(current: str, prior: str, *, dollar: bool = False) -> str:
        items = (current, "700", prior, "2,100", "1,900")
        return "".join(
            f"<td>{'$' if dollar else ''}</td><td>{item}</td><td></td>" for item in items
        )

    return f"""
    <html><body>
      <p>CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS — UNAUDITED</p>
      <p>(IN MILLIONS, EXCEPT PER SHARE DATA)</p>
      <table id="operations">
        <tr><td></td><td>Fiscal Quarter Ended</td><td>Three Fiscal Quarters Ended</td></tr>
        <tr><td></td><td>August 2, 2026</td><td>May 3, 2026</td>
            <td>August 3, 2025</td><td>August 2, 2026</td><td>August 3, 2025</td></tr>
        <tr><td>Net revenue</td>{values("1,234", "987", dollar=True)}</tr>
        <tr><td>Net income</td>{values("123", "98", dollar=True)}</tr>
        <tr><td>Net income per share:</td></tr>
        <tr><td>Diluted</td>{values("1.25", "0.98", dollar=True)}</tr>
        <tr><td>Weighted-average shares used in per share calculations:</td></tr>
        <tr><td>Diluted</td>{values(diluted_shares, "888")}</tr>
      </table>
      <table id="summary">
        <tr><td></td><td>GAAP</td><td>Non-GAAP</td></tr>
        <tr><td>(Dollars in millions, except per share data)</td>
            <td>August 2, 2026</td><td>August 3, 2025</td><td>Change</td>
            <td>August 2, 2026</td><td>August 3, 2025</td><td>Change</td></tr>
        <tr><td>Net revenue</td><td>$</td><td>{summary_revenue}</td><td>$</td><td>987</td>
            <td>25</td><td>%</td><td>$</td><td>1,300</td><td>$</td><td>1,000</td><td>30</td><td>%</td></tr>
        <tr><td>Net income</td><td>$</td><td>123</td><td>$</td><td>98</td>
            <td>26</td><td>%</td><td>$</td><td>150</td><td>$</td><td>110</td><td>36</td><td>%</td></tr>
        <tr><td>Earnings per common share - diluted</td><td>$</td><td>1.25</td>
            <td>$</td><td>0.98</td><td>28</td><td>%</td><td>$</td><td>1.40</td><td>$</td><td>1.05</td><td>33</td><td>%</td></tr>
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
            b"<tr><td>Net income</td>",
            b"<tr><td>Net revenue</td><td>1</td></tr><tr><td>Net income</td>",
        ),
        "cumulative_header_missing": (b"Three Fiscal Quarters Ended", b"Nine Months"),
        "unit_missing": (b"(IN MILLIONS, EXCEPT PER SHARE DATA)", b"(UNITS UNKNOWN)"),
        "gaap_missing": (b"<td>GAAP</td>", b"<td>Adjusted</td>"),
        "value_missing": (b"<td>1.25</td>", b"<td></td>", 1),
        "nested_table": (
            b'<table id="operations">',
            b'<table id="operations"><tr><td><table><tr><td>noise</td></tr></table>'
            b"</td></tr>",
        ),
        "hidden_required_row": (b"<tr><td>Net revenue</td>", b'<tr hidden><td>Net revenue</td>'),
    }
    if damage in replacements:
        old, new, *count = replacements[damage]
        raw = raw.replace(old, new, count[0] if count else -1)
    with pytest.raises(ValueError, match="SEC_EXHIBIT_QUARTER_METRICS"):
        parse(raw)


def test_hidden_and_share_count_diluted_values_cannot_satisfy_eps_row():
    raw = exhibit_html().replace(
        b"<tr><td>Diluted</td><td>$</td><td>1.25</td>",
        b'<tr hidden><td>Diluted</td><td>$</td><td>1.25</td>',
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
