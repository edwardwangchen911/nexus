"""Strict projection of three GAAP quarter metrics from one evidenced exhibit layout."""

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from html.parser import HTMLParser
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field

_STATEMENT_TITLE = "CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS — UNAUDITED"
_UNIT_LABEL = "(IN MILLIONS, EXCEPT PER SHARE DATA)"
_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
_DATE = re.compile(rf"^({_MONTHS}) \d{{1,2}},\s*\d{{4}}$")
_NUMBER = re.compile(r"^\(?-?\d[\d,]*(?:\.\d+)?\)?$")


class ExhibitQuarterMetric(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    metric: Literal["net_revenue", "gaap_net_income", "gaap_diluted_eps"]
    row_path: tuple[str, ...]
    current_value: str
    comparable_value: str
    scale: int = Field(ge=0)
    unit: Literal["millions", "per_share"]
    currency: None = None


class ExhibitQuarterMetrics(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    source_kind: Literal["issuer_exhibit_table"] = "issuer_exhibit_table"
    statement_title: Literal[
        "CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS — UNAUDITED"
    ] = "CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS — UNAUDITED"
    accounting_basis: Literal["gaap"] = "gaap"
    audit_status: Literal["unaudited"] = "unaudited"
    filing_status: Literal["furnished_not_filed"] = "furnished_not_filed"
    period_scope: Literal["fiscal_quarter_ended"] = "fiscal_quarter_ended"
    current_period_end: date
    comparable_period_end: date
    current_column_label: str
    comparable_column_label: str
    original_unit_label: Literal["(IN MILLIONS, EXCEPT PER SHARE DATA)"] = (
        "(IN MILLIONS, EXCEPT PER SHARE DATA)"
    )
    raw_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    metrics: tuple[ExhibitQuarterMetric, ExhibitQuarterMetric, ExhibitQuarterMetric]


class _Document(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hidden_depth = 0
        self.table_depth = 0
        self.nested_table = False
        self.tables: list[list[list[str]]] = []
        self.table: list[list[str]] | None = None
        self.row: list[str] | None = None
        self.cell: list[str] | None = None
        self.visible: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        style = (values.get("style") or "").replace(" ", "").lower()
        hidden = tag in {"script", "style"} or "hidden" in values or "display:none" in style
        if self.hidden_depth or hidden:
            self.hidden_depth += 1
            return
        if tag == "table":
            self.table_depth += 1
            if self.table_depth > 1:
                self.nested_table = True
            else:
                self.table = []
        elif self.table is not None and tag == "tr":
            self.row = []
        elif self.row is not None and tag in {"td", "th"}:
            self.cell = []

    def handle_data(self, data: str) -> None:
        if self.hidden_depth:
            return
        text = " ".join(data.split())
        if not text:
            return
        self.visible.append(text)
        if self.cell is not None:
            self.cell.append(text)

    def handle_endtag(self, tag: str) -> None:
        if self.hidden_depth:
            self.hidden_depth -= 1
            return
        if tag in {"td", "th"} and self.cell is not None and self.row is not None:
            self.row.append(" ".join(self.cell))
            self.cell = None
        elif tag == "tr" and self.row is not None and self.table is not None:
            self.table.append(self.row)
            self.row = None
        elif tag == "table" and self.table_depth:
            self.table_depth -= 1
            if self.table_depth == 0 and self.table is not None:
                self.tables.append(self.table)
                self.table = None


def _fail() -> None:
    raise ValueError("SEC_EXHIBIT_QUARTER_METRICS_UNSUPPORTED")


def _numbers(row: list[str]) -> list[str]:
    return [cell for cell in row if _NUMBER.fullmatch(cell)]


def _decimal(raw: str) -> tuple[str, int]:
    normalized = raw.replace(",", "")
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = "-" + normalized[1:-1]
    try:
        value = Decimal(normalized)
    except InvalidOperation:
        _fail()
    if not value.is_finite():
        _fail()
    exponent = cast(int, value.as_tuple().exponent)
    scale = max(0, -exponent)
    return format(value, "f"), scale


def _one_row(table: list[list[str]], label: str) -> tuple[int, list[str]]:
    matches = [(index, row) for index, row in enumerate(table) if row and row[0] == label]
    if len(matches) != 1:
        _fail()
    return matches[0]


def _metric(
    metric: Literal["net_revenue", "gaap_net_income", "gaap_diluted_eps"],
    path: tuple[str, ...],
    row: list[str],
    unit: Literal["millions", "per_share"],
) -> ExhibitQuarterMetric:
    values = _numbers(row)
    if len(values) != 5:
        _fail()
    current, current_scale = _decimal(values[0])
    comparable, comparable_scale = _decimal(values[2])
    return ExhibitQuarterMetric(
        metric=metric,
        row_path=path,
        current_value=current,
        comparable_value=comparable,
        scale=max(current_scale, comparable_scale),
        unit=unit,
    )


def parse_exhibit_quarter_metrics(raw: bytes) -> ExhibitQuarterMetrics:
    """Parse only the exact, cross-checked GAAP quarter layout evidenced by the archive."""
    try:
        parser = _Document()
        parser.feed(raw.decode("utf-8-sig"))
        parser.close()
    except (UnicodeDecodeError, ValueError):
        _fail()
    if parser.nested_table or parser.visible.count(_STATEMENT_TITLE) != 1:
        _fail()
    if parser.visible.count(_UNIT_LABEL) != 1:
        _fail()

    operations = [
        table
        for table in parser.tables
        if sum(cell == "Fiscal Quarter Ended" for row in table for cell in row) == 1
        and sum(cell == "Three Fiscal Quarters Ended" for row in table for cell in row) == 1
        and sum(bool(row and row[0] == "Net revenue") for row in table) == 1
        and sum(bool(row and row[0] == "Net income") for row in table) == 1
        and sum(bool(row and row[0] == "Net income per share:") for row in table) == 1
        and sum(
            bool(row and row[0] == "Weighted-average shares used in per share calculations:")
            for row in table
        )
        == 1
    ]
    summaries = [
        table
        for table in parser.tables
        if sum(cell == "GAAP" for row in table for cell in row) == 1
        and sum(cell == "Non-GAAP" for row in table for cell in row) == 1
        and any(row and row[0] == "Earnings per common share - diluted" for row in table)
    ]
    if len(operations) != 1 or len(summaries) != 1:
        _fail()
    statement, summary = operations[0], summaries[0]

    date_rows = [[cell for cell in row if _DATE.fullmatch(cell)] for row in statement]
    date_rows = [row for row in date_rows if row]
    if len(date_rows) != 1 or len(date_rows[0]) != 5:
        _fail()
    labels = date_rows[0]
    parsed_dates = [
        datetime.strptime(label.replace(",", ", "), "%B %d, %Y").date()
        for label in labels
    ]
    if labels[0] != labels[3] or labels[2] != labels[4] or len(set(labels[:3])) != 3:
        _fail()

    _, revenue_row = _one_row(statement, "Net revenue")
    _, income_row = _one_row(statement, "Net income")
    section_index, _ = _one_row(statement, "Net income per share:")
    shares_index, _ = _one_row(statement, "Weighted-average shares used in per share calculations:")
    diluted = [
        row
        for index, row in enumerate(statement)
        if section_index < index < shares_index and row and row[0] == "Diluted"
    ]
    if len(diluted) != 1:
        _fail()

    metrics = (
        _metric("net_revenue", ("Net revenue",), revenue_row, "millions"),
        _metric("gaap_net_income", ("Net income",), income_row, "millions"),
        _metric("gaap_diluted_eps", ("Net income per share:", "Diluted"), diluted[0], "per_share"),
    )
    summary_labels = ("Net revenue", "Net income", "Earnings per common share - diluted")
    for metric, label in zip(metrics, summary_labels, strict=True):
        _, row = _one_row(summary, label)
        values = _numbers(row)
        if len(values) not in {4, 6}:
            _fail()
        if (
            _decimal(values[0])[0] != metric.current_value
            or _decimal(values[1])[0] != metric.comparable_value
        ):
            _fail()

    return ExhibitQuarterMetrics(
        current_period_end=parsed_dates[0],
        comparable_period_end=parsed_dates[2],
        current_column_label=labels[0],
        comparable_column_label=labels[2],
        raw_content_hash=sha256(raw).hexdigest(),
        metrics=metrics,
    )
