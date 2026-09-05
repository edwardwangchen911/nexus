"""Strict projection of three GAAP quarter metrics from one evidenced exhibit layout."""

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from html.parser import HTMLParser
from typing import Literal, NoReturn, cast

from pydantic import BaseModel, ConfigDict, Field

_STATEMENT_TITLE = "CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS — UNAUDITED"
_UNIT_LABEL = "(IN MILLIONS, EXCEPT PER SHARE DATA)"
_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
_DATE = re.compile(rf"^({_MONTHS}) \d{{1,2}},\s*\d{{4}}$")
_SUMMARY_PERIOD = re.compile(r"^Q([1-4]) (\d{2})$")
_NUMBER = re.compile(r"^\(?(?:-?(?:0|[1-9]\d*|[1-9]\d{0,2}(?:,\d{3})+)(?:\.\d+)?)\)?$")
_VOID_TAGS = {
    "area",
    "base",
    "br",
    "col",
    "embed",
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "source",
    "track",
    "wbr",
}


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
    statement_title: Literal["CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS — UNAUDITED"] = (
        "CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS — UNAUDITED"
    )
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


@dataclass(frozen=True)
class _Cell:
    text: str
    start: int
    end: int


@dataclass(frozen=True)
class _Table:
    rows: tuple[tuple[_Cell, ...], ...]
    preceding_outside: tuple[str, ...]


class _Document(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tag_stack: list[tuple[str, bool]] = []
        self.table_depth = 0
        self.nested_table = False
        self.invalid_span = False
        self.tables: list[_Table] = []
        self.table_rows: list[tuple[_Cell, ...]] | None = None
        self.table_prefix: tuple[str, ...] = ()
        self.row: list[_Cell] | None = None
        self.cell_parts: list[str] | None = None
        self.cell_start = 0
        self.cell_span = 1
        self.row_width = 0
        self.visible: list[str] = []
        self.outside_visible: list[str] = []

    @property
    def hidden(self) -> bool:
        return bool(self.tag_stack and self.tag_stack[-1][1])

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        style = re.sub(r"\s+", "", values.get("style") or "").lower()
        own_hidden = (
            tag in {"script", "style"}
            or "hidden" in values
            or (values.get("aria-hidden") or "").lower() == "true"
            or "display:none" in style
            or "visibility:hidden" in style
        )
        hidden = self.hidden or own_hidden
        if tag not in _VOID_TAGS:
            self.tag_stack.append((tag, hidden))
        if hidden:
            return
        if tag == "table":
            self.table_depth += 1
            if self.table_depth > 1:
                self.nested_table = True
            else:
                self.table_rows = []
                self.table_prefix = tuple(self.outside_visible[-3:])
        elif self.table_rows is not None and tag == "tr":
            self.row = []
            self.row_width = 0
        elif self.row is not None and tag in {"td", "th"}:
            raw_span = values.get("colspan") or "1"
            if not raw_span.isdigit() or not 1 <= int(raw_span) <= 50:
                self.invalid_span = True
                self.cell_span = 1
            else:
                self.cell_span = int(raw_span)
            if values.get("rowspan") not in {None, "1"}:
                self.invalid_span = True
            self.cell_start = self.row_width
            self.cell_parts = []

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if tag not in _VOID_TAGS:
            self.handle_endtag(tag)

    def handle_data(self, data: str) -> None:
        if self.hidden:
            return
        text = " ".join(data.split())
        if not text:
            return
        self.visible.append(text)
        if self.table_depth == 0:
            self.outside_visible.append(text)
        if self.cell_parts is not None:
            self.cell_parts.append(text)

    def handle_endtag(self, tag: str) -> None:
        if tag in _VOID_TAGS or not self.tag_stack:
            return
        _, hidden = self.tag_stack.pop()
        if hidden:
            return
        if tag in {"td", "th"} and self.cell_parts is not None and self.row is not None:
            end = self.cell_start + self.cell_span
            self.row.append(_Cell(" ".join(self.cell_parts), self.cell_start, end))
            self.row_width = end
            self.cell_parts = None
        elif tag == "tr" and self.row is not None and self.table_rows is not None:
            self.table_rows.append(tuple(self.row))
            self.row = None
        elif tag == "table" and self.table_depth:
            self.table_depth -= 1
            if self.table_depth == 0 and self.table_rows is not None:
                self.tables.append(_Table(tuple(self.table_rows), self.table_prefix))
                self.table_rows = None


def _fail() -> NoReturn:
    raise ValueError("SEC_EXHIBIT_QUARTER_METRICS_UNSUPPORTED")


def _cell_at(row: tuple[_Cell, ...], column: int) -> _Cell:
    matches = [cell for cell in row if cell.start <= column < cell.end]
    if len(matches) != 1:
        _fail()
    return matches[0]


def _exact_cell(row: tuple[_Cell, ...], start: int, end: int, text: str) -> bool:
    return sum(cell.start == start and cell.end == end and cell.text == text for cell in row) == 1


def _one_row(table: _Table, label: str) -> tuple[int, tuple[_Cell, ...]]:
    matches = [
        (index, row) for index, row in enumerate(table.rows) if _exact_cell(row, 0, 3, label)
    ]
    if len(matches) != 1:
        _fail()
    return matches[0]


def _decimal(raw: str) -> tuple[str, int]:
    if not _NUMBER.fullmatch(raw) or raw.startswith("(-"):
        _fail()
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
    return format(value, "f"), max(0, -exponent)


def _value_at(row: tuple[_Cell, ...], column: int) -> tuple[str, int]:
    cell = _cell_at(row, column)
    if cell.start != column or cell.end != column + 1:
        _fail()
    return _decimal(cell.text)


def _metric(
    metric: Literal["net_revenue", "gaap_net_income", "gaap_diluted_eps"],
    path: tuple[str, ...],
    row: tuple[_Cell, ...],
    unit: Literal["millions", "per_share"],
) -> ExhibitQuarterMetric:
    if not _exact_cell(row, 6, 7, "$") or not _exact_cell(row, 18, 19, "$"):
        _fail()
    current, current_scale = _value_at(row, 7)
    comparable, comparable_scale = _value_at(row, 19)
    return ExhibitQuarterMetric(
        metric=metric,
        row_path=path,
        current_value=current,
        comparable_value=comparable,
        scale=max(current_scale, comparable_scale),
        unit=unit,
    )


def _date_cell(row: tuple[_Cell, ...], start: int) -> tuple[str, date]:
    cell = _cell_at(row, start)
    if cell.start != start or cell.end != start + 3 or not _DATE.fullmatch(cell.text):
        _fail()
    return cell.text, datetime.strptime(re.sub(r",\s*", ", ", cell.text), "%B %d, %Y").date()


def _summary_period_cell(row: tuple[_Cell, ...], start: int) -> tuple[str, str]:
    cell = _cell_at(row, start)
    match = _SUMMARY_PERIOD.fullmatch(cell.text)
    if cell.start != start or cell.end != start + 3 or match is None:
        _fail()
    return match.group(1), match.group(2)


def parse_exhibit_quarter_metrics(raw: bytes) -> ExhibitQuarterMetrics:
    """Parse only the exact, cross-checked GAAP quarter layout evidenced by the archive."""
    try:
        parser = _Document()
        parser.feed(raw.decode("utf-8-sig"))
        parser.close()
    except (UnicodeDecodeError, ValueError):
        _fail()
    if parser.nested_table or parser.invalid_span or parser.visible.count(_STATEMENT_TITLE) != 1:
        _fail()
    if parser.visible.count(_UNIT_LABEL) != 1:
        _fail()

    operations = []
    required = {
        "Net revenue",
        "Net income",
        "Net income per share:",
        "Weighted-average shares used in per share calculations:",
    }
    for table in parser.tables:
        header_rows = [
            row
            for row in table.rows
            if _exact_cell(row, 6, 21, "Fiscal Quarter Ended")
            and _exact_cell(row, 24, 33, "Three Fiscal Quarters Ended")
        ]
        labels = {cell.text for row in table.rows for cell in row if cell.start == 0}
        if len(header_rows) == 1 and required <= labels:
            operations.append(table)
    if len(operations) != 1:
        _fail()
    statement = operations[0]
    if statement.preceding_outside[-2:] != (_STATEMENT_TITLE, _UNIT_LABEL):
        _fail()

    date_rows = [
        row
        for row in statement.rows
        if all(
            any(cell.start == start and _DATE.fullmatch(cell.text) for cell in row)
            for start in (6, 12, 18, 24, 30)
        )
    ]
    if len(date_rows) != 1:
        _fail()
    bound_dates = tuple(_date_cell(date_rows[0], start) for start in (6, 12, 18, 24, 30))
    date_labels = tuple(item[0] for item in bound_dates)
    dates = tuple(item[1] for item in bound_dates)
    if date_labels[0] != date_labels[3] or date_labels[2] != date_labels[4]:
        _fail()
    if not dates[0] > dates[1] > dates[2]:
        _fail()

    _, revenue_row = _one_row(statement, "Net revenue")
    _, income_row = _one_row(statement, "Net income")
    section_index, _ = _one_row(statement, "Net income per share:")
    shares_index, _ = _one_row(statement, "Weighted-average shares used in per share calculations:")
    diluted = [
        row
        for index, row in enumerate(statement.rows)
        if section_index < index < shares_index and _exact_cell(row, 0, 3, "Diluted")
    ]
    if len(diluted) != 1:
        _fail()
    metrics = (
        _metric("net_revenue", ("Net revenue",), revenue_row, "millions"),
        _metric("gaap_net_income", ("Net income",), income_row, "millions"),
        _metric("gaap_diluted_eps", ("Net income per share:", "Diluted"), diluted[0], "per_share"),
    )

    summaries = []
    for table in parser.tables:
        group_rows = [
            row
            for row in table.rows
            if _exact_cell(row, 6, 24, "GAAP") and _exact_cell(row, 27, 45, "Non-GAAP")
        ]
        if len(group_rows) == 1:
            summaries.append(table)
    if len(summaries) != 1:
        _fail()
    summary = summaries[0]
    summary_date_rows = [
        row
        for row in summary.rows
        if _exact_cell(row, 0, 3, "(Dollars in millions, except per share data)")
    ]
    if len(summary_date_rows) != 1:
        _fail()
    summary_current = _summary_period_cell(summary_date_rows[0], 6)
    summary_comparable = _summary_period_cell(summary_date_rows[0], 12)
    non_gaap_current = _summary_period_cell(summary_date_rows[0], 27)
    non_gaap_comparable = _summary_period_cell(summary_date_rows[0], 33)
    if (
        summary_current != non_gaap_current
        or summary_comparable != non_gaap_comparable
        or summary_current[0] != summary_comparable[0]
        or summary_current[0] != "3"
        or summary_current[1] != dates[0].strftime("%y")
        or summary_comparable[1] != dates[2].strftime("%y")
    ):
        _fail()

    for metric, label in zip(
        metrics,
        ("Net revenue", "Net income", "Earnings per common share - diluted"),
        strict=True,
    ):
        _, row = _one_row(summary, label)
        if not _exact_cell(row, 6, 7, "$") or not _exact_cell(row, 12, 13, "$"):
            _fail()
        if (
            _value_at(row, 7)[0] != metric.current_value
            or _value_at(row, 13)[0] != metric.comparable_value
        ):
            _fail()

    return ExhibitQuarterMetrics(
        current_period_end=dates[0],
        comparable_period_end=dates[2],
        current_column_label=date_labels[0],
        comparable_column_label=date_labels[2],
        raw_content_hash=sha256(raw).hexdigest(),
        metrics=metrics,
    )
