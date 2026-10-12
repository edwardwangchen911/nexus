"""Reversible metric/source-period tables for annotated model inputs only."""

import json
from copy import deepcopy
from typing import Any

ROW_FIELDS = ("fact", "raw_taxonomy", "raw_tag", "unit")
PRESENTATION = "metric_and_source_period_table_v1"
FORMAT_NOTE = (
    " Financial-facts table: each metric row preserves fact, raw_taxonomy/raw_tag and unit. "
    "source_columns preserve every other original attribute, including source/citation, "
    "period_start/end/kind, accepted_at, revision and amendment restrictions. Each cells entry "
    "[source_column_index, original_source_row, value] is an existing frozen observation, "
    "not a derived fact. Missing cells mean no supplied observation, never zero; present null "
    "stays null. Multiple cells at one column are distinct observations: preserve conflicts "
    "and multiplicity. All original observations are present."
)


def _key(value: object) -> str:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )


def financial_table_payload(payload: dict[str, object]) -> dict[str, object]:
    """Group complete annotated observations, preserving arbitrary extra attributes."""
    inputs, instructions = payload.get("frozen_inputs"), payload.get("instructions")
    if not isinstance(inputs, dict) or not isinstance(instructions, str):
        raise ValueError("MODEL_FINANCIAL_TABLE_INVALID")
    tool = inputs.get("financial_facts")
    if not isinstance(tool, dict) or not isinstance(tool.get("items"), list):
        raise ValueError("MODEL_FINANCIAL_TABLE_INVALID")
    original = tool["items"]
    if any(
        field in tool for field in ("source_columns", "original_row_count", "model_presentation")
    ):
        raise ValueError("MODEL_FINANCIAL_TABLE_INVALID")
    if not original:
        return payload
    rows: list[dict[str, Any]] = []
    columns: list[dict[str, Any]] = []
    row_indexes: dict[str, int] = {}
    column_indexes: dict[str, int] = {}
    for ordinal, item in enumerate(original):
        if (
            not isinstance(item, dict)
            or not all(field in item for field in (*ROW_FIELDS, "value", "citation_index"))
            or type(item["citation_index"]) is not int
            or item["citation_index"] < 0
        ):
            raise ValueError("MODEL_FINANCIAL_TABLE_INVALID")
        row = {field: item[field] for field in ROW_FIELDS}
        column = {
            field: value for field, value in item.items() if field not in (*ROW_FIELDS, "value")
        }
        row_key, column_key = _key(row), _key(column)
        if row_key not in row_indexes:
            row_indexes[row_key] = len(rows)
            rows.append({**deepcopy(row), "cells": []})
        if column_key not in column_indexes:
            column_indexes[column_key] = len(columns)
            columns.append(deepcopy(column))
        rows[row_indexes[row_key]]["cells"].append(
            [column_indexes[column_key], ordinal, deepcopy(item["value"])]
        )
    table = {
        **tool,
        "items": rows,
        "source_columns": columns,
        "original_row_count": len(original),
        "model_presentation": PRESENTATION,
    }
    if restore_financial_table(table) != original:
        raise ValueError("MODEL_FINANCIAL_TABLE_INVALID")
    return {
        **payload,
        "frozen_inputs": {**inputs, "financial_facts": table},
        "instructions": instructions + FORMAT_NOTE,
    }


def restore_financial_table(tool: dict[str, Any]) -> list[dict[str, Any]]:
    """Reject damaged tables; never fill a missing observation or deduplicate conflicts."""
    if tool.get("model_presentation") != PRESENTATION:
        raise ValueError("MODEL_FINANCIAL_TABLE_INVALID")
    columns, count, rows = (
        tool.get("source_columns"),
        tool.get("original_row_count"),
        tool.get("items"),
    )
    if (
        not isinstance(columns, list)
        or not isinstance(rows, list)
        or type(count) is not int
        or count < 0
    ):
        raise ValueError("MODEL_FINANCIAL_TABLE_INVALID")
    result: dict[int, dict[str, Any]] = {}
    try:
        for row in rows:
            if not isinstance(row, dict) or set(row) != {*ROW_FIELDS, "cells"}:
                raise ValueError("MODEL_FINANCIAL_TABLE_INVALID")
            if not isinstance(row["cells"], list):
                raise ValueError("MODEL_FINANCIAL_TABLE_INVALID")
            for index, ordinal, value in row["cells"]:
                if (
                    type(index) is not int
                    or not 0 <= index < len(columns)
                    or type(ordinal) is not int
                    or not 0 <= ordinal < count
                    or ordinal in result
                    or not isinstance(columns[index], dict)
                    or any(field in columns[index] for field in (*ROW_FIELDS, "value"))
                ):
                    raise ValueError("MODEL_FINANCIAL_TABLE_INVALID")
                result[ordinal] = deepcopy(
                    {
                        **{field: row[field] for field in ROW_FIELDS},
                        **columns[index],
                        "value": value,
                    }
                )
        if sorted(result) != list(range(count)):
            raise ValueError("MODEL_FINANCIAL_TABLE_INVALID")
        return [result[index] for index in range(count)]
    except (KeyError, TypeError, IndexError) as error:
        raise ValueError("MODEL_FINANCIAL_TABLE_INVALID") from error
