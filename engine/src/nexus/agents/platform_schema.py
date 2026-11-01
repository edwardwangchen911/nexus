"""Platform-only structured-output encoding; original models still validate output.

The API supports a JSON Schema subset, unlike the historical SDK schema.
Only the known SEC fixed pair is lowered to homogeneous array items. Its exact
order remains enforced by DisclosureReviewCondition in the normal stage parser.
Unknown constructs fail before allocation/dispatch, never silently lose rules.
"""

from typing import Any

from pydantic import BaseModel

from nexus.agents.codex_sdk import codex_output_schema

LEGACY_PLATFORM_UNITS_VERSION = "platform-full-http-utf8-plus8192-v1"
CURRENT_PLATFORM_UNITS_VERSION = "platform-structured-http-utf8-plus8192-v2"
PLATFORM_UNITS_VERSIONS = frozenset({LEGACY_PLATFORM_UNITS_VERSION, CURRENT_PLATFORM_UNITS_VERSION})
_KEYWORDS = frozenset(
    {
        "$defs",
        "$ref",
        "title",
        "description",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "anyOf",
        "enum",
        "const",
        "minItems",
        "maxItems",
        "minLength",
        "maxLength",
        "pattern",
        "format",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
    }
)
_TYPES = {"object", "array", "string", "number", "integer", "boolean", "null"}
_FORMATS = {"date-time", "time", "date", "duration", "email", "hostname", "ipv4", "ipv6", "uuid"}


def platform_output_schema(
    model: type[BaseModel], *, units_version: str = CURRENT_PLATFORM_UNITS_VERSION
) -> dict[str, Any]:
    schema = codex_output_schema(model)
    if units_version == LEGACY_PLATFORM_UNITS_VERSION:
        return schema
    if units_version != CURRENT_PLATFORM_UNITS_VERSION:
        raise ValueError("PLATFORM_ALLOCATION_CHANGED")
    error = "PLATFORM_OUTPUT_SCHEMA_UNSUPPORTED"
    if schema.get("type") != "object" or "anyOf" in schema:
        raise ValueError(error)
    definitions = schema.get("$defs", {})
    if not isinstance(definitions, dict):
        raise ValueError(error)

    def visit(node: Any) -> None:
        if not isinstance(node, dict):
            raise ValueError(error)
        if "prefixItems" in node:
            # No generic tuple rewrite: preserve every other constraint and
            # reject any shape except the existing fixed SEC disclosure pair.
            if (
                node.get("type") != "array"
                or node["prefixItems"]
                != [
                    {"const": "8-K", "type": "string"},
                    {"const": "8-K/A", "type": "string"},
                ]
                or node.get("minItems") != 2
                or node.get("maxItems") != 2
                or "items" in node
            ):
                raise ValueError(error)
            del node["prefixItems"]
            node["items"] = {"type": "string", "enum": ["8-K", "8-K/A"]}
        if set(node) - _KEYWORDS:
            raise ValueError(error)
        if "format" in node and (
            not isinstance(node["format"], str) or node["format"] not in _FORMATS
        ):
            raise ValueError(error)
        kind = node.get("type")
        if kind is not None and (
            not isinstance(kind, str | list)
            or any(
                not isinstance(t, str) or t not in _TYPES
                for t in (kind if isinstance(kind, list) else [kind])
            )
        ):
            raise ValueError(error)
        if kind == "object":
            properties = node.get("properties", {})
            if (
                not isinstance(properties, dict)
                or node.get("additionalProperties") is not False
                or set(node.get("required", [])) != set(properties)
            ):
                raise ValueError(error)
        if kind == "array" and "items" not in node:
            raise ValueError(error)
        if "$ref" in node and (
            not isinstance(node["$ref"], str)
            or not node["$ref"].startswith("#/$defs/")
            or node["$ref"].removeprefix("#/$defs/") not in definitions
        ):
            raise ValueError(error)
        # Traverse schema positions only: property names and enum values are
        # data, even when they happen to be names of unsupported keywords.
        for key in ("$defs", "properties"):
            for child in node.get(key, {}).values():
                visit(child)
        for child in node.get("anyOf", []):
            visit(child)
        if "items" in node:
            visit(node["items"])

    visit(schema)
    return schema
