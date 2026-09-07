"""Boundary tests for the fixed, read-only research tool catalogue."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.agents.database_tools import _focused_diagnostics
from nexus.agents.portfolio_context import ContextDiagnostic
from nexus.agents.tools import EXPECTED_READ_ONLY_TOOL_NAMES, ReadOnlyTools, ToolInputError


def test_read_only_catalogue_has_only_the_eight_declared_tools() -> None:
    """Adding a capability or accepting an arbitrary selector would cross the Agent boundary."""
    tools = ReadOnlyTools()
    assert tools.names == EXPECTED_READ_ONLY_TOOL_NAMES
    assert len(tools.names) == 8
    assert not any("order" in name.lower() for name in tools.names)


def test_diagnostic_view_keeps_portfolio_categories_and_target_security() -> None:
    target = UUID(int=1)
    other = UUID(int=2)

    def item(code: str) -> ContextDiagnostic:
        return ContextDiagnostic(
            code=code,
            severity="info",
            actionability="research",
            unknown_reason=None,
        )

    selected, narrowed = _focused_diagnostics(
        (
            item("CONCENTRATION_TOP1"),
            item("RISK_BUCKET_EXPOSURE:EQUITY"),
            item(f"VOLATILITY_63:{target}"),
            item(f"VOLATILITY_63:{other}"),
        ),
        instrument_id=target,
    )

    assert narrowed is True
    assert tuple(value.code for value in selected) == (
        "CONCENTRATION_TOP1",
        "RISK_BUCKET_EXPOSURE:EQUITY",
        f"VOLATILITY_63:{target}",
    )


def test_tool_payloads_accept_only_typed_allowlisted_fields() -> None:
    """A URL, SQL, path, broker identifier, or exact assets must fail before any tool work."""
    tools = ReadOnlyTools()
    payload = tools.validate(
        "get_financial_facts",
        {
            "instrument_id": "0198d500-1000-7000-8000-000000000112",
            "data_snapshot_id": "0198d500-1000-7000-8000-000000000113",
            "as_of": datetime(2026, 8, 24, tzinfo=UTC),
            "fields": ["revenue", "operating_income"],
        },
    )
    assert payload["fields"] == ("revenue", "operating_income")
    for forbidden_key in ("url", "sql", "path", "broker_id", "account_id", "total_assets"):
        with pytest.raises(ToolInputError):
            tools.validate(
                "get_financial_facts",
                {
                    "instrument_id": UUID("0198d500-1000-7000-8000-000000000112"),
                    "data_snapshot_id": UUID("0198d500-1000-7000-8000-000000000113"),
                    "as_of": datetime(2026, 8, 24, tzinfo=UTC),
                    "fields": ["revenue"],
                    forbidden_key: "untrusted",
                },
            )
