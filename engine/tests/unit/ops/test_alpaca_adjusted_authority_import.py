"""Adjusted authority import stays archive-only and fail-closed."""

import importlib
import json
from datetime import UTC, date, datetime
from uuid import UUID

import pytest
from nexus.ops.alpaca_adjusted_authority_import import _ArchivedTransport
from nexus.providers.base import BarsRequest, InstrumentId
from nexus.research.alpaca_prices_archive import PriceOriginal, ValidatedPriceArchive


def _archive(*, pages: int = 1) -> ValidatedPriceArchive:
    instrument_id = InstrumentId(UUID("00000000-0000-4000-8000-000000000001"))
    request = BarsRequest(
        instrument_id=instrument_id,
        start=datetime(2024, 1, 2, tzinfo=UTC),
        end=datetime(2024, 6, 29, tzinfo=UTC),
        interval="1Day",
        adjustment="all",
    )
    received = datetime(2024, 6, 30, tzinfo=UTC)
    raw = b'{"symbol":"AAPL","bars":[],"next_page_token":null}'
    originals = tuple(
        PriceOriginal(f"page-{index:03}.json", raw, received)
        for index in range(1, pages + 1)
    )
    return ValidatedPriceArchive(
        request=request,
        symbol="AAPL",
        identity_asof=date(2024, 6, 28),
        received_at=received,
        manifest_content_hash="0" * 64,
        originals=originals,
    )


@pytest.mark.asyncio
async def test_archived_transport_returns_exact_single_page_without_network() -> None:
    archive = _archive()
    response = await _ArchivedTransport(archive).fetch_bars(archive.request, None)

    assert response.raw_payload == archive.originals[0].content
    assert response.payload["next_page_token"] is None


def test_multi_page_archive_is_not_silently_truncated() -> None:
    with pytest.raises(ValueError, match="SINGLE_PAGE_REQUIRED"):
        _ArchivedTransport(_archive(pages=2))


def test_agent_paper_purpose_selects_its_decision_price_policy() -> None:
    entry = importlib.import_module("nexus.ops.alpaca_adjusted_authority_import")

    assert entry._source_policy_id("diagnostic") is None
    assert entry._source_policy_id("agent-paper-decision") == UUID(
        "2573d63a-fb6d-53d3-8d44-207766a78322"
    )


def test_invalid_cli_fails_before_loading_local_configuration(monkeypatch, capsys) -> None:
    name = "nexus.ops.alpaca_adjusted_authority_import"
    assert importlib.util.find_spec(name) is not None
    entry = importlib.import_module(name)
    monkeypatch.setattr("sys.argv", ["alpaca_adjusted_authority_import"])

    def forbidden(*args):
        raise AssertionError("invalid request must not load credentials")

    monkeypatch.setattr(entry, "load_local_environment", forbidden)
    assert entry.main() == 1
    output = capsys.readouterr()
    assert json.loads(output.err)["status"] == "rejected"
    assert "AssertionError" not in output.err
