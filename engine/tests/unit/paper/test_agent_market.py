from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.agent_market import (
    PaperMarketError,
    agent_paper_window_bounds,
    qualify_agent_paper_window,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
)
from nexus.providers.base import InstrumentId, MarketDataProvenance, PriceBar, ProviderPage
from nexus.quality.source_policy import (
    paper_decision_price_authorized,
    paper_execution_price_authorized,
)

INSTRUMENT_ID = UUID("71000000-0000-0000-0000-000000000001")
WINDOW_START = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
EVALUATED_AT = WINDOW_START + timedelta(minutes=5, seconds=2)


def _page(mutation: str | None = None) -> ProviderPage[PriceBar]:
    observed = [WINDOW_START + timedelta(minutes=index) for index in range(5)]
    if mutation == "four_bars":
        observed.pop()
    elif mutation == "duplicate_minute":
        observed[-1] = observed[-2]
    bars = tuple(
        PriceBar(
            instrument_id=InstrumentId(
                UUID("71000000-0000-0000-0000-000000000002")
                if mutation == "wrong_instrument" and index == 4
                else INSTRUMENT_ID
            ),
            session=date(2026, 9, 14),
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100.5"),
            volume=Decimal("1000"),
            observed_at=value,
            received_at=(
                EVALUATED_AT + timedelta(seconds=1)
                if mutation == "future_received_at" and index == 4
                else EVALUATED_AT - timedelta(seconds=1)
            ),
            available_at=(
                EVALUATED_AT + timedelta(seconds=1)
                if mutation == "future_available_at" and index == 4
                else EVALUATED_AT - timedelta(seconds=1)
            ),
        )
        for index, value in enumerate(observed)
    )
    return ProviderPage(
        items=bars,
        next_cursor="more" if mutation == "pagination" else None,
        raw_payload=b"" if mutation == "missing_raw_artifact" else b'{"bars":[]}',
        source_request_id="request-1",
        as_of=EVALUATED_AT - timedelta(seconds=1),
        provenance=MarketDataProvenance(
            source="alpaca",
            feed="sip" if mutation == "sip_feed" else "iex",
            adjustment="all" if mutation == "adjusted" else "raw",
            interval="1Day" if mutation == "daily" else "1Min",
            mode="delayed" if mutation == "delayed" else "read_only",
            delivery_mode="delayed" if mutation == "delayed" else "real_time",
        ),
    )


def test_iex_window_requires_five_consecutive_minutes() -> None:
    qualified = qualify_agent_paper_window(
        page=_page(),
        instrument_id=INSTRUMENT_ID,
        window_start=WINDOW_START,
        evaluated_at=EVALUATED_AT,
    )

    assert qualified.window_start == WINDOW_START
    assert qualified.window_end == WINDOW_START + timedelta(minutes=5)
    assert qualified.bar_count == 5
    assert qualified.source == "alpaca"
    assert qualified.feed == "iex"
    assert qualified.delivery_mode == "real_time"


def test_schwab_window_uses_next_minute_after_output_and_rejects_close_overrun() -> None:
    output_available_at = datetime(2026, 9, 14, 19, 52, 0, tzinfo=UTC)
    market_close_at = datetime(2026, 9, 14, 20, 0, tzinfo=UTC)

    assert agent_paper_window_bounds(output_available_at, market_close_at) == (
        datetime(2026, 9, 14, 19, 53, tzinfo=UTC),
        datetime(2026, 9, 14, 19, 58, tzinfo=UTC),
    )
    with pytest.raises(PaperMarketError, match="PAPER_MARKET_WINDOW_UNAVAILABLE"):
        agent_paper_window_bounds(datetime(2026, 9, 14, 19, 55, 0, tzinfo=UTC), market_close_at)


def test_schwab_price_history_window_is_qualified_as_unknown_delivery() -> None:
    page = _page()
    schwab = page.model_copy(
        update={
            "provenance": page.provenance.model_copy(
                update={"source": "schwab", "feed": "price_history", "delivery_mode": "unknown"}
            )
        }
    )

    qualified = qualify_agent_paper_window(
        page=schwab,
        instrument_id=INSTRUMENT_ID,
        window_start=WINDOW_START,
        evaluated_at=EVALUATED_AT,
        expected_source="schwab",
    )

    assert qualified.source == "schwab"
    assert qualified.feed == "price_history"
    assert qualified.delivery_mode == "unknown"


def test_schwab_price_history_rejects_a_stale_latest_bar() -> None:
    page = _page()
    stale_received = WINDOW_START + timedelta(minutes=7, seconds=1)
    schwab = page.model_copy(
        update={
            "items": tuple(
                bar.model_copy(
                    update={"available_at": stale_received, "received_at": stale_received}
                )
                for bar in page.items
            ),
            "provenance": page.provenance.model_copy(
                update={"source": "schwab", "feed": "price_history", "delivery_mode": "unknown"}
            ),
        }
    )

    with pytest.raises(PaperMarketError, match="PAPER_MARKET_WINDOW_UNAVAILABLE"):
        qualify_agent_paper_window(
            page=schwab,
            instrument_id=INSTRUMENT_ID,
            window_start=WINDOW_START,
            evaluated_at=stale_received,
            expected_source="schwab",
        )


def test_schwab_price_history_reports_incomplete_window_as_unavailable() -> None:
    page = _page("four_bars")
    schwab = page.model_copy(
        update={
            "provenance": page.provenance.model_copy(
                update={"source": "schwab", "feed": "price_history", "delivery_mode": "unknown"}
            )
        }
    )

    with pytest.raises(PaperMarketError, match="PAPER_MARKET_WINDOW_UNAVAILABLE"):
        qualify_agent_paper_window(
            page=schwab,
            instrument_id=INSTRUMENT_ID,
            window_start=WINDOW_START,
            evaluated_at=EVALUATED_AT,
            expected_source="schwab",
        )


def test_agent_paper_price_purposes_require_their_exact_policy_ids() -> None:
    unrelated = UUID(int=1)

    assert paper_decision_price_authorized(PAPER_DECISION_SOURCE_POLICY_ID)
    assert not paper_decision_price_authorized(PAPER_EXECUTION_SOURCE_POLICY_ID)
    assert not paper_decision_price_authorized(unrelated)
    assert paper_execution_price_authorized(PAPER_EXECUTION_SOURCE_POLICY_ID)
    assert not paper_execution_price_authorized(PAPER_DECISION_SOURCE_POLICY_ID)
    assert not paper_execution_price_authorized(unrelated)


@pytest.mark.parametrize(
    "mutation",
    [
        "four_bars",
        "duplicate_minute",
        "future_available_at",
        "future_received_at",
        "sip_feed",
        "delayed",
        "adjusted",
        "daily",
        "wrong_instrument",
        "missing_raw_artifact",
        "pagination",
    ],
)
def test_iex_window_rejects_incomplete_or_wrong_authority(mutation: str) -> None:
    with pytest.raises(PaperMarketError, match="PAPER_MARKET_POLICY_REJECTED"):
        qualify_agent_paper_window(
            page=_page(mutation),
            instrument_id=INSTRUMENT_ID,
            window_start=WINDOW_START,
            evaluated_at=EVALUATED_AT,
        )
