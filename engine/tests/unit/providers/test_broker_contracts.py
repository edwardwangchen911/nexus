from datetime import UTC, datetime
from decimal import Decimal

import pytest
from nexus.providers.broker.base import BrokerPage, CashBalance
from pydantic import ValidationError

NOW = datetime(2026, 8, 23, 4, 0, tzinfo=UTC)


def test_broker_contract_rejects_binary_float_and_non_finite_values() -> None:
    with pytest.raises(TypeError, match="binary floats"):
        CashBalance(currency="USD", cash_balance=1.0, available_cash=Decimal("1"))
    with pytest.raises(ValidationError, match="finite"):
        CashBalance(currency="USD", cash_balance=Decimal("NaN"), available_cash=Decimal("1"))


@pytest.mark.parametrize("currency", ("", "usd", "US", "USDD", "U1D"))
def test_broker_currency_is_three_uppercase_ascii_letters(currency: str) -> None:
    with pytest.raises(ValidationError, match="currency"):
        CashBalance(currency=currency, cash_balance=Decimal("1"), available_cash=Decimal("1"))


def test_broker_page_cursor_and_response_time_are_validated() -> None:
    with pytest.raises(ValidationError, match="pagination state"):
        BrokerPage[CashBalance](
            items=(),
            has_more=True,
            next_cursor=None,
            source_request_id="synthetic-request",
            as_of=NOW,
        )
    with pytest.raises(ValidationError, match="timezone-aware"):
        BrokerPage[CashBalance](
            items=(),
            has_more=False,
            next_cursor=None,
            source_request_id="synthetic-request",
            as_of=NOW.replace(tzinfo=None),
        )
