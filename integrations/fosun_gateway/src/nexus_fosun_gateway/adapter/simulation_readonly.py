"""One-shot simulation reads through the existing normalized SDK transport."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from types import SimpleNamespace
from typing import cast

from nexus_fosun_gateway.adapter.sdk import DualTokenBucket, RateLimiter, SdkFosunTransport
from nexus_fosun_gateway.domain import GatewayError, GatewayItem, GatewayPage
from nexus_fosun_gateway.policy.operations import GatewayOperation


def refusal(code: str) -> GatewayError:
    return GatewayError(code, code, status_code=503, next_action="REVIEW_SIMULATION_READ_ONLY")


class SimulationReadOnlyTransport:
    """No commands, account creation/reset, reconciliation, or execution authority."""

    def __init__(
        self,
        client: object,
        *,
        account_id: str,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        if not account_id or len(account_id) > 128 or account_id != account_id.strip():
            raise ValueError("simulation account selection is invalid")
        self._account_id = account_id
        self._client = client
        self._used = False
        self._verified = False
        self._date_from = ""
        self._date_to = ""
        # Only these five methods are exposed to the existing read transport.
        facade = SimpleNamespace(
            account=SimpleNamespace(list_accounts=self._accounts),
            portfolio=SimpleNamespace(
                get_assets_summary=self._cash,
                get_holdings=self._holdings,
            ),
            trade=SimpleNamespace(list_orders=self._orders, get_cash_flows=self._cash_flows),
        )
        self._transport = SdkFosunTransport(
            facade,
            max_attempts=1,
            rate_limiter=rate_limiter
            or DualTokenBucket(
                qps_capacity=1,
                qps_refill_per_second=1,
                requests_per_minute=30,
            ),
        )

    def _method(self, section: str, method: str) -> Callable[..., object]:
        value = getattr(getattr(self._client, section, None), method, None)
        if not callable(value):
            raise refusal("SIMULATION_SDK_CONTRACT_MISMATCH")
        return cast(Callable[..., object], value)

    def _accounts(self) -> object:
        self._verified = False
        raw = self._method("account", "list_accounts")()
        if not isinstance(raw, dict) or type(raw.get("code")) is not int or raw["code"] != 0:
            raise refusal("SIMULATION_ACCOUNT_UNVERIFIED")
        data = raw.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("subAccounts"), list):
            raise refusal("SIMULATION_ACCOUNT_UNVERIFIED")
        matches = [
            row
            for row in data["subAccounts"]
            if isinstance(row, dict) and row.get("subAccountId") == self._account_id
        ]
        if len(matches) != 1:
            raise refusal("SIMULATION_ACCOUNT_UNVERIFIED")
        row = matches[0]
        if (
            type(row.get("subAccountType")) is not int
            or row["subAccountType"] != 2
            or type(row.get("status")) is not int
            or row["status"] != 1
        ):
            raise refusal("SIMULATION_ACCOUNT_UNVERIFIED")
        self._verified = True
        # Other accounts and personal profile fields never leave this boundary.
        return {"code": 0, "requestId": raw.get("requestId"), "data": {"subAccounts": [row]}}

    def _read(self, section: str, method: str, kwargs: dict[str, object]) -> object:
        if not self._verified or kwargs.get("sub_account_id") != self._account_id:
            raise refusal("SIMULATION_ACCOUNT_UNVERIFIED")
        return self._method(section, method)(**kwargs)

    def _cash(self, **kwargs: object) -> object:
        return self._read("portfolio", "get_assets_summary", kwargs)

    def _holdings(self, **kwargs: object) -> object:
        return self._read("portfolio", "get_holdings", kwargs)

    def _orders(self, **kwargs: object) -> object:
        # The ordinary read adapter selects open orders. This bounded snapshot
        # reads all statuses within an explicit window, including historical orders.
        kwargs.update(status_arr=None, from_date=self._date_from, to_date=self._date_to)
        return self._read("trade", "list_orders", kwargs)

    def _cash_flows(self, **kwargs: object) -> object:
        return self._read("trade", "get_cash_flows", kwargs)

    async def collect(
        self,
        date_from: date,
        date_to: date,
        *,
        max_pages: int = 3,
    ) -> dict[str, object]:
        if date_from > date_to or (date_to - date_from).days > 31:
            raise ValueError("simulation read window must be at most 32 inclusive days")
        if type(max_pages) is not int or not 1 <= max_pages <= 3:
            raise ValueError("simulation page budget is invalid")
        if self._used:
            raise refusal("SIMULATION_READ_ALREADY_ATTEMPTED")
        self._used = True
        self._date_from, self._date_to = date_from.isoformat(), date_to.isoformat()
        try:
            accounts = await self._transport.execute("fosun_sync_accounts", {})
            cash = await self._transport.execute(
                "fosun_sync_portfolio",
                {
                    "account_id": self._account_id,
                    "read_kind": "cash",
                },
            )
            holdings = await self._pages("fosun_sync_portfolio", max_pages)
            orders = await self._pages("fosun_list_orders", max_pages)
            flows = await self._pages("fosun_list_cash_flows", 1)
            return {
                "mode": "local_read_only",
                "account_type": 2,
                "simulation_write_enabled": False,
                "execution_locked": True,
                "date_from": self._date_from,
                "date_to": self._date_to,
                "accounts": accounts.model_dump(mode="json"),
                "cash": cash.model_dump(mode="json"),
                "holdings": [page.model_dump(mode="json") for page in holdings],
                "orders": [page.model_dump(mode="json") for page in orders],
                "cash_flows": [page.model_dump(mode="json") for page in flows],
            }
        finally:
            self._verified = False
            await self._transport.aclose()

    async def _pages(
        self,
        operation: GatewayOperation,
        max_pages: int,
    ) -> list[GatewayPage[GatewayItem]]:
        pages: list[GatewayPage[GatewayItem]] = []
        cursor: str | None = None
        for _ in range(max_pages):
            page = await self._transport.execute(
                operation,
                {
                    "account_id": self._account_id,
                    "read_kind": "holdings",
                    "cursor": cursor,
                    "limit": 100,
                    "date_from": self._date_from,
                    "date_to": self._date_to,
                },
            )
            pages.append(page)
            if not page.has_more:
                return pages
            if page.next_cursor is None or int(page.next_cursor) <= int(cursor or "0"):
                raise refusal("SIMULATION_CURSOR_INVALID")
            cursor = page.next_cursor
        # CashFlows SDK has no pagination input; never replay it with a made-up cursor.
        raise refusal("SIMULATION_PAGE_LIMIT")
