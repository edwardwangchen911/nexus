"""Offline-only synthetic fixture transport."""

import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

from nexus_fosun_gateway.adapter.normalization import (
    normalize_cash_summary_response,
    normalize_holdings_response,
    normalize_provider_response,
)
from nexus_fosun_gateway.domain import CapabilityItem, GatewayItem, GatewayPage
from nexus_fosun_gateway.policy.operations import (
    READ_ONLY_OPERATIONS,
    GatewayOperation,
)


class FixtureFosunTransport:
    """Return deterministic sanitized data without importing a provider SDK."""

    def __init__(self, fixture_dir: Path | None = None) -> None:
        self._fixture_dir = fixture_dir
        self._closed = False

    def is_ready(self) -> bool:
        return not self._closed

    def readiness_expires_at(self) -> datetime | None:
        return None

    async def aclose(self) -> None:
        self._closed = True

    async def execute(
        self,
        operation: GatewayOperation,
        payload: Mapping[str, object],
    ) -> GatewayPage[GatewayItem]:
        if self._closed:
            raise RuntimeError("FIXTURE_TRANSPORT_CLOSED")
        fixture_case = payload.get("fixture_case")
        if self._fixture_dir is not None and isinstance(fixture_case, str):
            if re.fullmatch(r"[a-z][a-z0-9_]{0,63}", fixture_case) is None:
                raise ValueError("invalid fixture case")
            document = json.loads((self._fixture_dir / f"{fixture_case}.json").read_text())
            if document.get("operation") != operation:
                raise ValueError("fixture operation mismatch")
            received_at = _received_at(document.get("received_at"))
            context = document.get("request_payload")
            if not isinstance(context, Mapping):
                context = {}
            if operation == "fosun_sync_portfolio":
                responses = document.get("provider_responses")
                if not isinstance(responses, Mapping):
                    raise ValueError("portfolio fixture responses missing")
                account_id = context.get("account_id")
                if not isinstance(account_id, str):
                    raise ValueError("portfolio fixture account missing")
                read_kind = context.get("read_kind")
                if read_kind == "cash":
                    return normalize_cash_summary_response(
                        responses.get("assets"),
                        account_id=account_id,
                        received_at=received_at,
                    )
                if read_kind == "holdings":
                    return normalize_holdings_response(
                        responses.get("holdings"),
                        account_id=account_id,
                        received_at=received_at,
                        context=context,
                    )
                raise ValueError("portfolio fixture read kind missing")
            return normalize_provider_response(
                operation,
                document.get("provider_response"),
                received_at=received_at,
                context=context,
            )
        now = datetime.now(UTC)
        if operation == "fosun_capabilities":
            items: tuple[GatewayItem, ...] = (
                CapabilityItem(
                    mode="fixture",
                    operations=tuple(sorted(READ_ONLY_OPERATIONS)),
                ),
            )
        else:
            items = ()
        return GatewayPage[GatewayItem](
            items=items,
            source_request_id=f"fixture-{operation}",
            has_more=False,
            next_cursor=None,
            as_of=now,
        )


def _received_at(value: object) -> datetime:
    if not isinstance(value, str):
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("fixture received_at must be timezone-aware")
    return parsed.astimezone(UTC)
