"""Pure canonical digests shared by paper execution writes and reads."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID


def canonical_time(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def canonical_decimal(value: Decimal) -> str:
    return format(value, ".12f")


def _digest(schema: str, payload: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            {"schema": schema, **payload},
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()


def canonical_paper_fact_hash(kind: str, payload: dict[str, object]) -> str:
    return _digest("paper_fact_payload_sha256_v1", {"kind": kind, "payload": payload})


def canonical_cancel_request_hash(*, order_id: UUID, expected_event_seq: int) -> str:
    return _digest(
        "paper_cancel_request_sha256_v1",
        {"order_id": str(order_id), "expected_event_seq": expected_event_seq},
    )


def canonical_cancel_result_hash(
    *,
    order_id: UUID,
    account_id: UUID,
    total_quantity: Decimal,
    filled_quantity: Decimal,
    result_event_seq: int,
) -> str:
    return _digest(
        "paper_cancel_result_sha256_v1",
        {
            "order_id": str(order_id),
            "account_id": str(account_id),
            "status": "cancelled",
            "filled_quantity": canonical_decimal(filled_quantity),
            "unfilled_quantity": canonical_decimal(total_quantity - filled_quantity),
            "result_event_seq": result_event_seq,
        },
    )
