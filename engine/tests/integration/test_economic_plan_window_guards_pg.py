"""Forged stage references fail service reads and the actual insert trigger."""

from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.paper.economic_window import _hash, _read_window
from nexus.persistence.models.economic import EconomicCommonWindowRecord
from sqlalchemy.exc import DBAPIError


async def verify_plan_window_guards(session, row, root_orders):
    original = {
        column.name: getattr(row, column.name)
        for column in EconomicCommonWindowRecord.__table__.columns
    }
    await _read_window(session, SimpleNamespace(**original))
    for mode in ("plan_hash", "event_hash", "omit_order", "root_order", "wrong_lane"):
        document = deepcopy(row.qualification)
        if mode == "plan_hash":
            document["plan_event_hash"] = "0" * 64
        elif mode == "event_hash":
            document["order_states"][0]["event_hash"] = "0" * 64
        elif mode == "omit_order":
            omitted = document["order_states"].pop()
            document["orders"].remove(omitted["order_id"])
        elif mode == "root_order":
            document["order_states"][0]["order_id"] = str(root_orders[0].id)
            document["orders"][0] = str(root_orders[0].id)
            document["orders"].sort()
        else:
            document["order_states"][0]["lane"] = (
                "challenger" if document["order_states"][0]["lane"] == "incumbent" else "incumbent"
            )
        values = {
            **original,
            "qualification": document,
            "content_hash": _hash(document),
        }
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_(INVALID|NOT_READY)"):
            await _read_window(session, SimpleNamespace(**values))
        with pytest.raises(DBAPIError, match="ECONOMIC_WINDOW_INVALID"):
            async with session.begin_nested():
                session.add(EconomicCommonWindowRecord(**{**values, "id": uuid4()}))
                await session.flush()
