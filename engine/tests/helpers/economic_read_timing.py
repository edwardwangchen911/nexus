"""Scoped diagnostics around actual economic GET; no facts or request identities."""

import json
import time
from contextlib import ExitStack
from contextvars import ContextVar
from functools import wraps
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import event


class EconomicReadTiming:
    def __init__(self, application, directory: str, case: str):
        from nexus.api.routes import paper as routes
        from nexus.paper import economic_read
        from nexus.paper.read_service import PaperReadService

        self.path = Path(directory) / f"{case}.json"
        if not self.path.parent.is_absolute() or self.path.parent.stat().st_mode & 0o077:
            raise AssertionError("private timing directory required")
        self.records = []
        self.active = ContextVar("economic_read_timing", default=None)
        self.stack = ExitStack()
        self.engine = application.state.session_factory.kw["bind"].sync_engine

        def statement(conn, cursor, sql, parameters, context, executemany):
            current = self.active.get()
            if current is not None:
                current["sql_count"] += 1

        self.statement = statement
        event.listen(self.engine, "before_cursor_execute", statement)

        def instrument(label, original):
            @wraps(original)
            async def timed(*args, **kwargs):
                current = self.active.get()
                if current is None:
                    return await original(*args, **kwargs)
                before = current["sql_count"]
                started = time.monotonic()
                try:
                    return await original(*args, **kwargs)
                finally:
                    current["phases"].append(
                        {
                            "phase": label,
                            "seconds": time.monotonic() - started,
                            "sql_count": current["sql_count"] - before,
                        }
                    )

            return timed

        for owner, name, label in [
            (
                economic_read,
                "_read_economic_pair"
                if hasattr(economic_read, "_read_economic_pair")
                else "read_economic_pair",
                "pair",
            ),
            (economic_read._EconomicLaneReadService, "account", "account"),
            (economic_read, "require_economic_lane", "lane_authority"),
            (PaperReadService, "management_cycle_detail", "management"),
            (PaperReadService, "_orders_for_cycle", "orders"),
            (PaperReadService, "current_actual", "actual"),
            (PaperReadService, "portfolio", "portfolio"),
        ]:
            self.stack.enter_context(
                patch.object(owner, name, instrument(label, getattr(owner, name)))
            )
        original = routes.read_economic_lane

        @wraps(original)
        async def request(*args, **kwargs):
            record = {"sql_count": 0, "phases": []}
            token = self.active.set(record)
            started = time.monotonic()
            try:
                return await original(*args, **kwargs)
            finally:
                record["seconds"] = time.monotonic() - started
                self.records.append(record)
                self.active.reset(token)

        self.stack.enter_context(patch.object(routes, "read_economic_lane", request))

    def close(self):
        self.stack.close()
        event.remove(self.engine, "before_cursor_execute", self.statement)
        self.path.write_text(json.dumps(self.records, indent=2))
        self.path.chmod(0o600)
