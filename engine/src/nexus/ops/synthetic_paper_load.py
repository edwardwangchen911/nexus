"""Diagnostic-only synthetic model load; never create Paper or provider authority."""

from collections.abc import Mapping
from datetime import datetime, timedelta
from uuid import UUID

from nexus.agents.tools import ReadOnlyToolRegistry


class SyntheticPaperLoadTools:
    diagnostic_synthetic_paper_load = True

    def __init__(
        self, base: ReadOnlyToolRegistry, *, instrument_id: UUID, decision_time: datetime
    ) -> None:
        self._base = base
        self.names = base.names
        self.filing_forms = getattr(base, "filing_forms", ("10-K", "10-Q", "8-K", "6-K", "497K"))
        # Fixed fixture values, unrelated to any private account or provider quote.
        self._context = {
            "cycle_id": str(UUID(int=0)),
            "instrument_id": str(instrument_id),
            "decision_time": decision_time.isoformat(),
            "decision_quote": {
                "source": "diagnostic_synthetic",
                "feed": "fixture",
                "delivery_mode": "fixture",
                "delayed": False,
                "bid": "100.000000000000",
                "ask": "100.100000000000",
                "observed_at": decision_time.isoformat(),
                "available_at": decision_time.isoformat(),
                "received_at": decision_time.isoformat(),
            },
            "prior_daily": {
                "source": "diagnostic_synthetic",
                "feed": "fixture",
                "delivery_mode": "fixture",
                "adjustment": "all",
                "session": (decision_time - timedelta(days=1)).date().isoformat(),
                "close": "100.000000000000",
            },
            "paper_portfolio": {
                "candidate_position_weight": "0.150000000000",
                "cash_weight": "0.800000000000",
                "projection_hash": "0" * 64,
            },
            "execution_locked": True,
        }

    async def execute(self, name: str, payload: Mapping[str, object]) -> Mapping[str, object]:
        result = await self._base.execute(name, payload)
        if name != "get_security_snapshot":
            return result
        items = result.get("items")
        if not isinstance(items, list):
            raise ValueError("DIAGNOSTIC_SYNTHETIC_LOAD_INPUT_INVALID")
        return {
            **result,
            "diagnostic_synthetic_paper_load": {
                "source": "diagnostic_synthetic",
                "diagnostic_only": True,
                "agent_paper_context": self._context,
            },
        }
