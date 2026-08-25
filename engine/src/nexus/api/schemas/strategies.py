"""Read-only strategy registry contracts for the locked Phase 0-1 baseline."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.time import require_utc
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _StrictStrategySchema(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class BacktestEvidenceSummaryResponse(_StrictStrategySchema):
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    data_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    code_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    start_session: date
    end_session: date
    oos_window_count: int = Field(ge=0)
    comparison_keys: tuple[Literal["spy", "equal_weight", "champion"], ...]
    benchmark_comparison_complete: bool
    data_mode: Literal["fixture", "delayed", "real_time", "mixed"]
    data_sources: tuple[str, ...] = Field(min_length=1)
    completed_at: datetime

    @field_validator("completed_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _consistent_evidence(self) -> BacktestEvidenceSummaryResponse:
        if self.start_session > self.end_session:
            raise ValueError("backtest date range must be ordered")
        if len(self.comparison_keys) != len(set(self.comparison_keys)):
            raise ValueError("comparison keys must be unique")
        complete = set(self.comparison_keys) == {"spy", "equal_weight", "champion"}
        if self.benchmark_comparison_complete != complete:
            raise ValueError("benchmark completeness must match the comparison keys")
        if not self.data_sources or len(self.data_sources) != len(set(self.data_sources)):
            raise ValueError("backtest evidence requires unique data-source provenance")
        sources_by_domain = {
            domain: tuple(source for source in self.data_sources if source.startswith(f"{domain}:"))
            for domain in ("price", "universe", "sector", "corporate_action", "calendar")
        }
        if any(not sources for sources in sources_by_domain.values()):
            raise ValueError("backtest evidence provenance must cover every authority domain")
        market_sources = tuple(
            source
            for domain in ("price", "universe", "sector", "corporate_action")
            for source in sources_by_domain[domain]
        )
        fixture_flags = tuple("fixture" in source.split(":") for source in market_sources)
        price_modes = {source.rsplit(":", 1)[-1] for source in sources_by_domain["price"]}
        mode_consistent = (
            (self.data_mode == "fixture" and all(fixture_flags))
            or (
                self.data_mode in {"delayed", "real_time"}
                and not any(fixture_flags)
                and price_modes == {self.data_mode}
            )
            or (
                self.data_mode == "mixed"
                and ((any(fixture_flags) and not all(fixture_flags)) or len(price_modes) > 1)
            )
        )
        if not mode_consistent:
            raise ValueError("backtest data mode must match its authority provenance")
        return self


class StrategySummaryResponse(_StrictStrategySchema):
    strategy_version: Literal["baseline_v1"]
    champion: Literal[True]
    evolution_status: Literal["locked"]
    rebalance_frequency: Literal["weekly"]
    historical_agent_overlay: Literal[False]
    live_execution_enabled: Literal[False]
    latest_backtest: BacktestEvidenceSummaryResponse | None = None


class StrategyListEnvelope(_StrictStrategySchema):
    data: tuple[StrategySummaryResponse, ...]
    meta: SuccessMetaResponse
