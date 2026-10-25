"""Frozen, value-free price-review notification contract; no research admission."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator

from nexus.domain.time import require_utc

RESEARCH_MONITOR_EVENT = ("research.monitor.triggered", "research_price_trigger_v1")


class ResearchPriceTriggerNotification(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    schema_version: Literal["research_price_trigger_v1"]
    trigger_id: UUID
    plan_id: UUID
    source_run_id: UUID
    instrument_id: UUID
    cause: Literal["initial_match", "crossed"]
    occurred_at: datetime

    @field_validator("occurred_at", mode="before")
    @classmethod
    def _timestamp_text(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("MONITOR_NOTIFICATION_TIME_INVALID")
        return value

    @field_validator("occurred_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    def render(self) -> tuple[str, str]:
        cause = (
            "首次观察到价格复核条件已满足"
            if self.cause == "initial_match"
            else "价格复核条件由不满足转为满足"
        )
        return (
            "[研究复核] 价格条件满足",
            f"{cause}。\n触发时研究待准入，待后续复核。\n"
            f"触发时间：{self.occurred_at:%Y-%m-%d %H:%M:%S} UTC",
        )
