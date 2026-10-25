"""Frozen monitor-batch identity and bounded notification prose; no admission or I/O."""

import re
from datetime import datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

RESEARCH_MONITOR_BATCH_EVENT = ("research.monitor.batch_triggered", "research_monitor_batch_v1")

_UUID_TEXT = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_EVENT_ID = re.compile(rf"(?:price:{_UUID_TEXT}|sec:[0-9]{{10}}:[0-9]{{10}}-[0-9]{{2}}-[0-9]{{6}})")
_UTC_TEXT = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|\+00:00)"
)


class ResearchMonitorBatchNotification(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    schema_version: Literal["research_monitor_batch_v1"]
    batch_id: UUID
    instrument_id: UUID
    trigger_ids: tuple[UUID, ...] = Field(min_length=1, max_length=1001)
    event_ids: tuple[str, ...] = Field(min_length=1, max_length=1001)
    source_run_ids: tuple[UUID, ...] = Field(min_length=1, max_length=2)
    occurred_at: datetime

    @field_validator("trigger_ids", "event_ids", "source_run_ids", mode="before")
    @classmethod
    def _ordered_collection(cls, value: object) -> object:
        if not isinstance(value, list | tuple):
            raise ValueError("MONITOR_BATCH_COLLECTION_INVALID")
        return value

    @field_validator("event_ids", mode="before")
    @classmethod
    def _event_ids(cls, value: object) -> object:
        if (
            not isinstance(value, list | tuple)
            or not 1 <= len(value) <= 1001
            or any(not isinstance(item, str) or _EVENT_ID.fullmatch(item) is None for item in value)
        ):
            raise ValueError("MONITOR_BATCH_EVENT_INVALID")
        return value

    @field_validator("occurred_at", mode="before")
    @classmethod
    def _utc_timestamp_text(cls, value: object) -> object:
        if not isinstance(value, str) or _UTC_TEXT.fullmatch(value) is None:
            raise ValueError("MONITOR_BATCH_TIME_INVALID")
        return value

    @model_validator(mode="after")
    def _unique_ordered_members(self) -> Self:
        for members in (self.trigger_ids, self.event_ids, self.source_run_ids):
            identities = tuple(str(item) for item in members)
            if identities != tuple(sorted(set(identities))):
                raise ValueError("MONITOR_BATCH_COLLECTION_INVALID")
        if len(self.trigger_ids) != len(self.event_ids):
            raise ValueError("MONITOR_BATCH_MEMBERSHIP_INVALID")
        return self

    def render(self) -> tuple[str, str]:
        return (
            "[研究复核] 监控批次已触发",
            f"本批次记录 {len(self.event_ids)} 个监控事件，"
            f"关联 {len(self.source_run_ids)} 份来源研究。\n"
            "新增收录不等于新发布，不代表重要性或影响判断。\n"
            "触发时研究待准入；用量未知时预算不自动释放。\n"
            "通知服务接受不等于设备送达。\n"
            f"触发时间：{self.occurred_at:%Y-%m-%d %H:%M:%S} UTC",
        )
