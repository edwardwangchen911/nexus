"""Frozen SEC discovery facts, independent of a research plan or its expiry."""

from datetime import datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.domain.time import require_utc

DURABLE_SEC_EVENT = ("research.sec.discovered", "durable_sec_notice_v1")


class DurableSecNotification(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    schema_version: Literal["durable_sec_notice_v1"] = "durable_sec_notice_v1"
    discovery_id: UUID
    instrument_id: UUID
    rule_id: UUID
    symbol: str | None = Field(default=None, pattern=r"^[A-Z0-9][A-Z0-9.\-]{0,15}$")
    event_key: str = Field(pattern=r"^sec:[0-9]{10}:[0-9]{10}-[0-9]{2}-[0-9]{6}$")
    form: Literal["8-K", "8-K/A"]
    accepted_at: datetime
    discovered_at: datetime

    @field_validator("accepted_at", "discovered_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.accepted_at > self.discovered_at:
            raise ValueError("SEC_NOTICE_TIME_INVALID")
        return self

    def render(self) -> tuple[str, str]:
        accepted = self.accepted_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        discovered = self.discovered_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        return (
            "[公告发现] SEC 披露已收录",
            f"{self.symbol or '已登记证券'} · {self.form}\nSEC 接受时间：{accepted}\n"
            f"首次发现：{discovered}\n投资判断待更新。公告事实不代表投资结论或交易指令。",
        )
