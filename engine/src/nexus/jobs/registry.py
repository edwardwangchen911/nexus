"""Closed, strict handler registry for durable NEXUS Jobs."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.jobs.models import JOB_PRIORITIES, JobType, QueueName, RetryPolicy
from nexus.jobs.repository import ClaimedJob

HandlerOutcome = Literal["succeeded", "skipped", "failed"]
Clock = Callable[[], datetime]
_NAMESPACE = re.compile(r"^[a-z][a-z0-9._-]{0,63}$")
_FORBIDDEN_KEY_TOKENS = frozenset(
    {
        "code",
        "credential",
        "endpoint",
        "method",
        "path",
        "prompt",
        "secret",
        "sql",
        "token",
        "uri",
        "url",
    }
)
_CODE_PREFIX = re.compile(r"^\s*(?:SELECT\s|INSERT\s|UPDATE\s|DELETE\s|def\s|import\s)", re.I)


@dataclass(frozen=True)
class JobHandlerResult:
    outcome: HandlerOutcome
    result_code: str | None
    error_code: str | None = None


@dataclass(frozen=True)
class JobHandlerContext:
    session: AsyncSession
    claim: ClaimedJob
    payload: BaseModel
    _clock: Clock

    def finished_at(self) -> datetime:
        """Return the injected worker clock without exposing runtime configuration."""
        return self._clock()


JobHandler = Callable[[JobHandlerContext], Awaitable[JobHandlerResult]]


@dataclass(frozen=True)
class JobHandlerSpec:
    job_type: JobType
    schema_version: str
    payload_model: type[BaseModel]
    queue: QueueName
    timeout_seconds: float
    retry_policy: RetryPolicy
    max_attempts: int
    idempotency_namespace: str
    handler: JobHandler

    def __post_init__(self) -> None:
        if not 1 <= len(self.schema_version) <= 32:
            raise ValueError("handler schema_version is invalid")
        if self.payload_model.model_config.get("extra") != "forbid":
            raise ValueError("handler payload model must set extra='forbid'")
        if not 0 < self.timeout_seconds <= 45:
            raise ValueError("handler timeout_seconds must be in (0, 45]")
        if not 1 <= self.max_attempts <= 3:
            raise ValueError("handler max_attempts must be between 1 and 3")
        if _NAMESPACE.fullmatch(self.idempotency_namespace) is None:
            raise ValueError("handler idempotency_namespace is invalid")
        expected_queue = (
            QueueName.BROKER
            if self.job_type in {JobType.BROKER_SYNC, JobType.BROKER_RECONCILE}
            else QueueName.STRATEGY_RND
            if self.job_type is JobType.STRATEGY_RND
            else QueueName.GENERAL
        )
        if self.queue is not expected_queue:
            raise ValueError(
                f"{self.job_type.value} handler must use {expected_queue.value}"
            )

    @property
    def priority(self) -> int:
        return JOB_PRIORITIES[self.job_type]


class JobRegistry:
    """Mutable only during composition; duplicate Job types fail closed."""

    def __init__(self, specs: Iterable[JobHandlerSpec] = ()) -> None:
        self._specs: dict[str, JobHandlerSpec] = {}
        for spec in specs:
            self.register(spec)

    def register(self, spec: JobHandlerSpec) -> None:
        key = spec.job_type.value
        if key in self._specs:
            raise ValueError(f"duplicate Job handler: {key}")
        self._specs[key] = spec

    def resolve(self, job_type: str) -> JobHandlerSpec | None:
        return self._specs.get(job_type)

    @property
    def job_types(self) -> frozenset[str]:
        return frozenset(self._specs)


def job_payload_is_safe(payload: dict[str, object]) -> bool:
    """Reject executable/location/provider-control shapes before model parsing."""
    try:
        canonical_json(payload)
    except (TypeError, ValueError):
        return False

    def safe(value: object) -> bool:
        if value is None or isinstance(value, bool | int):
            return True
        if isinstance(value, str):
            return (
                len(value) <= 4_096
                and "://" not in value
                and not value.startswith(("/", "~"))
                and "\x00" not in value
                and _CODE_PREFIX.match(value) is None
            )
        if isinstance(value, list):
            return all(safe(item) for item in value)
        if isinstance(value, dict):
            return all(
                not (_FORBIDDEN_KEY_TOKENS & set(key.lower().split("_")))
                and safe(item)
                for key, item in value.items()
            )
        return False

    return safe(payload)
