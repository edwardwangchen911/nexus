"""Admit one explicitly selected existing SDK model; never invoke it here."""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.ops.daily_once import _require_research_model
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings


class LocalResearchAdmission(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    request: ResearchRequest
    model_config_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255)


async def admit_local_research(settings: EngineSettings, path: Path) -> dict[str, object]:
    if (
        not settings.database_url
        or make_url(settings.database_url).host not in {"127.0.0.1", "localhost", "::1"}
        or settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK
    ):
        raise ValueError("CODEX_LOCAL_ADMISSION_REQUIRED")
    with path.open("rb") as stream:
        raw = stream.read(65537)
    if len(raw) > 65536:
        raise ValueError("CODEX_ADMISSION_REQUEST_TOO_LARGE")
    payload = LocalResearchAdmission.model_validate_json(raw)
    digest = hashlib.sha256(
        json.dumps(payload.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await _require_research_model(session, settings, payload.model_config_id)
            now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(now, datetime)
            result = await admit_research_run(
                session,
                actor_id="local-research-cli",
                route="nexus-openai-local/admit-research",
                idempotency_key=payload.idempotency_key,
                request_hash=digest,
                request=payload.request,
                prompt_version_id=None,
                model_config_id=payload.model_config_id,
                daily_limit_usd=settings.agent_daily_budget_usd,
                now=now,
            )
            return {
                "status": result.run.status,
                "run_id": str(result.run.id),
                "replayed": result.replayed,
                "model_requests": 0,
                "provider_requests": 0,
                "execution_locked": True,
            }
    finally:
        await factory.kw["bind"].dispose()
