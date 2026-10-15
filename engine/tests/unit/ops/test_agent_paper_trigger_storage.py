"""Native trigger sessions must retain the configuration needed to verify old evidence."""

from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.ops import agent_paper_once as cli
from nexus.paper.agent_cycle_models import AgentPaperCycleResult, AgentPaperTriggerReservation
from nexus.research import sec_exhibit_storage as storage
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome", ["replay", "created", "provider_failure", "corrupt_original", "resume"]
)
async def test_native_trigger_verifies_archived_evidence_in_its_own_sessions(
    monkeypatch, tmp_path, outcome
):
    now = datetime(2026, 10, 15, 16, tzinfo=UTC)
    original = b"synthetic archived SEC original"
    digest = sha256(original).hexdigest()
    uri = f"s3://test-evidence/raw/sec/{digest}"
    artifact = SimpleNamespace(
        source="sec",
        schema_version="sec_exhibit_0_raw_v2",
        received_at=now,
        available_at=now,
        observed_at=now,
        created_at=now,
        uri=uri,
        byte_size=len(original),
    )
    settings = cli.AgentPaperTriggerSettings(
        database_url="postgresql+asyncpg://fixture:fixture@127.0.0.1:1/never_connect",
        object_store_endpoint="http://127.0.0.1:1",
        alpaca_key=SecretStr("fixture"),
        alpaca_secret=SecretStr("fixture"),
    )
    reads = []
    phases = []

    class Store:
        def __init__(self, supplied, **_kwargs):
            assert supplied is settings

        async def read_raw(self, key, *, uri):
            reads.append(key)
            assert uri == artifact.uri
            return b"x" * len(original) if outcome == "corrupt_original" else original

    async def scalar(self, statement, *_args, **_kwargs):
        if "clock_timestamp()" in str(statement):
            return now
        return artifact

    async def verify(session, phase):
        phases.append(phase)
        assert (
            await storage._read_original(
                session,
                storage.configured_exhibit_store(session),
                digest=digest,
                schema=artifact.schema_version,
                received_at=now,
                as_of=now,
            )
            == original
        )

    result = AgentPaperCycleResult(
        cycle_id=UUID(int=9),
        status="researching",
        result_code="RESEARCH_JOBS_PREPARED",
        replayed=outcome == "replay",
    )
    reservation = AgentPaperTriggerReservation(
        claim_id=UUID(int=8),
        decision_session=now.date(),
        decision_slot_at=now,
        replay=result if outcome == "replay" else None,
    )

    async def reserve(session, **_kwargs):
        await verify(session, "reservation")
        return reservation

    async def trigger(session, *_args, **_kwargs):
        await verify(session, "provider")
        if outcome == "provider_failure":
            raise ValueError("SCHWAB_L1_INCOMPLETE")
        return result

    async def finish(session, **kwargs):
        await verify(session, "failure_receipt" if "error_code" in kwargs else "receipt")

    async def resume(session, **_kwargs):
        await verify(session, "resume")
        return result

    # Keep the real engine, sessionmaker and transaction lifecycle. Only database
    # rows and object-store transport are synthetic; no database/network is used.
    monkeypatch.setattr(AsyncSession, "scalar", scalar)
    monkeypatch.setattr(storage, "S3ProviderEvidenceStore", Store)
    monkeypatch.setattr(cli, "S3ProviderEvidenceStore", Store)
    monkeypatch.setattr(cli, "reserve_schwab_agent_paper_trigger", reserve)
    monkeypatch.setattr(cli, "trigger_in_session", trigger)
    monkeypatch.setattr(cli, "finish_schwab_agent_paper_trigger", finish)
    monkeypatch.setattr(cli, "resume_schwab_agent_paper_cycle", resume)
    request = cli.AgentPaperTriggerRequest(
        action="trigger",
        account_id=UUID(int=1),
        candidate_batch_id=UUID(int=2),
        idempotency_key="archived-evidence-config",
    )
    if outcome == "resume":
        assert (
            await cli.resume_once(
                settings,
                cli.AgentPaperResumeRequest(
                    action="resume",
                    cycle_id=result.cycle_id,
                    idempotency_key="resume-archived-evidence",
                    formal_stage_capacity=True,
                ),
            )
            == result
        )
    elif outcome in {"provider_failure", "corrupt_original"}:
        expected = (
            "SEC_EXHIBIT_ARTIFACT_INVALID"
            if outcome == "corrupt_original"
            else "SCHWAB_L1_INCOMPLETE"
        )
        with pytest.raises(ValueError, match=expected):
            await cli.trigger_once(settings, request, repo_root=Path(tmp_path))
    else:
        assert await cli.trigger_once(settings, request, repo_root=Path(tmp_path)) == result
    assert (
        phases
        == {
            "replay": ["reservation"],
            "created": ["reservation", "provider", "receipt"],
            "provider_failure": ["reservation", "provider", "failure_receipt"],
            "corrupt_original": ["reservation"],
            "resume": ["resume"],
        }[outcome]
    )
    assert reads == [f"raw/sec/{digest}"] * len(phases)
