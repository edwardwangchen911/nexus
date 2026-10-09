import asyncio
from uuid import uuid4

import pytest
from nexus.ops.research_budget_diagnostic import require_isolated_case
from nexus.persistence.models.portfolio import Account
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.fixture
def diagnostic_database_url(temporary_database_factory, migrated_database_template):
    name = f"nexus_diag_{uuid4().hex}"
    admin_url = temporary_database_factory.admin_url

    async def change_database(create: bool) -> None:
        engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                if create:
                    await connection.execute(
                        text(
                            f'CREATE DATABASE "{name}" TEMPLATE "{migrated_database_template.name}"'
                        )
                    )
                else:
                    await connection.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        finally:
            await engine.dispose()

    try:
        asyncio.run(change_database(True))
        url = admin_url.set(database=name).render_as_string(hide_password=False)

        async def remove_migration_seed() -> None:
            engine = create_async_engine(url)
            try:
                async with engine.begin() as connection:
                    # Only this owned ephemeral clone: migrations seed an internal Paper account.
                    await connection.execute(text("TRUNCATE accounts CASCADE"))
            finally:
                await engine.dispose()

        asyncio.run(remove_migration_seed())
        yield url
    finally:
        asyncio.run(change_database(False))


@pytest.mark.asyncio
async def test_actual_database_identity_is_checked_before_case_reads(migrated_database_url: str):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as session:
            # A local, migrated test database is still not the dedicated diagnostic database.
            with pytest.raises(ValueError, match="DIAGNOSTIC_DATABASE_REQUIRED"):
                await require_isolated_case(session, uuid4())
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_real_account_is_rejected_in_actual_diagnostic_database(diagnostic_database_url):
    factory = session_factory(EngineSettings(database_url=diagnostic_database_url))
    try:
        async with factory() as session, session.begin():
            session.add(Account(provider="fosun", status="ACTIVE"))
        async with factory() as session:
            with pytest.raises(ValueError, match="DIAGNOSTIC_SYNTHETIC_ACCOUNTS_REQUIRED"):
                await require_isolated_case(session, uuid4())
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_synthetic_account_does_not_substitute_for_fresh_case(diagnostic_database_url):
    factory = session_factory(EngineSettings(database_url=diagnostic_database_url))
    try:
        async with factory() as session, session.begin():
            session.add(Account(provider="diagnostic_synthetic", status="ACTIVE"))
        async with factory() as session:
            with pytest.raises(ValueError, match="DIAGNOSTIC_SINGLE_CASE_REQUIRED"):
                await require_isolated_case(session, uuid4())
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("candidate", [False, True])
async def test_actual_target_mismatch_precedes_marker_and_worker(
    diagnostic_database_url,
    tmp_path,
    monkeypatch,
    candidate,
):
    from unittest.mock import AsyncMock

    from nexus import worker
    from nexus.ops import research_budget_diagnostic as module
    from sqlalchemy.engine import make_url

    factory = session_factory(EngineSettings(database_url=diagnostic_database_url))
    claimed = make_url(diagnostic_database_url).set(database="nexus_diag_other")
    settings = EngineSettings(
        database_url=claimed.render_as_string(hide_password=False),
        agent_runtime_mode="codex_sdk",
    )
    call = AsyncMock()
    monkeypatch.setattr(module, "session_factory", lambda _: factory)
    monkeypatch.setattr(worker, "run_agent_worker_once", call)
    tmp_path.chmod(0o700)
    output = tmp_path / "attempt"
    with pytest.raises(ValueError, match="DIAGNOSTIC_DATABASE_REQUIRED"):
        await module.execute_diagnostic(
            settings,
            run_id=uuid4(),
            output=output,
            candidate_stage_capacity=candidate,
        )
    assert not output.exists()
    call.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_setup_rejects_real_account_before_registration(diagnostic_database_url):
    from integration.test_candidate_research_authority import OPTIONS
    from nexus.ops.openai_local import prepare_database
    from nexus.persistence.models.agents import AgentModelConfigRecord, AgentPromptVersionRecord
    from sqlalchemy import func, select

    settings = EngineSettings(database_url=diagnostic_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            session.add(Account(provider="fosun", status="ACTIVE"))
        with pytest.raises(ValueError, match="DIAGNOSTIC_SYNTHETIC_ACCOUNTS_REQUIRED"):
            await prepare_database(settings, **OPTIONS)
        async with factory() as session:
            for model in (AgentModelConfigRecord, AgentPromptVersionRecord):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("model_fails", [False, True])
async def test_candidate_normal_cli_register_admit_execute_and_refuse_replay(
    diagnostic_database_url,
    tmp_path,
    monkeypatch,
    capsys,
    model_fails,
):
    import json
    from pathlib import Path
    from unittest.mock import AsyncMock
    from uuid import UUID

    from integration.test_agent_execution_pg import (
        _INSTRUMENT_ID,
        _NOW,
        _SNAPSHOT_ID,
        _seed_authority,
        _seed_database_tool_authority,
    )
    from nexus.agents import codex_staged
    from nexus.agents.codex_versions import CANDIDATE_BUDGET_CONFIG_VERSION
    from nexus.agents.responses_budget import UsageSnapshot
    from nexus.ops import openai_local, research_budget_diagnostic
    from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
    from nexus.persistence.models.runtime import Job
    from sqlalchemy import select

    # The CLI's configured default deliberately points elsewhere; only its protected
    # diagnostic URL file may select this owned, migrated synthetic database.
    default = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus",
        agent_runtime_mode="codex_sdk",
    )
    settings = default.model_copy(update={"database_url": diagnostic_database_url})
    factory = session_factory(settings)
    monkeypatch.chdir(Path(__file__).resolve().parents[3])
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: default)
    monkeypatch.setattr(openai_local, "status", lambda _: {"execution_locked": True})

    async def invoke_stage(payload, **kwargs):
        assert kwargs["timeout_seconds"] > 0
        if model_fails:
            raise TimeoutError("private-sdk-canary")
        stage = payload["stage"]
        reviewed = (
            []
            if stage == "research"
            else ["bull:0"]
            if stage == "critique"
            else [
                "bear:0",
                "evidence_audit:0",
            ]
        )
        if stage == "synthesis":
            response = {
                "synthesis_assessment": {
                    "stance": "watch",
                    "claims": [{"text": "合成证据仅供研究；行情不完整。", "citations": [0]}],
                    "reviews": [
                        {
                            "claim_id": claim,
                            "status": "accepted",
                            "rationale": "行情缺口。",
                            "citations": [0],
                            "resolved": False,
                            "missing_evidence": ["合格行情"],
                            "prevented_conclusion": "不能比较。",
                        }
                        for claim in reviewed
                    ],
                    "review_conclusion": "insufficient",
                    "uncertainties": ["行情缺失。"],
                    "conflicting_facts": False,
                }
            }
        else:
            response = {
                "roles": [
                    {
                        "role": prompt["role"],
                        "vote": "abstain",
                        "claims": [{"text": "冻结合成证据。", "citations": [0]}],
                        "conflicting_facts": False,
                        "reviewed_claim_ids": reviewed,
                    }
                    for prompt in payload["prompts"]
                ]
            }
        return json.dumps(response), UsageSnapshot(input_tokens=300, output_tokens=80)

    invoke = AsyncMock(side_effect=invoke_stage)
    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    tmp_path.chmod(0o700)
    config = tmp_path / "database-url"
    config.write_text(diagnostic_database_url)
    config.chmod(0o600)
    try:
        await _seed_authority(factory, include_base_evidence=False)
        await _seed_database_tool_authority(
            factory,
            link_price=False,
            bar_count=1,
            price_quality_flags=("missing",),
        )
        monkeypatch.setattr(
            "sys.argv",
            [
                "nexus-openai-local",
                "setup",
                "--skip-key",
                "--structured-synthesis",
                "--aggregate-input-tokens",
                "96000",
                "--max-wire-bytes",
                "64000",
                "--inline-citations",
                "--actionable-paper",
                "--stage-time-budget",
                "--candidate-stage-capacity",
                "--database-url-file",
                str(config),
            ],
        )
        assert await asyncio.to_thread(openai_local.main) == 0
        capsys.readouterr()
        async with factory() as session:
            model_id = await session.scalar(
                select(AgentModelConfigRecord.id).where(
                    AgentModelConfigRecord.version == CANDIDATE_BUDGET_CONFIG_VERSION,
                )
            )
        assert model_id is not None
        request = tmp_path / "request.json"
        request.write_text(
            json.dumps(
                {
                    "request": {
                        "instrument_id": str(_INSTRUMENT_ID),
                        "data_snapshot_id": str(_SNAPSHOT_ID),
                        "decision_time": _NOW.isoformat(),
                    },
                    "model_config_id": str(model_id),
                    "idempotency_key": "budget-diagnostic:candidate-cli",
                }
            )
        )
        monkeypatch.setattr(
            "sys.argv",
            [
                "nexus-openai-local",
                "admit-research",
                "--research-request",
                str(request),
                "--candidate-stage-capacity",
                "--database-url-file",
                str(config),
            ],
        )
        assert await asyncio.to_thread(openai_local.main) == 0
        admission = json.loads(capsys.readouterr().out)
        assert admission["model_requests"] == 0 and admission["provider_requests"] == 0
        invoke.assert_not_awaited()
        output = tmp_path / "attempt"
        monkeypatch.setattr(
            "sys.argv",
            [
                "nexus-engine-research-budget-diagnostic",
                "--repo-root",
                str(Path.cwd()),
                "--database-url-file",
                str(config),
                "--run-id",
                admission["run_id"],
                "--output-dir",
                str(output),
                "--candidate-stage-capacity",
            ],
        )
        assert await asyncio.to_thread(research_budget_diagnostic.main) == 0
        result = json.loads(capsys.readouterr().out)
        assert result["status"] == "observed" and result["attempt_count"] == 1
        assert result["run_status"] == ("failed" if model_fails else "completed")
        assert result["job_status"] == ("failed" if model_fails else "succeeded")
        assert not result["formal_acceptance"] and result["execution_locked"]
        assert result["candidate_stage_capacity"] is True
        if model_fails:
            assert result["failure_diagnostic"]["stage"] == "research"
        else:
            assert result["failure_diagnostic"] is None
            assert set(result["stage_timings"]) == {"research", "critique", "synthesis"}
            # Eight roles plus synthesis and validation are separate persisted outputs.
            assert result["output_count"] == 10
        expected_calls = 1 if model_fails else 3
        assert invoke.await_count == expected_calls
        assert (output / "started.json").is_file() and (output / "result.json").is_file()
        # Neither the same receipt directory nor a new output name can replay this Job.
        for path in (output, tmp_path / "another-attempt"):
            monkeypatch.setattr(
                "sys.argv",
                [
                    "nexus-engine-research-budget-diagnostic",
                    "--repo-root",
                    str(Path.cwd()),
                    "--database-url-file",
                    str(config),
                    "--run-id",
                    admission["run_id"],
                    "--output-dir",
                    str(path),
                    "--candidate-stage-capacity",
                ],
            )
            assert await asyncio.to_thread(research_budget_diagnostic.main) == 1
            stopped = capsys.readouterr().out
            assert "DIAGNOSTIC_FRESH_RUN_REQUIRED" in stopped
            assert "private-sdk-canary" not in stopped and diagnostic_database_url not in stopped
        assert invoke.await_count == expected_calls and not (tmp_path / "another-attempt").exists()
        async with factory() as session:
            run = await session.get(AgentResearchRunRecord, UUID(admission["run_id"]))
            job = await session.scalar(select(Job))
            assert run.retry_count == 0 and job.attempt_count == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("synthetic", [False, True])
async def test_normal_diagnostic_entry_rejects_real_accounts_before_marker_or_worker(
    diagnostic_database_url, tmp_path, monkeypatch, synthetic
):
    from unittest.mock import AsyncMock

    from nexus import worker
    from nexus.ops.research_budget_diagnostic import execute_diagnostic

    settings = EngineSettings(database_url=diagnostic_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    call = AsyncMock()
    monkeypatch.setattr(worker, "run_agent_worker_once", call)
    tmp_path.chmod(0o700)
    output = tmp_path / "attempt"
    try:
        async with factory.begin() as session:
            session.add(Account(provider="fosun", status="ACTIVE"))
        with pytest.raises(ValueError, match="DIAGNOSTIC_SYNTHETIC_ACCOUNTS_REQUIRED"):
            await execute_diagnostic(
                settings, run_id=uuid4(), output=output, synthetic_paper_load=synthetic
            )
        assert not output.exists()
        call.assert_not_awaited()
    finally:
        await factory.kw["bind"].dispose()
