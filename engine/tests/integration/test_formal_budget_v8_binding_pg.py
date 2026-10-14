"""Protected-file apply must freeze the explicitly selected formal v8 authority."""

import asyncio
import subprocess
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from integration.test_candidate_multistock_path import AT, FIVE, _set_test_insert_clock
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.agents.portfolio_synthesis_contracts import SYNTHESIS_PROMPT_VERSION
from nexus.ops import agent_paper_loop as loop
from nexus.ops import agent_paper_once as once
from nexus.ops.formal_budget_inventory import configuration
from nexus.ops.formal_budget_protection import apply, bind_runtime_environment, inspect_protection
from nexus.ops.openai_local import prepare_database
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    FormalBudgetBindingRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_agent_paper_loop import service_ready_environment
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", ["v8", "v9", "v10", "missing", "legacy", "legacy_binding"])
async def test_protected_file_freezes_exact_v8_profile(
    migrated_database_url, monkeypatch, tmp_path, selection
):
    persisted_inspect = loop.inspect_agent_paper_loop
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    from nexus.agents.codex_versions import (
        FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
        FULL_INPUT_CAPACITY_CONFIG_VERSION,
    )
    from unit.ops.test_full_input_capacity_registration import V9_OPTIONS

    version = (
        FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION
        if selection == "v10"
        else FULL_INPUT_CAPACITY_CONFIG_VERSION
        if selection == "v9"
        else FORMAL_STAGE_CAPACITY_CONFIG_VERSION
    )
    await prepare_database(
        settings,
        **(
            (V9_OPTIONS | {"formal_flexible_stage_budget": True})
            if selection == "v10"
            else V9_OPTIONS
            if selection == "v9"
            else OPTIONS
        ),
    )
    now = AT - timedelta(hours=1)
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=FIVE[:3], decision_time=AT
    )
    try:
        await _set_test_insert_clock(sessions, now)
        async with sessions.begin() as session:
            assert (await session.scalar(select(func.current_database()))).startswith("nexus_test_")
            next_day = AT + timedelta(days=3)
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=authority.calendar_authority_id,
                    session=next_day.date(),
                    decision_at=next_day.replace(hour=21),
                    market_open_at=next_day.replace(hour=13, minute=30),
                    market_close_at=next_day.replace(hour=20),
                    created_at=now,
                )
            )
            model = await session.scalar(
                select(AgentModelConfigRecord).where(AgentModelConfigRecord.version == version)
            )
            assert model is not None
            legacy = await session.scalar(
                select(AgentModelConfigRecord).where(AgentModelConfigRecord.version != version)
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="protected-v8-batch",
                created_at=now,
            )
            if (
                await session.scalar(
                    select(AgentPromptVersionRecord).where(
                        AgentPromptVersionRecord.role == "portfolio_decision",
                        AgentPromptVersionRecord.version == SYNTHESIS_PROMPT_VERSION,
                    )
                )
                is None
            ):
                content = "Synthetic isolated portfolio prompt."
                session.add(
                    AgentPromptVersionRecord(
                        role="portfolio_decision",
                        version=SYNTHESIS_PROMPT_VERSION,
                        content=content,
                        content_hash=sha256(content.encode()).hexdigest(),
                        created_at=now,
                    )
                )
        original_scalar = AsyncSession.scalar

        async def clock(self, statement, *args, **kwargs):
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return now
            return await original_scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", clock)
        chosen = (
            model.id
            if selection in {"v8", "v9", "v10"}
            else UUID(int=999)
            if selection == "missing"
            else legacy.id
        )
        root = tmp_path / "protected-v8"
        (root / "infra").mkdir(parents=True)
        subprocess.run(["git", "init", "--quiet", str(root)], check=True, capture_output=True)
        (root / ".gitignore").write_text("infra/.env.local\n")
        values = {
            "NEXUS_ENV": "test",
            "AGENT_RUNTIME_MODE": "codex_sdk",
            "DATABASE_URL": migrated_database_url,
            "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED": "true",
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
            "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(authority.account_id),
            "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": str(batch.batch_id),
            "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": str(chosen),
            "AGENT_DAILY_BUDGET_USD": "1",
        }
        values = service_ready_environment() | values
        if selection == "legacy_binding":
            values.pop("NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID")
        path = root / "infra/.env.local"
        path.write_text("\n".join(f"{key}={value}" for key, value in values.items()) + "\n")
        path.chmod(0o600)

        async def verify_host_consumes_frozen_file(pending_settings, expected_model):
            # Real protected-file loading, DB binding and EngineSettings parsing;
            # scheduler inspection/lock are controlled boundaries, with no action.
            loaded, calendar = await loop._load_bound_loop_environment(
                pending_settings, root, {}, env_file=path
            )
            assert calendar is not None
            expected_calendar = UUID(calendar)
            parsed = loop._FileEngineSettings.model_validate(loaded)
            assert parsed.agent_paper_formal_model_config_id == expected_model
            inspected = []

            async def select_calendar(_settings):
                return expected_calendar

            async def inspect_evaluation(_settings, **_kwargs):
                return None

            async def inspect_normal(current, **kwargs):
                inspected.append(current)
                assert current.database_url == migrated_database_url
                assert current.agent_paper_formal_model_config_id == expected_model
                loop.require_formal_host_configuration_match(current, parsed)
                await loop.require_formal_host_trigger_profile(current, now=now)
                assert kwargs["account_id"] == authority.account_id
                assert kwargs["expected_calendar_authority_id"] == expected_calendar
                return loop.AgentPaperLoopSnapshot(now=now, enabled=True)

            async def one_inspection(**kwargs):
                await kwargs["inspect"]()
                return loop.AgentPaperLoopAction.STOP_REQUESTED

            monkeypatch.setattr(loop, "acquire_agent_paper_lock", lambda: nullcontext())
            monkeypatch.setattr(
                loop, "_select_initial_service_calendar_authority_id", select_calendar
            )
            monkeypatch.setattr(loop, "inspect_agent_version_evaluation_loop", inspect_evaluation)
            monkeypatch.setattr(loop, "inspect_agent_paper_loop", inspect_normal)
            monkeypatch.setattr(loop, "run_foreground_loop", one_inspection)
            assert (
                await loop.run_local_agent_paper_loop(
                    pending_settings,
                    account_id=UUID(int=888),
                    repo_root=root,
                    env_file=path,
                    clock=lambda: now,
                )
                == loop.AgentPaperLoopAction.STOP_REQUESTED
            )
            assert len(inspected) == 1

        if selection == "legacy_binding":
            result = await apply(root, idempotency_key="protected-legacy")
            async with sessions() as session:
                original_binding = await session.scalar(select(FormalBudgetBindingRecord))
                frozen_payload = dict(original_binding.payload)
                assert frozen_payload["pricing_version"] != FORMAL_STAGE_CAPACITY_CONFIG_VERSION
            changed_values = values | {"NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": str(model.id)}
            path.write_text(
                "\n".join(f"{key}={value}" for key, value in changed_values.items()) + "\n"
            )
            assert (await inspect_protection(root))["status"] == "configuration_pending_apply"
            pending_settings, *_ = configuration(root)
            pinned, _ = await bind_runtime_environment(pending_settings, changed_values)
            assert "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID" not in pinned
            await verify_host_consumes_frozen_file(pending_settings, None)
            async with sessions() as session:
                binding = await session.scalar(select(FormalBudgetBindingRecord))
                assert binding.content_hash == result["binding_hash"]
                assert binding.payload == frozen_payload
                assert binding.payload["runtime_spec"]["research_job_timeout_seconds"] == 120
                assert (
                    await session.scalar(
                        select(func.count()).select_from(FormalBudgetBindingRecord)
                    )
                    == 1
                )
            return
        if selection not in {"v8", "v9", "v10"}:
            with pytest.raises(ValueError, match="CODEX_FORMAL_AUTHORITY_INVALID"):
                await apply(root, idempotency_key="protected-v8")
            async with sessions() as session:
                assert (
                    await session.scalar(
                        select(func.count()).select_from(FormalBudgetBindingRecord)
                    )
                    == 0
                )
            return
        result = await apply(root, idempotency_key="protected-v8")
        runtime, *_ = configuration(root)
        bound, _calendar = await bind_runtime_environment(runtime, values)
        assert bound["NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID"] == str(model.id)
        # A subsequent file change cannot silently replace the frozen model.
        changed_values = values | {"NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": str(UUID(int=999))}
        path.write_text("\n".join(f"{key}={value}" for key, value in changed_values.items()) + "\n")
        assert (await inspect_protection(root))["status"] == "configuration_pending_apply"
        changed_settings, *_ = configuration(root)
        pinned, _ = await bind_runtime_environment(changed_settings, changed_values)
        assert pinned["NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID"] == str(model.id)
        await verify_host_consumes_frozen_file(changed_settings, model.id)
        with pytest.raises(ValueError, match="CODEX_FORMAL_AUTHORITY_INVALID"):
            await apply(root, idempotency_key="protected-v8-changed")
        # Execute the real host action and normal trigger transaction. Only
        # preparation/provider authority are substituted with seeded evidence.
        now = AT
        await _set_test_insert_clock(sessions, now)
        prepared = []

        async def ready(*_args, **kwargs):
            prepared.append(kwargs["candidate"].instrument_id)
            return "ready"

        async def seeded_authority(_session, _store, current, **kwargs):
            assert current.agent_paper_formal_model_config_id == model.id
            assert kwargs["expected_calendar_authority_id"] == UUID(_calendar)
            assert (
                tuple(item.instrument_id for item in kwargs["identities"]) == batch.instrument_ids
            )
            assert kwargs["process_environment"]["NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID"] == str(
                model.id
            )
            return authority

        @asynccontextmanager
        async def controlled_http(**_kwargs):
            yield object()

        monkeypatch.setattr(loop, "prepare_prior_daily_once", ready)
        monkeypatch.setattr(loop, "prepare_macro_once", ready)
        monkeypatch.setattr(loop, "prepare_current_trends_once", ready)
        monkeypatch.setattr(once, "prepare_schwab_decision_authority", seeded_authority)
        monkeypatch.setattr(once, "S3ProviderEvidenceStore", lambda *_args, **_kwargs: object())
        monkeypatch.setattr(once, "schwab_http_client", controlled_http)
        loaded, calendar = await loop._load_bound_loop_environment(
            changed_settings, root, {}, env_file=path
        )
        active = loop._FileEngineSettings.model_validate(loaded)
        planned = loop.AgentPaperLoopSnapshot(
            now=AT,
            enabled=True,
            calendar_authority_id=UUID(calendar),
            decision_window_start_at=AT - timedelta(hours=3),
            decision_window_end_at=AT + timedelta(hours=6),
            next_decision_window_start_at=AT + timedelta(days=1),
        )
        assert loop.choose_loop_action(planned) == loop.AgentPaperLoopAction.TRIGGER
        assert (
            await loop.execute_agent_paper_loop_action(
                active,
                account_id=UUID(int=888),
                repo_root=root,
                action=loop.choose_loop_action(planned),
                snapshot=planned,
                env_file=path,
            )
            is None
        )
        assert len(prepared) == 9
        from nexus.persistence.models.agents import AgentResearchRunRecord
        from nexus.persistence.models.paper import AgentPaperCycleRecord
        from nexus.persistence.models.runtime import IdempotencyRecord, Job

        async with sessions() as session:
            cycle = await session.scalar(select(AgentPaperCycleRecord))
            frozen_binding = await session.scalar(select(FormalBudgetBindingRecord))
            assert cycle.account_id == authority.account_id
            assert cycle.formal_budget_binding_id == frozen_binding.id
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            jobs = tuple(await session.scalars(select(Job)))
            assert len(runs) == len(jobs) == 3
            assert all(run.model_config_id == model.id for run in runs)
            assert all(job.attempt_count == 0 and job.status == "queued" for job in jobs)
            claims = tuple(
                await session.scalars(
                    select(IdempotencyRecord).where(
                        IdempotencyRecord.formal_budget_binding_id == frozen_binding.id
                    )
                )
            )
            assert len(claims) == 1
            assert claims[0].actor_id == f"local-agent-paper-schwab:{authority.account_id}"
            assert claims[0].route == "agent-paper-schwab-trigger-v2"
            assert claims[0].idempotency_key == AT.isoformat()
            assert claims[0].response_payload["outcome"] == "completed"
            assert set(claims[0].response_payload["result"]["research_run_ids"]) == {
                str(run.id) for run in runs
            }
            assert claims[0].response_payload["result"]["cycle_id"] == str(cycle.id)
            assert claims[0].response_payload["result"]["result_code"] == "RESEARCH_JOBS_PREPARED"
            assert (
                loop.choose_loop_action(replace(planned, cycle_slot_at=AT))
                == loop.AgentPaperLoopAction.WAIT_FOR_SLOT
            )
        now = AT + timedelta(seconds=1)
        observed = await persisted_inspect(
            active, account_id=authority.account_id, expected_calendar_authority_id=UUID(calendar)
        )
        assert observed.cycle_id == cycle.id
        assert observed.cycle_slot_at == AT
        assert observed.cycle_status == "researching"
        assert observed.research_job_id in {job.id for job in jobs}
        assert observed.research_job_status == "queued"
        assert loop.choose_loop_action(observed) == loop.AgentPaperLoopAction.RUN_RESEARCH_JOB
        reread = await persisted_inspect(
            active, account_id=authority.account_id, expected_calendar_authority_id=UUID(calendar)
        )
        assert reread == observed
        assert loop.choose_loop_action(reread) != loop.AgentPaperLoopAction.TRIGGER
        async with sessions() as session:
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 1
            )
            assert await session.scalar(select(func.count()).select_from(Job)) == 3
            assert all(job.attempt_count == 0 for job in await session.scalars(select(Job)))
        async with sessions() as session:
            assert (
                await session.scalar(select(func.count()).select_from(FormalBudgetBindingRecord))
                == 1
            )
            binding = await session.scalar(select(FormalBudgetBindingRecord))
            assert binding is not None
            assert binding.content_hash == result["binding_hash"]
            assert binding.payload["model_id"] == str(model.id)
            assert binding.payload["model_hash"] == model.config_hash
            assert binding.payload["pricing_version"] == version
            assert binding.payload["runtime_spec"]["research_job_timeout_seconds"] == 180
            assert binding.payload["runtime_spec"]["research_handler_timeout_seconds"] == 195
        # Normal applied facts prohibit destructive branch rollback. Alembic
        # must roll back the attempted unmerge too, retaining the merged head.
        monkeypatch.setenv("DATABASE_URL", migrated_database_url)
        migration_config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
        with pytest.raises(Exception, match="FORMAL_BUDGET_BINDING_ROLLBACK_REQUIRES_EMPTY"):
            await asyncio.to_thread(
                command.downgrade,
                migration_config,
                "0067_formal_budget_binding@0066_research_identity_binding",
            )
        from sqlalchemy import text

        async with sessions() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == (
                "0078_formal_v10_output_binding"
            )
            retained = await session.get(FormalBudgetBindingRecord, binding.id)
            assert retained is not None and retained.content_hash == result["binding_hash"]
            assert (
                await session.get(AgentPaperCycleRecord, cycle.id)
            ).formal_budget_binding_id == (binding.id)
    finally:
        await engine.dispose()
