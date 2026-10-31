"""A failed stage-evidence read is never a deterministic price refusal."""

from datetime import timedelta

import pytest
from integration.test_comparison_closeout_pg import arrange, facts
from nexus.agents.comparison_closeout import close_expired_comparison_position
from nexus.agents.research_stages import protocol as stage_protocol
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


async def original_facts(factory):
    snapshot = await facts(factory)
    async with factory() as session:
        for table in ("agent_research_outputs", "agent_research_events"):
            snapshot[table] = (
                (await session.execute(text(f"SELECT to_jsonb(t) FROM {table} t"))).scalars().all()
            )
    return snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize("orphan", [False, True], ids=["normal-close", "public-orphan-close"])
async def test_settlement_stage_read_value_error_propagates_and_rolls_back_every_fact(
    migrated_database_url, monkeypatch, tmp_path, orphan
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    observer = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        result, manifest, claim, protocol, _, old = await arrange(
            factory, monkeypatch, tmp_path, known=True
        )
        before = await original_facts(observer)
        assert before["research_stage_scope"][0]["stopped"] is False
        assert before["research_comparison_chain_stop"] == []
        read_stage_evidence, calls = stage_protocol.stage_evidence, []
        injected = ValueError("CONTROLLED_STAGE_EVIDENCE_READ_FAILED")

        async def fail_settlement_read(session, run_id, *args, **kwargs):
            calls.append(run_id)
            if len(calls) == 2:
                raise injected
            return await read_stage_evidence(session, run_id, *args, **kwargs)

        monkeypatch.setattr(stage_protocol, "stage_evidence", fail_settlement_read)
        with pytest.raises(ValueError) as error:
            if orphan:
                await close_expired_comparison_position(factory, manifest.experiment_id, 1)
            else:
                await protocol.close(claim, now=claim.expires_at - timedelta(seconds=329))
        assert error.value is injected
        assert calls == [result.run_id, result.run_id]
        after = await original_facts(observer)
        assert after == before
        assert after["research_stage_terminal"] == []
        assert after["research_comparison_chain_stop"] == []
        assert len(after["job_attempts"]) == 1 and after["job_attempts"][0]["outcome"] == "running"
        budgets = {row["id"]: row for row in after["agent_daily_budget_reservations"]}
        assert budgets[str(old)]["state"] == "reserved"
        current = next(row for row in budgets.values() if row["run_id"] == str(result.run_id))
        assert current["state"] == "reserved" and current["settled_usd"] is None
    finally:
        await factory.kw["bind"].dispose()
        await observer.kw["bind"].dispose()
