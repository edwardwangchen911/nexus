"""Blocked-path diagnosis: a new Sol target cannot rescue an expired source today."""

from copy import deepcopy
from datetime import timedelta
from hashlib import sha256

import httpx
import pytest
from helpers.disclosure_observations import reader_source
from nexus.agents.platform_native import native_increment_model_allowed
from nexus.agents.platform_profile import register_sol_business_profile
from nexus.monitoring.installation import load_approved_source
from nexus.monitoring.source_qualification import load_qualified_source
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from unit.agents.test_sol_pricing import sol_price_record


@pytest.mark.asyncio
async def test_expired_independent_parent_blocks_source_despite_allowed_current_sol_target(
    migrated_database_url, monkeypatch, tmp_path
):
    """Keep today's expiry guard visible, without endorsing a replacement policy.

    The existing normal independent fixture uses a formal SDK parent. The native
    contract explicitly permits that parent to use a new Sol child. No existing
    model, output, run, price authority, or acceptance receipt is rewritten.
    """

    external_calls = []

    async def forbidden_async(*args, **kwargs):
        external_calls.append("async")
        raise AssertionError("isolated monitor price diagnosis must not send external requests")

    def forbidden_sync(*args, **kwargs):
        external_calls.append("sync")
        raise AssertionError("isolated monitor price diagnosis must not send external requests")

    monkeypatch.setattr(httpx.AsyncClient, "send", forbidden_async)
    monkeypatch.setattr(httpx.Client, "send", forbidden_sync)
    factory, _, _, captured, pointer = await reader_source(
        migrated_database_url, monkeypatch, tmp_path, independent=True
    )
    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", forbidden_async)
    original_time = captured["clock"][0]
    fixture_scalar = AsyncSession.scalar

    async def frozen_clock(session, statement, *args, **kwargs):
        # The existing fixture freezes expression SELECTs; registration uses
        # the equivalent explicit clock query. Keep both on that same clock.
        if str(statement) == "SELECT clock_timestamp()":
            return captured["clock"][0]
        return await fixture_scalar(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", frozen_clock)
    try:
        async with factory.begin() as session:
            source = await load_qualified_source(
                session,
                account_id=pointer.account_id,
                output_id=pointer.source_output_id,
                now=original_time,
            )
            parent = source.run
            old = await session.get(AgentModelConfigRecord, parent.model_config_id)
            assert old is not None
            original = (
                old.config_hash,
                old.pricing_available_at,
                old.pricing_expires_at,
                deepcopy(old.platform_profile),
                parent.model_config_id,
                parent.model_hash,
                parent.pricing_version,
            )
            # Only the isolated database clock advances. Old records stay exact.
            captured["clock"][0] = old.pricing_expires_at
            evidence = b"synthetic refreshed Sol price source for monitor blocked-path diagnosis"
            now = captured["clock"][0]
            new = await register_sol_business_profile(
                session,
                price_record=sol_price_record()
                | {
                    "verified_at": (now - timedelta(seconds=1)).isoformat(),
                    "expires_at": (now + timedelta(days=6)).isoformat(),
                    "source_evidence_sha256": sha256(evidence).hexdigest(),
                },
                source_evidence=evidence,
            )
            assert new.id != old.id
            assert new.pricing_available_at <= now < new.pricing_expires_at
            assert await native_increment_model_allowed(
                session, parent=parent, model_config_id=new.id
            )
            run_count = await session.scalar(
                select(func.count()).select_from(AgentResearchRunRecord)
            )
            for reader in (load_qualified_source, load_approved_source):
                # Cover historical presentation and the current-time caller.
                # This diagnoses source qualification only: this SDK fixture's
                # plans have expired by now, so it is not prepare/admit evidence.
                for source_time in (original_time, now):
                    with pytest.raises(ValueError, match="^MONITOR_SOURCE_INVALID$"):
                        await reader(
                            session,
                            account_id=pointer.account_id,
                            output_id=pointer.source_output_id,
                            now=source_time,
                        )
            assert (
                old.config_hash,
                old.pricing_available_at,
                old.pricing_expires_at,
                old.platform_profile,
                parent.model_config_id,
                parent.model_hash,
                parent.pricing_version,
            ) == original
            assert (
                await session.scalar(select(func.count()).select_from(AgentResearchRunRecord))
                == run_count
            )
            assert external_calls == []
    finally:
        await factory.kw["bind"].dispose()
