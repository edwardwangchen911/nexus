"""Renewal reads immutable real rows and derives one exact configuration target."""

import hashlib
from copy import deepcopy
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from nexus.agents.platform_profile import register_sol_business_profile, sol_business_model_fields
from nexus.agents.platform_schema import COMPACT_PLATFORM_UNITS_VERSION
from nexus.ops.agent_paper_loop import _FileEngineSettings, load_agent_paper_service_environment
from nexus.ops.managed_exit_reconciliation import _price_target
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from unit.agents.test_sol_pricing import sol_price_record


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", [None, "rate", "format", "missing", "env_drift", "duplicate_key", "settings_drift"]
)
async def test_real_price_target_is_read_only_and_exact(migrated_database_url, tmp_path, fault):
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    evidence = b"isolated renewal source fixture"
    try:
        async with factory.begin() as session:
            now = await session.scalar(text("SELECT clock_timestamp()"))
            old_price = sol_price_record() | {
                "verified_at": (now - timedelta(days=8)).isoformat(),
                "expires_at": (now - timedelta(days=1)).isoformat(),
            }
            old = AgentModelConfigRecord(
                id=uuid4(),
                **sol_business_model_fields(
                    old_price, units_version=COMPACT_PLATFORM_UNITS_VERSION
                ),
            )
            session.add(old)
            original_profile = deepcopy(old.platform_profile)
            new_price = old_price | {
                "verified_at": (now - timedelta(minutes=1)).isoformat(),
                "expires_at": (now + timedelta(days=6)).isoformat(),
                "source_evidence_sha256": hashlib.sha256(evidence).hexdigest(),
            }
            if fault == "rate":
                new_price["cache_read_usd_per_million"] = "0.2"
            kwargs = {} if fault == "format" else {"units_version": COMPACT_PLATFORM_UNITS_VERSION}
            new = await register_sol_business_profile(
                session, price_record=new_price, source_evidence=evidence, **kwargs
            )
            old_id, new_id = old.id, new.id
        settings = settings.model_copy(
            update={
                "agent_paper_formal_model_config_id": old_id,
                "agent_paper_account_id": UUID(int=1),
                "agent_paper_candidate_batch_id": UUID(int=2),
            }
        )
        source = (
            f"NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID={old_id}\nPRIVATE_CANARY=must-not-appear\n"
            f"DATABASE_URL={migrated_database_url}\n"
            f"NEXUS_AGENT_PAPER_ACCOUNT_ID={UUID(int=1)}\n"
            f"NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID={UUID(int=2)}\n"
        ).encode()
        if fault == "duplicate_key":
            source += f"NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID={new_id}\n".encode()
        env = tmp_path / "source.env"
        env.write_bytes(source + (b"NEW_PERMISSION=true\n" if fault == "env_drift" else b""))
        before = env.read_bytes()
        capture = {
            "producer": {"config_hash": hashlib.sha256(source).hexdigest()},
            "reconciliation_revision": "a" * 40,
        }
        if fault == "settings_drift":
            settings = settings.model_copy(update={"agent_paper_formal_model_config_id": uuid4()})
        if fault:
            with pytest.raises(ValueError, match="AGENT_PAPER_EXIT_RECONCILIATION_"):
                await _price_target(
                    settings, source, env, uuid4() if fault == "missing" else new_id, capture
                )
        else:
            target_settings, proof = await _price_target(settings, source, env, new_id, capture)
            assert target_settings.model_dump() == settings.model_dump() | {
                "agent_paper_formal_model_config_id": new_id
            }
            expected = source.replace(str(old_id).encode(), str(new_id).encode())
            actual_target_file = tmp_path / "target.env"
            actual_target_file.write_bytes(expected)
            loaded = _FileEngineSettings.model_validate(
                load_agent_paper_service_environment(tmp_path, {}, env_file=actual_target_file)
            )
            assert [
                name
                for name, value in target_settings.model_dump().items()
                if getattr(loaded, name) != value
            ] == []
            assert proof["config_hash"] == hashlib.sha256(expected).hexdigest()
            assert proof["old_model_id"] == str(old_id) and proof["new_model_id"] == str(new_id)
            assert "PRIVATE_CANARY" not in repr(proof)
        assert env.read_bytes() == before
        async with factory() as session:
            rows = list(
                await session.scalars(
                    select(AgentModelConfigRecord).where(
                        AgentModelConfigRecord.id.in_([old_id, new_id])
                    )
                )
            )
            assert len(rows) == 2
            original = next(row for row in rows if row.id == old_id)
            assert original.pricing_expires_at == now - timedelta(days=1)
            assert original.platform_profile == original_profile
    finally:
        await factory.kw["bind"].dispose()
