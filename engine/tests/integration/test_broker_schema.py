import pytest
from nexus.persistence.models.portfolio import Account, AccountExternalIdEnvelope
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


def test_account_and_external_id_envelope_constraints_are_declared() -> None:
    assert Account.__table__.name == "accounts"
    constraints = {
        constraint.name for constraint in AccountExternalIdEnvelope.__table__.constraints
    }
    assert "ck_account_external_id_envelopes_aad_version_allowed" in constraints
    assert "uq_account_external_id_envelopes_version" in constraints


@pytest.mark.asyncio
async def test_account_schema_migration_is_runnable(migrated_database_url: str) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        tables = set(
            await session.scalars(
                text(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                    "AND tablename IN ('accounts', 'account_external_id_envelopes')"
                )
            )
        )
    assert tables == {"accounts", "account_external_id_envelopes"}
