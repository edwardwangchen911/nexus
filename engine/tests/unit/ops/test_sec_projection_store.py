"""The local projection producer reads loopback originals without environment proxies."""

from uuid import UUID

import pytest
from nexus.ops import sec_local
from nexus.settings import EngineSettings


@pytest.mark.asyncio
async def test_local_projection_store_disables_environment_proxy(monkeypatch):
    captured = []

    class Constructed(Exception):
        pass

    def store(settings, **options):
        captured.append(options)
        raise Constructed

    monkeypatch.setattr(sec_local, "S3ProviderEvidenceStore", store)
    settings = EngineSettings(
        database_url="postgresql+asyncpg://fixture:fixture@127.0.0.1:56039/nexus_test_store",
        object_store_endpoint="http://127.0.0.1:56049",
    )
    with pytest.raises(Constructed):
        await sec_local.project_saved_sec_facts(settings, UUID(int=1))
    assert captured == [{"disable_environment_proxies": True}]
