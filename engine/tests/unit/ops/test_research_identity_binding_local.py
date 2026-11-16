"""The public identity CLI must not need a model secret for local material registration."""

import json
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.ops import research_identity_binding_local as local


@pytest.mark.parametrize("ambient_key", [None, "synthetic-model-key-never-used"])
def test_identity_registration_cli_uses_keyless_local_data_configuration(
    tmp_path, monkeypatch, capsys, ambient_key
):
    (tmp_path / "infra").mkdir()
    (tmp_path / "infra/.env.local").write_text(
        "\n".join(
            [
                "DATABASE_URL=postgresql+asyncpg://fixture:fixture@127.0.0.1:56039/nexus_test_identity",
                "AGENT_RUNTIME_MODE=responses",
                "OPENAI_EXTRACTION_MODEL=gpt-6.1-sol",
                "OPENAI_REASONING_MODEL=gpt-6.1-sol",
            ]
        )
    )
    if ambient_key is None:
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    else:
        monkeypatch.setenv("OPENAI_API_KEY", ambient_key)
    action = AsyncMock(return_value={"status": "registered", "execution_locked": True})
    monkeypatch.setattr(local, "register_identity", action)
    monkeypatch.setattr(
        "sys.argv",
        [
            "identity",
            "--repo-root",
            str(tmp_path),
            "--identity-snapshot-id",
            str(UUID(int=1)),
            "--collection-dir",
            str(tmp_path / "archive"),
        ],
    )
    local.main()
    assert json.loads(capsys.readouterr().out)["status"] == "registered"
    action.assert_awaited_once()
    settings = action.await_args.args[0]
    assert settings.openai_api_key is None
    import os

    assert os.environ.get("OPENAI_API_KEY") == ambient_key
    assert settings.provider_jobs_enabled is False
    assert settings.database_url.endswith("/nexus_test_identity")
    assert action.await_args.kwargs["identity_snapshot_id"] == UUID(int=1)


def test_sec_index_inspection_cli_uses_keyless_local_data_configuration(
    tmp_path, monkeypatch, capsys
):
    from nexus.ops import sec_index_local

    (tmp_path / "infra").mkdir()
    (tmp_path / "infra/.env.local").write_text(
        "DATABASE_URL=postgresql+asyncpg://fixture:fixture@127.0.0.1:56039/nexus_test_identity\nAGENT_RUNTIME_MODE=responses\nOPENAI_EXTRACTION_MODEL=gpt-6.1-sol\nOPENAI_REASONING_MODEL=gpt-6.1-sol\n"
    )
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    action = AsyncMock(return_value={"status": "verified", "execution_locked": True})
    monkeypatch.setattr(sec_index_local, "inspect_index", action)
    monkeypatch.setattr(
        "sys.argv",
        ["sec-index", "--repo-root", str(tmp_path), "inspect", "--snapshot-id", str(UUID(int=1))],
    )
    sec_index_local.main()
    assert json.loads(capsys.readouterr().out)["status"] == "verified"
    action.assert_awaited_once()
    assert action.await_args.args[0].openai_api_key is None


def test_local_sec_index_storage_does_not_use_environment_proxy(monkeypatch):
    from nexus.ops import sec_index_local
    from nexus.settings import EngineSettings

    captured = []
    sentinel = object()

    def factory(settings, **options):
        captured.append(options)
        return sentinel

    monkeypatch.setattr(sec_index_local, "S3ProviderEvidenceStore", factory)
    settings = EngineSettings(
        database_url="postgresql+asyncpg://fixture:fixture@127.0.0.1:56039/nexus_test_identity",
        object_store_endpoint="http://127.0.0.1:56049",
    )
    assert sec_index_local._store(settings, None) is sentinel
    assert captured == [{"disable_environment_proxies": True}]
