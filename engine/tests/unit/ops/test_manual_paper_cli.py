import json
from types import SimpleNamespace

import pytest
from nexus.ops import manual_paper


@pytest.mark.parametrize("file_revision", [None, "1" * 40])
def test_normal_cli_binds_host_version_to_requested_repo_head(
    tmp_path, monkeypatch, capsys, file_revision
):
    calls = []
    observed = []
    revision = "2" * 40
    environment = {} if file_revision is None else {"NEXUS_RELEASE_REVISION": file_revision}
    monkeypatch.setattr(manual_paper, "_validate_local_env", lambda root, path: path)
    monkeypatch.setattr(
        manual_paper, "load_agent_paper_service_environment", lambda *a, **k: environment.copy()
    )

    def checkout(root):
        calls.append(root)
        return revision

    monkeypatch.setattr(manual_paper, "_checkout_revision", checkout, raising=False)

    async def run(settings, args):
        observed.append(settings.release_revision)
        return SimpleNamespace(model_dump_json=lambda: '{"status":"fixture"}')

    monkeypatch.setattr(manual_paper, "run", run)
    assert (
        manual_paper.main(
            [
                "--repo-root",
                str(tmp_path),
                "start",
                "--mode",
                "research_and_paper",
                "--idempotency-key",
                "fixture",
            ]
        )
        == 0
    )
    assert calls == [tmp_path.resolve()]
    assert observed == [revision]
    assert json.loads(capsys.readouterr().out) == {"status": "fixture"}


def test_missing_checkout_revision_does_not_trust_file_but_keeps_receipt_read(
    tmp_path, monkeypatch
):
    observed = []
    monkeypatch.setattr(manual_paper, "_validate_local_env", lambda root, path: path)
    monkeypatch.setattr(
        manual_paper,
        "load_agent_paper_service_environment",
        lambda *a, **k: {"NEXUS_RELEASE_REVISION": "1" * 40},
    )
    monkeypatch.setattr(manual_paper, "_checkout_revision", lambda root: None, raising=False)

    async def run(settings, args):
        observed.append(settings.release_revision)
        assert args.action == "read"
        return SimpleNamespace(model_dump_json=lambda: '{"status":"fixture"}')

    monkeypatch.setattr(manual_paper, "run", run)
    assert (
        manual_paper.main(
            [
                "--repo-root",
                str(tmp_path),
                "read",
                "--request-id",
                "00000000-0000-0000-0000-000000000001",
            ]
        )
        == 0
    )
    assert observed == [None]
