"""An operator receipt has one exact purpose, never general research authority."""

from hashlib import sha256

import pytest
from nexus.monitoring import filing_adoption as adoption


def sample(monkeypatch):
    material = {"issuer_cik": "0001045810", "entry": {"accession_number": "0001045810-26-000088"}}
    report = "Independent review of the exact image: decorative header branding only."
    monkeypatch.setattr(
        adoption, "MATERIAL_SHA256", sha256(adoption.canonical(material)).hexdigest()
    )
    monkeypatch.setattr(adoption, "REPORT_SHA256", sha256(report.encode()).hexdigest())
    monkeypatch.setattr(
        adoption, "OWNER_SHA256", sha256("仅允许上述本次限定采纳".encode()).hexdigest()
    )
    return dict(
        material=material,
        report_text=report,
        report_source="chatgpt:independent-reply-1234",
        owner_message_text="仅允许上述本次限定采纳",
        owner_message_source="codex:actual-user-reply-1234",
        purpose="nvda_000088_header_graphic_only",
        graphic_filename="nvdalogoa19.jpg",
        graphic_sha256=adoption.GRAPHIC_SHA256,
        research_qualified=False,
    )


def test_exact_designation_is_bounded(monkeypatch):
    value = sample(monkeypatch)
    result = adoption.validate_designation(value)
    assert result.research_qualified is False
    for key, replacement in (
        ("purpose", "all_logos"),
        ("graphic_filename", "another.jpg"),
        ("graphic_sha256", "0" * 64),
        ("research_qualified", True),
        ("owner_message_text", ""),
        ("owner_message_source", ""),
        ("report_text", value["report_text"] + " changed"),
    ):
        with pytest.raises(ValueError):
            adoption.validate_designation({**value, key: replacement})
    value["material"]["entry"]["accession_number"] = "0001045810-26-000089"
    with pytest.raises(ValueError):
        adoption.validate_designation(value)


def test_normal_monitor_cannot_designate():
    from nexus.ops.research_monitor import parser

    for command in ("register-graphic-adoption", "adopt-content", "designate-reviewer"):
        with pytest.raises(SystemExit):
            parser().parse_args([command])


@pytest.mark.asyncio
async def test_operator_requires_configured_account_before_access(monkeypatch, tmp_path):
    from nexus.ops import sec_graphic_adoption_local as local
    from nexus.settings import EngineSettings

    def forbidden(*args):
        raise AssertionError("unconfigured adoption accessed storage")

    monkeypatch.setattr(local, "session_factory", forbidden)
    args = local.parser().parse_args(
        ["register", "--designation", str(tmp_path / "missing.json"), "--operator-confirmed"]
    )
    with pytest.raises(ValueError, match="MONITOR_ACCOUNT_UNCONFIGURED"):
        await local.run(EngineSettings(), args)
    with pytest.raises(SystemExit):
        local.parser().parse_args(["register", "--designation", str(tmp_path / "missing.json")])


def test_database_error_never_claims_rollback_or_prints_response(monkeypatch, tmp_path, capsys):
    from nexus.ops import local_stack, openai_local, sec_graphic_adoption_local as local
    from nexus.settings import EngineSettings
    from sqlalchemy.exc import SQLAlchemyError

    async def unknown(*args):
        raise SQLAlchemyError("PRIVATE_RESPONSE_MUST_NOT_PRINT")

    monkeypatch.setattr(local, "run", unknown)
    monkeypatch.setattr(local_stack, "_validate_local_env", lambda *args: None)
    monkeypatch.setattr(openai_local, "local_settings", lambda *args, **kwargs: EngineSettings())
    monkeypatch.setattr("sys.argv", ["operator", "--repo-root", str(tmp_path), "inspect"])
    assert local.main() == 2
    output = capsys.readouterr().out
    assert '"status": "unknown"' in output
    assert "inspect_original_identity_without_resubmission" in output
    assert "PRIVATE_RESPONSE_MUST_NOT_PRINT" not in output
