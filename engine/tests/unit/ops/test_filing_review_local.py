"""Normal review preparation cannot accept a reviewer or issue content authority."""

import pytest
from nexus.ops.research_monitor import parser


@pytest.mark.asyncio
async def test_unconfigured_export_cannot_connect_or_write(tmp_path, monkeypatch):
    from uuid import UUID

    from nexus.ops import filing_review_local
    from nexus.settings import EngineSettings

    def forbidden(*args):
        raise AssertionError("unconfigured export reached database")

    monkeypatch.setattr(filing_review_local, "session_factory", forbidden)
    output = tmp_path / "review"
    with pytest.raises(ValueError, match="MONITOR_ACCOUNT_UNCONFIGURED"):
        await filing_review_local.prepare_review_material(
            EngineSettings(),
            snapshot_id=UUID(int=1),
            output_dir=output,
        )
    assert not output.exists()


def test_review_prepare_is_a_fixed_source_export_without_authority_options(tmp_path):
    args = parser().parse_args(
        [
            "prepare-content-review",
            "--snapshot-id",
            "00000000-0000-0000-0000-000000000001",
            "--output-dir",
            str(tmp_path / "review"),
        ]
    )
    assert args.output_dir == tmp_path / "review"
    for option in ("--reviewer", "--qualified", "--operator-confirmed", "--account-id"):
        with pytest.raises(SystemExit):
            parser().parse_args(
                [
                    "prepare-content-review",
                    "--snapshot-id",
                    str(args.snapshot_id),
                    "--output-dir",
                    str(args.output_dir),
                    option,
                    "x",
                ]
            )
