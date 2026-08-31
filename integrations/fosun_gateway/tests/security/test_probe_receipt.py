from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

import nexus_fosun_gateway.probe_receipt as probe_receipt
import pytest


def test_receipt_publisher_binds_success_frame_to_current_artifact(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    frame_dir, artifact_dir, artifact_name, receipt, artifact_sha256 = _evidence(
        tmp_path
    )

    exit_code = probe_receipt.main(
        _arguments(frame_dir, artifact_dir, artifact_name)
    )

    captured = capfd.readouterr()
    receipt_bytes = json.dumps(
        receipt,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "status": "receipt_published",
        "artifact_sha256": artifact_sha256,
        "receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
    }
    assert (artifact_dir / "publication-receipt.json").read_bytes() == receipt_bytes


def test_nonzero_capture_outcome_cannot_publish_complete_success_frame(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    frame_dir, artifact_dir, artifact_name, _receipt, _sha256 = _evidence(tmp_path)
    arguments = _arguments(frame_dir, artifact_dir, artifact_name)
    arguments[1] = "130"

    exit_code = probe_receipt.main(arguments)

    captured = capfd.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == ""
    assert not (artifact_dir / "publication-receipt.json").exists()


def test_reserved_receipt_name_is_refused_defensively_before_file_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame_dir = tmp_path / "operator"
    artifact_dir = tmp_path / "staging"
    frame_dir.mkdir(mode=0o700)
    artifact_dir.mkdir(mode=0o700)
    monkeypatch.setattr(
        probe_receipt,
        "_read_current_user_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError),
    )

    exit_code = probe_receipt.main(
        _arguments(frame_dir, artifact_dir, "publication-receipt.json")
    )

    assert exit_code == 2


def test_receipt_publisher_rejects_symlink_frame_without_leaking_values(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    frame_dir, artifact_dir, artifact_name, _receipt, _sha256 = _evidence(tmp_path)
    frame = frame_dir / "probe.frame.json"
    canary = "synthetic-frame-secret-canary"
    target = frame_dir / "target.frame.json"
    target.write_text(canary)
    target.chmod(0o600)
    frame.unlink()
    frame.symlink_to(target.name)

    exit_code = probe_receipt.main(
        _arguments(frame_dir, artifact_dir, artifact_name)
    )

    captured = capfd.readouterr()
    assert exit_code == 2
    assert canary not in captured.out
    assert canary not in captured.err
    assert not (artifact_dir / "publication-receipt.json").exists()


def test_receipt_publisher_preserves_existing_receipt(
    tmp_path: Path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    frame_dir, artifact_dir, artifact_name, _receipt, _sha256 = _evidence(tmp_path)
    existing = artifact_dir / "publication-receipt.json"
    existing.write_bytes(b"preserve")
    existing.chmod(0o600)

    exit_code = probe_receipt.main(
        _arguments(frame_dir, artifact_dir, artifact_name)
    )

    captured = capfd.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == ""
    assert existing.read_bytes() == b"preserve"


def test_receipt_publisher_requires_exact_owner_only_frame_mode(
    tmp_path: Path,
) -> None:
    frame_dir, artifact_dir, artifact_name, _receipt, _sha256 = _evidence(tmp_path)
    (frame_dir / "probe.frame.json").chmod(0o640)

    exit_code = probe_receipt.main(
        _arguments(frame_dir, artifact_dir, artifact_name)
    )

    assert exit_code == 2
    assert not (artifact_dir / "publication-receipt.json").exists()


def _arguments(
    frame_dir: Path,
    artifact_dir: Path,
    artifact_name: str,
) -> list[str]:
    return [
        "--capture-exit",
        "0",
        "--frame-dir",
        str(frame_dir),
        "--frame-basename",
        "probe.frame.json",
        "--artifact-dir",
        str(artifact_dir),
        "--artifact-basename",
        artifact_name,
    ]


def _evidence(
    tmp_path: Path,
) -> tuple[Path, Path, str, dict[str, str], str]:
    frame_dir = tmp_path / "operator"
    artifact_dir = tmp_path / "staging"
    frame_dir.mkdir(mode=0o700)
    artifact_dir.mkdir(mode=0o700)
    artifact_name = "probe.json"
    artifact_bytes = b'{"schema_version":"synthetic"}'
    artifact_path = artifact_dir / artifact_name
    artifact_path.write_bytes(artifact_bytes)
    artifact_path.chmod(0o600)
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    receipt = {
        "schema_version": "nexus.fosun.probe-publication.v1",
        "artifact_sha256": artifact_sha256,
        "signature_b64u": base64.urlsafe_b64encode(bytes(64)).rstrip(b"=").decode(),
    }
    frame = {
        "status": "probe_published",
        "code": "OK",
        "artifact_sha256": artifact_sha256,
        "byte_length": len(artifact_bytes),
        "capability_expires_at": "2026-08-31T09:00:00Z",
        "activation_contract_hash": "1" * 64,
        "publication_receipt": receipt,
    }
    frame_path = frame_dir / "probe.frame.json"
    frame_path.write_text(json.dumps(frame, separators=(",", ":")) + "\n")
    frame_path.chmod(0o600)
    return frame_dir, artifact_dir, artifact_name, receipt, artifact_sha256
