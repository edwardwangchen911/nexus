import base64
import hashlib
import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from nexus_fosun_gateway.adapter.probe_sdk import ProbeSdkError
from nexus_fosun_gateway.publication import (
    DurableProbePublication,
    load_probe_signing_key,
    publish_probe_artifact,
)


def test_publisher_create_only_fsyncs_value_free_bytes_and_returns_attestation(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    artifact_bytes = b'{"schema_version":"synthetic"}'
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()

    publication = publish_probe_artifact(
        output_dir,
        "probe.json",
        artifact_bytes,
        expected_sha256=artifact_sha256,
        expected_byte_length=len(artifact_bytes),
    )

    output = output_dir / "probe.json"
    assert output.read_bytes() == artifact_bytes
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert publication.attests(artifact_sha256, len(artifact_bytes))
    assert "output" not in repr(publication).lower()


def test_publication_stages_file_link_and_directory_durability_in_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    events: list[str] = []
    real_fsync = os.fsync
    real_link = os.link
    real_unlink = os.unlink

    def recording_fsync(file_descriptor: int) -> None:
        kind = "dir" if stat.S_ISDIR(os.fstat(file_descriptor).st_mode) else "file"
        events.append(f"fsync:{kind}")
        real_fsync(file_descriptor)

    def recording_link(*args: object, **kwargs: object) -> None:
        events.append("link:final")
        real_link(*args, **kwargs)

    def recording_unlink(path: str, *, dir_fd: int | None = None) -> None:
        events.append("unlink:temp" if path.startswith(".nexus-probe-") else "unlink:final")
        real_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(os, "fsync", recording_fsync)
    monkeypatch.setattr(os, "link", recording_link)
    monkeypatch.setattr(os, "unlink", recording_unlink)

    publish_probe_artifact(
        output_dir,
        "probe.json",
        b"new",
        expected_sha256=hashlib.sha256(b"new").hexdigest(),
        expected_byte_length=3,
    )

    assert events == [
        "fsync:file",
        "link:final",
        "fsync:dir",
        "unlink:temp",
        "fsync:dir",
    ]


def test_publisher_retries_partial_writes_until_the_exact_bytes_are_complete(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    content = b"partial-write-loop"
    real_write = os.write
    write_calls = 0

    def one_byte_write(file_descriptor: int, chunk: bytes | memoryview) -> int:
        nonlocal write_calls
        write_calls += 1
        return real_write(file_descriptor, chunk[:1])

    monkeypatch.setattr(os, "write", one_byte_write)

    publish_probe_artifact(
        output_dir,
        "probe.json",
        content,
        expected_sha256=hashlib.sha256(content).hexdigest(),
        expected_byte_length=len(content),
    )

    assert write_calls == len(content)
    assert (output_dir / "probe.json").read_bytes() == content


def test_publisher_quarantines_temporary_name_swap_before_link(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    real_link = os.link

    def swap_then_link(
        source: str,
        destination: str,
        *,
        src_dir_fd: int,
        dst_dir_fd: int,
        follow_symlinks: bool,
    ) -> None:
        os.unlink(source, dir_fd=src_dir_fd)
        replacement_fd = os.open(
            source,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=src_dir_fd,
        )
        try:
            os.write(replacement_fd, b"replacement")
        finally:
            os.close(replacement_fd)
        real_link(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(os, "link", swap_then_link)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"expected",
            expected_sha256=hashlib.sha256(b"expected").hexdigest(),
            expected_byte_length=len(b"expected"),
        )

    assert exc_info.value.code == "PROBE_OUTPUT_QUARANTINE_REQUIRED"
    assert (output_dir / "probe.json").read_bytes() == b"replacement"


def test_publisher_quarantines_final_name_swap_after_link(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    real_fsync = os.fsync
    directory_fsyncs = 0

    def swap_after_second_directory_fsync(file_descriptor: int) -> None:
        nonlocal directory_fsyncs
        real_fsync(file_descriptor)
        if not stat.S_ISDIR(os.fstat(file_descriptor).st_mode):
            return
        directory_fsyncs += 1
        if directory_fsyncs != 2:
            return
        os.unlink("probe.json", dir_fd=file_descriptor)
        replacement_fd = os.open(
            "probe.json",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=file_descriptor,
        )
        try:
            os.write(replacement_fd, b"replacement")
        finally:
            os.close(replacement_fd)

    monkeypatch.setattr(os, "fsync", swap_after_second_directory_fsync)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"expected",
            expected_sha256=hashlib.sha256(b"expected").hexdigest(),
            expected_byte_length=len(b"expected"),
        )

    assert exc_info.value.code == "PROBE_OUTPUT_QUARANTINE_REQUIRED"
    assert (output_dir / "probe.json").read_bytes() == b"replacement"


def test_publisher_preserves_preexisting_temporary_name_collision(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    monkeypatch.setattr(
        "nexus_fosun_gateway.publication.secrets.token_hex",
        lambda _length: "fixed",
    )
    collision = output_dir / ".nexus-probe-fixed.tmp"
    collision.write_bytes(b"preserve")
    collision.chmod(0o600)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"expected",
            expected_sha256=hashlib.sha256(b"expected").hexdigest(),
            expected_byte_length=len(b"expected"),
        )

    assert exc_info.value.code == "PROBE_OUTPUT_INVALID"
    assert collision.read_bytes() == b"preserve"
    assert not (output_dir / "probe.json").exists()


def test_interrupt_after_temporary_open_before_identity_requires_quarantine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    monkeypatch.setattr(
        "nexus_fosun_gateway.publication.secrets.token_hex",
        lambda _length: "fixed",
    )
    real_open = os.open

    def open_then_interrupt(directory_fd: int, candidate_name: str) -> int:
        real_open(
            candidate_name,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=directory_fd,
        )
        raise KeyboardInterrupt

    monkeypatch.setattr(
        "nexus_fosun_gateway.publication._open_temporary_output",
        open_then_interrupt,
    )

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"expected",
            expected_sha256=hashlib.sha256(b"expected").hexdigest(),
            expected_byte_length=len(b"expected"),
        )

    assert exc_info.value.code == "PROBE_OUTPUT_QUARANTINE_REQUIRED"
    assert (output_dir / ".nexus-probe-fixed.tmp").exists()
    assert not (output_dir / "probe.json").exists()


def test_publisher_never_unlinks_swapped_foreign_temporary_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    monkeypatch.setattr(
        "nexus_fosun_gateway.publication.secrets.token_hex",
        lambda _length: "fixed",
    )
    temporary = output_dir / ".nexus-probe-fixed.tmp"
    real_fsync = os.fsync

    def swap_then_fail_fsync(file_descriptor: int) -> None:
        if stat.S_ISREG(os.fstat(file_descriptor).st_mode):
            temporary.unlink()
            temporary.write_bytes(b"foreign")
            temporary.chmod(0o600)
            raise OSError("synthetic-file-fsync-canary")
        real_fsync(file_descriptor)

    monkeypatch.setattr(os, "fsync", swap_then_fail_fsync)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"expected",
            expected_sha256=hashlib.sha256(b"expected").hexdigest(),
            expected_byte_length=len(b"expected"),
        )

    assert exc_info.value.code == "PROBE_OUTPUT_QUARANTINE_REQUIRED"
    assert temporary.read_bytes() == b"foreign"


@pytest.mark.parametrize(
    "failure_stage",
    [
        "partial_write",
        "file_fsync",
        "link",
        "unsupported_link_flags",
        "second_directory_fsync",
        "temporary_unlink",
    ],
)
def test_recoverable_publication_stage_failure_leaves_no_residual_file(
    failure_stage: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    real_write = os.write
    real_fsync = os.fsync
    real_link = os.link
    real_unlink = os.unlink
    writes = 0
    directory_fsyncs = 0
    temporary_unlinks = 0

    def injected_write(file_descriptor: int, chunk: bytes | memoryview) -> int:
        nonlocal writes
        writes += 1
        if failure_stage == "partial_write":
            if writes == 1:
                return real_write(file_descriptor, chunk[:1])
            raise OSError("synthetic partial-write failure")
        return real_write(file_descriptor, chunk)

    def injected_fsync(file_descriptor: int) -> None:
        nonlocal directory_fsyncs
        metadata = os.fstat(file_descriptor)
        if failure_stage == "file_fsync" and stat.S_ISREG(metadata.st_mode):
            raise OSError("synthetic file-fsync failure")
        if stat.S_ISDIR(metadata.st_mode):
            directory_fsyncs += 1
            if failure_stage == "second_directory_fsync" and directory_fsyncs == 2:
                raise OSError("synthetic second-directory-fsync failure")
        real_fsync(file_descriptor)

    def injected_link(*args: object, **kwargs: object) -> None:
        if failure_stage == "link":
            raise OSError("synthetic link failure")
        if failure_stage == "unsupported_link_flags":
            raise NotImplementedError("synthetic unsupported link flags")
        real_link(*args, **kwargs)

    def injected_unlink(path: str, *, dir_fd: int | None = None) -> None:
        nonlocal temporary_unlinks
        if path.startswith(".nexus-probe-"):
            temporary_unlinks += 1
            if failure_stage == "temporary_unlink" and temporary_unlinks == 1:
                raise OSError("synthetic temporary-unlink failure")
        real_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(os, "write", injected_write)
    monkeypatch.setattr(os, "fsync", injected_fsync)
    monkeypatch.setattr(os, "link", injected_link)
    monkeypatch.setattr(os, "unlink", injected_unlink)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )

    expected_code = (
        "PROBE_OUTPUT_QUARANTINE_REQUIRED"
        if failure_stage in {"link", "unsupported_link_flags"}
        else "PROBE_OUTPUT_INVALID"
    )
    assert exc_info.value.code == expected_code
    assert list(output_dir.iterdir()) == []


def test_durable_publication_cannot_be_constructed_by_a_caller() -> None:
    with pytest.raises(ProbeSdkError) as exc_info:
        DurableProbePublication(
            artifact_sha256="0" * 64,
            byte_length=1,
        )

    assert exc_info.value.code == "PROBE_OUTPUT_INVALID"


def test_publisher_rejects_existing_final_and_untrusted_output_directory(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    (output_dir / "probe.json").write_bytes(b"existing")

    with pytest.raises(ProbeSdkError) as existing:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )
    assert existing.value.code == "PROBE_OUTPUT_EXISTS"
    assert (output_dir / "probe.json").read_bytes() == b"existing"

    unsafe_dir = tmp_path / "unsafe"
    unsafe_dir.mkdir(mode=0o755)
    with pytest.raises(ProbeSdkError) as unsafe:
        publish_probe_artifact(
            unsafe_dir,
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )
    assert unsafe.value.code == "PROBE_OUTPUT_INVALID"


def test_publisher_never_follows_output_directory_symlink(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir(mode=0o700)
    linked = tmp_path / "linked"
    linked.symlink_to(actual, target_is_directory=True)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            linked,
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )

    assert exc_info.value.code == "PROBE_OUTPUT_INVALID"
    assert not (actual / "probe.json").exists()


def test_publisher_never_follows_intermediate_output_directory_symlink(
    tmp_path: Path,
) -> None:
    actual_parent = tmp_path / "actual-parent"
    actual_parent.mkdir(mode=0o700)
    output_dir = actual_parent / "output"
    output_dir.mkdir(mode=0o700)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(actual_parent, target_is_directory=True)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            linked_parent / "output",
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )

    assert exc_info.value.code == "PROBE_OUTPUT_INVALID"
    assert not (output_dir / "probe.json").exists()


@pytest.mark.parametrize("parent_mode", [0o770, 0o777])
def test_publisher_rejects_writable_ancestor_directory(
    tmp_path: Path,
    parent_mode: int,
) -> None:
    writable_parent = tmp_path / "writable-parent"
    writable_parent.mkdir(mode=0o700)
    output_dir = writable_parent / "output"
    output_dir.mkdir(mode=0o700)
    writable_parent.chmod(parent_mode)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )

    assert exc_info.value.code == "PROBE_OUTPUT_INVALID"
    assert not (output_dir / "probe.json").exists()


def test_publisher_refuses_output_directory_rename_before_attestation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    moved_dir = tmp_path / "moved-output"
    real_fsync = os.fsync
    directory_fsyncs = 0

    def swap_after_link(file_descriptor: int) -> None:
        nonlocal directory_fsyncs
        metadata = os.fstat(file_descriptor)
        real_fsync(file_descriptor)
        if not stat.S_ISDIR(metadata.st_mode):
            return
        directory_fsyncs += 1
        if directory_fsyncs == 1:
            output_dir.rename(moved_dir)
            output_dir.mkdir(mode=0o700)

    monkeypatch.setattr(os, "fsync", swap_after_link)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )

    assert exc_info.value.code == "PROBE_OUTPUT_INVALID"
    assert not (output_dir / "probe.json").exists()
    assert not (moved_dir / "probe.json").exists()


def test_interrupted_link_is_cleaned_when_the_final_was_created(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    real_link = os.link

    def link_then_interrupt(*args: object, **kwargs: object) -> None:
        real_link(*args, **kwargs)
        raise KeyboardInterrupt

    monkeypatch.setattr(os, "link", link_then_interrupt)

    with pytest.raises(KeyboardInterrupt):
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )

    assert list(output_dir.iterdir()) == []


def test_link_then_raise_with_foreign_final_requires_quarantine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    real_link = os.link

    def swap_link_then_raise(
        source: str,
        destination: str,
        *,
        src_dir_fd: int,
        dst_dir_fd: int,
        follow_symlinks: bool,
    ) -> None:
        os.unlink(source, dir_fd=src_dir_fd)
        replacement_fd = os.open(
            source,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=src_dir_fd,
        )
        try:
            os.write(replacement_fd, b"replacement")
        finally:
            os.close(replacement_fd)
        real_link(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )
        raise OSError("synthetic-link-return-canary")

    monkeypatch.setattr(os, "link", swap_link_then_raise)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"expected",
            expected_sha256=hashlib.sha256(b"expected").hexdigest(),
            expected_byte_length=len(b"expected"),
        )

    assert exc_info.value.code == "PROBE_OUTPUT_QUARANTINE_REQUIRED"
    assert (output_dir / "probe.json").read_bytes() == b"replacement"


def test_link_create_unlink_then_raise_requires_quarantine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    real_link = os.link

    def link_unlink_then_raise(*args: object, **kwargs: object) -> None:
        real_link(*args, **kwargs)
        os.unlink("probe.json", dir_fd=kwargs["dst_dir_fd"])
        raise OSError("synthetic-uncertain-link-canary")

    monkeypatch.setattr(os, "link", link_unlink_then_raise)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"expected",
            expected_sha256=hashlib.sha256(b"expected").hexdigest(),
            expected_byte_length=len(b"expected"),
        )

    assert exc_info.value.code == "PROBE_OUTPUT_QUARANTINE_REQUIRED"
    assert list(output_dir.iterdir()) == []


def test_uncertain_interrupted_link_cleanup_requires_quarantine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    real_link = os.link
    real_unlink = os.unlink

    def link_then_interrupt(*args: object, **kwargs: object) -> None:
        real_link(*args, **kwargs)
        raise KeyboardInterrupt

    def refuse_final_cleanup(path: str, *, dir_fd: int | None = None) -> None:
        if path == "probe.json":
            raise OSError("synthetic-cleanup-canary")
        real_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(os, "link", link_then_interrupt)
    monkeypatch.setattr(os, "unlink", refuse_final_cleanup)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )

    assert exc_info.value.code == "PROBE_OUTPUT_QUARANTINE_REQUIRED"
    assert (output_dir / "probe.json").exists()


def test_post_link_failure_removes_unsigned_final(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    real_fsync = os.fsync
    calls = 0

    def fail_first_directory_fsync(fd: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic-fsync-canary")
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fail_first_directory_fsync)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )

    assert exc_info.value.code == "PROBE_OUTPUT_INVALID"
    assert not (output_dir / "probe.json").exists()


def test_uncertain_post_link_cleanup_requires_quarantine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    real_fsync = os.fsync
    real_unlink = os.unlink
    calls = 0

    def fail_first_directory_fsync(fd: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic-fsync-canary")
        real_fsync(fd)

    def fail_final_unlink(path: str, *, dir_fd: int | None = None) -> None:
        if path == "probe.json":
            raise OSError("synthetic-unlink-canary")
        real_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(os, "fsync", fail_first_directory_fsync)
    monkeypatch.setattr(os, "unlink", fail_final_unlink)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )

    assert exc_info.value.code == "PROBE_OUTPUT_QUARANTINE_REQUIRED"
    assert (output_dir / "probe.json").exists()


def test_uncertain_pre_link_temporary_cleanup_requires_quarantine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    real_unlink = os.unlink

    def fail_write(_fd: int, _content: memoryview) -> int:
        raise OSError("synthetic-write-canary")

    def fail_temporary_unlink(path: str, *, dir_fd: int | None = None) -> None:
        if path.startswith(".nexus-probe-"):
            raise OSError("synthetic-unlink-canary")
        real_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(os, "write", fail_write)
    monkeypatch.setattr(os, "unlink", fail_temporary_unlink)

    with pytest.raises(ProbeSdkError) as exc_info:
        publish_probe_artifact(
            output_dir,
            "probe.json",
            b"new",
            expected_sha256=hashlib.sha256(b"new").hexdigest(),
            expected_byte_length=3,
        )

    assert exc_info.value.code == "PROBE_OUTPUT_QUARANTINE_REQUIRED"
    assert not (output_dir / "probe.json").exists()


def test_signing_key_loader_requires_exact_private_file_and_matching_public_pin(
    tmp_path: Path,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    private_key_path = tmp_path / "publisher-private.b64u"
    private_key_path.write_bytes(_b64u(bytes(range(1, 33))).encode("ascii"))
    private_key_path.chmod(0o600)
    trusted_public_key = _b64u(
        key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )

    loaded = load_probe_signing_key(private_key_path, trusted_public_key)

    assert loaded.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    ) == key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )

    with pytest.raises(ProbeSdkError) as mismatch:
        load_probe_signing_key(private_key_path, "A" * 43)
    assert mismatch.value.code == "PROBE_SETTINGS_INVALID"


@pytest.mark.parametrize("unsafe_kind", ["mode", "symlink", "hard_link"])
def test_signing_key_loader_rejects_unsafe_files(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    trusted_public_key = _b64u(
        key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )
    actual = tmp_path / "actual-private.b64u"
    actual.write_bytes(_b64u(bytes(range(1, 33))).encode("ascii"))
    actual.chmod(0o600)
    supplied = actual
    if unsafe_kind == "mode":
        actual.chmod(0o644)
    elif unsafe_kind == "symlink":
        supplied = tmp_path / "linked-private.b64u"
        supplied.symlink_to(actual)
    else:
        supplied = tmp_path / "hard-linked-private.b64u"
        os.link(actual, supplied)

    with pytest.raises(ProbeSdkError) as exc_info:
        load_probe_signing_key(supplied, trusted_public_key)

    assert exc_info.value.code == "PROBE_SETTINGS_INVALID"


@pytest.mark.parametrize("parent_mode", [0o770, 0o777])
def test_signing_key_loader_rejects_writable_parent_directory(
    tmp_path: Path,
    parent_mode: int,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    trusted_public_key = _b64u(
        key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )
    private_parent = tmp_path / "publisher"
    private_parent.mkdir(mode=0o700)
    private_key_path = private_parent / "private.b64u"
    private_key_path.write_bytes(_b64u(bytes(range(1, 33))).encode("ascii"))
    private_key_path.chmod(0o600)
    private_parent.chmod(parent_mode)

    with pytest.raises(ProbeSdkError) as exc_info:
        load_probe_signing_key(private_key_path, trusted_public_key)

    assert exc_info.value.code == "PROBE_SETTINGS_INVALID"


@pytest.mark.parametrize("changed_field", ["st_ctime_ns", "st_nlink"])
def test_signing_key_loader_rejects_metadata_change_during_read(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    changed_field: str,
) -> None:
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    private_key_path = tmp_path / "publisher-private.b64u"
    private_key_path.write_bytes(_b64u(bytes(range(1, 33))).encode("ascii"))
    private_key_path.chmod(0o600)
    trusted_public_key = _b64u(
        key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )
    real_fstat = os.fstat
    regular_file_reads = 0

    def drifting_fstat(file_descriptor: int):
        nonlocal regular_file_reads
        metadata = real_fstat(file_descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            return metadata
        regular_file_reads += 1
        if regular_file_reads != 2:
            return metadata
        fields = {
            name: getattr(metadata, name)
            for name in (
                "st_dev",
                "st_ino",
                "st_uid",
                "st_mode",
                "st_nlink",
                "st_size",
                "st_mtime_ns",
                "st_ctime_ns",
            )
        }
        fields[changed_field] += 1
        return SimpleNamespace(**fields)

    monkeypatch.setattr(os, "fstat", drifting_fstat)

    with pytest.raises(ProbeSdkError) as exc_info:
        load_probe_signing_key(private_key_path, trusted_public_key)

    assert exc_info.value.code == "PROBE_SETTINGS_INVALID"
    assert regular_file_reads == 2


def _b64u(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")
