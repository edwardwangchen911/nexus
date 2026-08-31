"""Create-only, held-dirfd publication for sanitized probe evidence."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
import secrets
import stat
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from nexus_fosun_gateway.adapter.probe_sdk import ProbeSdkError

_PUBLICATION_ATTESTATION = object()
_OUTPUT_BASENAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,62}\.json")
_HEX64 = re.compile(r"[0-9a-f]{64}")


class DurableProbePublication:
    """Opaque proof returned only after the complete fsync transaction."""

    __slots__ = ("_attestation", "artifact_sha256", "byte_length")

    def __init__(
        self,
        *,
        artifact_sha256: str,
        byte_length: int,
        _attestation: object | None = None,
    ) -> None:
        if (
            _attestation is not _PUBLICATION_ATTESTATION
            or _HEX64.fullmatch(artifact_sha256) is None
            or not isinstance(byte_length, int)
            or isinstance(byte_length, bool)
            or not 1 <= byte_length <= 1024 * 1024
        ):
            raise ProbeSdkError("PROBE_OUTPUT_INVALID")
        self.artifact_sha256 = artifact_sha256
        self.byte_length = byte_length
        self._attestation = _attestation

    def __repr__(self) -> str:
        return (
            "DurableProbePublication("
            f"artifact_sha256={self.artifact_sha256!r}, "
            f"byte_length={self.byte_length!r})"
        )

    def attests(self, artifact_sha256: str, byte_length: int) -> bool:
        return (
            self._attestation is _PUBLICATION_ATTESTATION
            and hmac.compare_digest(self.artifact_sha256, artifact_sha256)
            and self.byte_length == byte_length
        )


def load_probe_signing_key(
    private_key_path: Path,
    trusted_public_key_b64u: str,
) -> Ed25519PrivateKey:
    """Load one protected raw Ed25519 seed and bind it to the configured public pin."""
    directory_fd: int | None = None
    file_descriptor: int | None = None
    try:
        if not private_key_path.is_absolute():
            raise OSError("invalid private key path")
        trusted_public_key = _decode_b64u(trusted_public_key_b64u)
        directory_fd = _open_trusted_output_dir(private_key_path.parent)
        file_descriptor = os.open(
            private_key_path.name,
            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
            dir_fd=directory_fd,
        )
        before = os.fstat(file_descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_size != 43
        ):
            raise OSError("invalid private key file")
        encoded_private_key = _read_exact(file_descriptor, 43)
        after = os.fstat(file_descriptor)
        if (
            len(encoded_private_key) != 43
            or before.st_dev != after.st_dev
            or before.st_ino != after.st_ino
            or before.st_uid != after.st_uid
            or before.st_mode != after.st_mode
            or before.st_nlink != after.st_nlink
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
            or before.st_ctime_ns != after.st_ctime_ns
        ):
            raise OSError("unstable private key file")
        raw_private_key = _decode_b64u(encoded_private_key.decode("ascii"))
        private_key = Ed25519PrivateKey.from_private_bytes(raw_private_key)
        actual_public_key = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        if not hmac.compare_digest(actual_public_key, trusted_public_key):
            raise OSError("publisher key mismatch")
        return private_key
    except (OSError, UnicodeError, ValueError):
        raise ProbeSdkError("PROBE_SETTINGS_INVALID") from None
    finally:
        if file_descriptor is not None:
            try:
                os.close(file_descriptor)
            except OSError:
                pass
        if directory_fd is not None:
            try:
                os.close(directory_fd)
            except OSError:
                pass

def publish_probe_artifact(
    output_dir: Path,
    output_basename: str,
    artifact_bytes: bytes,
    *,
    expected_sha256: str,
    expected_byte_length: int,
) -> DurableProbePublication:
    """Durably publish one artifact without replacing any existing name."""
    if (
        not output_dir.is_absolute()
        or _OUTPUT_BASENAME.fullmatch(output_basename) is None
        or not isinstance(artifact_bytes, bytes)
        or not artifact_bytes
        or len(artifact_bytes) > 1024 * 1024
        or expected_byte_length != len(artifact_bytes)
        or _HEX64.fullmatch(expected_sha256) is None
        or not hmac.compare_digest(
            hashlib.sha256(artifact_bytes).hexdigest(),
            expected_sha256,
        )
    ):
        raise ProbeSdkError("PROBE_OUTPUT_INVALID")

    directory_fd: int | None = None
    temporary_fd: int | None = None
    temporary_name: str | None = None
    final_linked = False
    try:
        directory_fd = _open_trusted_output_dir(output_dir)
        temporary_name = f".nexus-probe-{secrets.token_hex(16)}.tmp"
        temporary_fd = os.open(
            temporary_name,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_NOFOLLOW
            | os.O_CLOEXEC,
            0o600,
            dir_fd=directory_fd,
        )
        opened = os.fstat(temporary_fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_nlink != 1
        ):
            raise OSError("invalid temporary output")
        _write_all(temporary_fd, artifact_bytes)
        os.fsync(temporary_fd)
        os.close(temporary_fd)
        temporary_fd = None
        os.link(
            temporary_name,
            output_basename,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
            follow_symlinks=False,
        )
        final_linked = True
        os.fsync(directory_fd)
        os.unlink(temporary_name, dir_fd=directory_fd)
        temporary_name = None
        os.fsync(directory_fd)
        return DurableProbePublication(
            artifact_sha256=expected_sha256,
            byte_length=expected_byte_length,
            _attestation=_PUBLICATION_ATTESTATION,
        )
    except (KeyboardInterrupt, SystemExit):
        _cleanup_failed_publication(
            directory_fd,
            output_basename,
            temporary_name,
            final_linked=final_linked,
        )
        raise
    except Exception:
        cleanup_proven = _cleanup_failed_publication(
            directory_fd,
            output_basename,
            temporary_name,
            final_linked=final_linked,
        )
        if not cleanup_proven:
            raise ProbeSdkError("PROBE_OUTPUT_QUARANTINE_REQUIRED") from None
        raise ProbeSdkError("PROBE_OUTPUT_INVALID") from None
    finally:
        if temporary_fd is not None:
            try:
                os.close(temporary_fd)
            except OSError:
                pass
        if directory_fd is not None:
            try:
                os.close(directory_fd)
            except OSError:
                pass


def _open_trusted_output_dir(path: Path) -> int:
    components = path.parts
    if not components or not path.is_absolute():
        raise OSError("invalid output directory")
    directory_fd = os.open(
        os.path.sep,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
    )
    try:
        for component in components[1:]:
            if component in {"", ".", ".."}:
                raise OSError("invalid output directory")
            next_fd = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=directory_fd,
            )
            os.close(directory_fd)
            directory_fd = next_fd
        directory = os.fstat(directory_fd)
        if (
            not stat.S_ISDIR(directory.st_mode)
            or directory.st_uid != os.geteuid()
            or stat.S_IMODE(directory.st_mode) != 0o700
        ):
            raise OSError("untrusted output directory")
        return directory_fd
    except BaseException:
        os.close(directory_fd)
        raise


def _write_all(file_descriptor: int, content: bytes) -> None:
    view = memoryview(content)
    offset = 0
    while offset < len(view):
        try:
            written = os.write(file_descriptor, view[offset:])
        except InterruptedError:
            continue
        if written <= 0:
            raise OSError("short output write")
        offset += written


def _read_exact(file_descriptor: int, expected_length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = expected_length + 1
    while remaining > 0:
        try:
            chunk = os.read(file_descriptor, remaining)
        except InterruptedError:
            continue
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _decode_b64u(value: str) -> bytes:
    if len(value) != 43:
        raise ValueError("invalid key encoding")
    decoded = base64.b64decode(value + "=", altchars=b"-_", validate=True)
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
    if len(decoded) != 32 or not hmac.compare_digest(canonical, value):
        raise ValueError("invalid key encoding")
    return decoded


def _cleanup_failed_publication(
    directory_fd: int | None,
    output_basename: str,
    temporary_name: str | None,
    *,
    final_linked: bool,
) -> bool:
    if directory_fd is None:
        return not final_linked
    proven = True
    if final_linked:
        try:
            os.unlink(output_basename, dir_fd=directory_fd)
        except OSError:
            proven = False
        try:
            os.fsync(directory_fd)
        except OSError:
            proven = False
    if temporary_name is not None:
        try:
            os.unlink(temporary_name, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        except OSError:
            proven = False
    return proven
