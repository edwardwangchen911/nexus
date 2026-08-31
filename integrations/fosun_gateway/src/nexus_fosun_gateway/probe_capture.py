"""Secure create-only capture wrapper for the one-shot Fosun probe result."""

from __future__ import annotations

import json
import os
import re
import stat
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from nexus_fosun_gateway.adapter.probe_sdk import ProbeSdkError
from nexus_fosun_gateway.probe_contract import (
    PROBE_CLI_ARGUMENT_NAMES,
    PROBE_FAILURE_CATEGORIES,
    PROBE_FAILURE_OPERATIONS,
    PROBE_RESULT_FIELD_NAMES,
)
from nexus_fosun_gateway.publication import (
    _directory_path_matches_fd,
    _open_trusted_output_dir,
    _read_exact,
    load_probe_signing_key,
)
from nexus_fosun_gateway.security.provider_environment import (
    reject_unsafe_provider_environment,
)

_FRAME_ARGUMENT_NAMES = ("--frame-output-dir", "--frame-basename")
_FRAME_BASENAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,56}\.frame\.json")
_MAX_FRAME_BYTES = 16 * 1024
ProbeRunner = Callable[[int, tuple[str, ...]], int]


def capture_probe_result_frame(
    operator_dir: Path,
    frame_basename: str,
    probe_arguments: tuple[str, ...],
    *,
    runner: ProbeRunner | None = None,
) -> int:
    """Run the fixed probe command with stdout bound to one protected new file."""
    if (
        not operator_dir.is_absolute()
        or _FRAME_BASENAME.fullmatch(frame_basename) is None
        or len(probe_arguments) != len(PROBE_CLI_ARGUMENT_NAMES) * 2
        or tuple(probe_arguments[::2]) != PROBE_CLI_ARGUMENT_NAMES
        or any(not isinstance(value, str) for value in probe_arguments)
    ):
        raise ProbeSdkError("INVALID_PROBE_PLAN")

    directory_fd: int | None = None
    frame_fd: int | None = None
    try:
        directory_fd = _open_trusted_output_dir(operator_dir)
        try:
            frame_fd = os.open(
                frame_basename,
                os.O_RDWR
                | os.O_CREAT
                | os.O_EXCL
                | os.O_NOFOLLOW
                | os.O_CLOEXEC,
                0o600,
                dir_fd=directory_fd,
            )
        except FileExistsError:
            raise ProbeSdkError("PROBE_OUTPUT_EXISTS") from None
        opened = os.fstat(frame_fd)
        if not _is_protected_frame(opened):
            raise OSError("invalid result frame")
        frame_identity = (opened.st_dev, opened.st_ino)
        os.fsync(directory_fd)

        resolved_runner = _run_probe_cli if runner is None else runner
        try:
            exit_code = resolved_runner(frame_fd, probe_arguments)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            exit_code = 2

        os.fsync(frame_fd)
        os.fsync(directory_fd)
        after = os.fstat(frame_fd)
        final = os.stat(frame_basename, dir_fd=directory_fd, follow_symlinks=False)
        if (
            not _is_protected_frame(after)
            or (after.st_dev, after.st_ino) != frame_identity
            or (final.st_dev, final.st_ino) != frame_identity
            or not _directory_path_matches_fd(operator_dir, directory_fd)
        ):
            raise OSError("unstable result frame")
        os.lseek(frame_fd, 0, os.SEEK_SET)
        frame = _read_exact(frame_fd, _MAX_FRAME_BYTES)
        if not _valid_frame_for_exit(frame, exit_code):
            return 2
        return exit_code
    except ProbeSdkError:
        raise
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        raise ProbeSdkError("PROBE_OUTPUT_QUARANTINE_REQUIRED") from None
    finally:
        if frame_fd is not None:
            try:
                os.close(frame_fd)
            except OSError:
                pass
        if directory_fd is not None:
            try:
                os.close(directory_fd)
            except OSError:
                pass


def main(argv: Sequence[str] | None = None) -> int:
    """Parse fixed capture arguments without ever echoing operator values."""
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    try:
        if (
            len(arguments) != (len(_FRAME_ARGUMENT_NAMES) + len(PROBE_CLI_ARGUMENT_NAMES))
            * 2
            or tuple(arguments[:4:2]) != _FRAME_ARGUMENT_NAMES
        ):
            return 2
        operator_dir = Path(arguments[1])
        frame_basename = arguments[3]
        return capture_probe_result_frame(
            operator_dir,
            frame_basename,
            arguments[4:],
        )
    except (KeyboardInterrupt, SystemExit):
        return 130
    except BaseException:
        return 2


def run() -> None:
    raise SystemExit(main())


def publisher_key_preflight_main(argv: Sequence[str] | None = None) -> int:
    """Validate the protected publisher seed and independent pin without networking."""
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    if arguments:
        return 2
    try:
        reject_unsafe_provider_environment()
        private_path = Path(os.environ["FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE"])
        public_pin = os.environ[
            "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U"
        ]
        private_key = load_probe_signing_key(private_path, public_pin)
        del private_key
        _write_fixed_stdout(b"publisher_key_preflight=pass\n")
        return 0
    except BaseException:
        return 2


def key_preflight_run() -> None:
    raise SystemExit(publisher_key_preflight_main())


def _run_probe_cli(file_descriptor: int, arguments: tuple[str, ...]) -> int:
    from nexus_fosun_gateway import probe_cli

    return probe_cli.main(arguments, output_fd=file_descriptor)


def _write_fixed_stdout(content: bytes) -> None:
    view = memoryview(content)
    offset = 0
    while offset < len(view):
        try:
            written = os.write(1, view[offset:])
        except InterruptedError:
            continue
        if (
            not isinstance(written, int)
            or isinstance(written, bool)
            or not 1 <= written <= len(view) - offset
        ):
            raise OSError("short preflight status write")
        offset += written


def _is_protected_frame(metadata: os.stat_result) -> bool:
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_uid == os.geteuid()
        and stat.S_IMODE(metadata.st_mode) == 0o600
        and metadata.st_nlink == 1
        and 0 <= metadata.st_size <= _MAX_FRAME_BYTES
    )


def _valid_frame_for_exit(frame: bytes, exit_code: int) -> bool:
    if (
        not frame
        or len(frame) > _MAX_FRAME_BYTES
        or not frame.endswith(b"\n")
        or frame.count(b"\n") != 1
        or exit_code not in {0, 2, 130}
    ):
        return False
    try:
        document = json.loads(frame)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(document, dict) or tuple(document) != PROBE_RESULT_FIELD_NAMES:
        return False
    status = document.get("status")
    code = document.get("code")
    if exit_code == 0:
        return (
            status == "probe_published"
            and code == "OK"
            and all(
                document[name] is None
                for name in (
                    "failure_stage",
                    "failure_operation",
                    "failure_category",
                    "physical_request_count",
                )
            )
        )
    if (
        status != "probe_failed"
        or document.get("failure_stage") != "probe"
        or document.get("failure_operation") not in PROBE_FAILURE_OPERATIONS
        or document.get("failure_category") not in PROBE_FAILURE_CATEGORIES
        or not isinstance(document.get("physical_request_count"), int)
        or isinstance(document.get("physical_request_count"), bool)
        or not 0 <= document["physical_request_count"] <= 24
        or any(
            document[name] is not None
            for name in (
                "artifact_sha256",
                "byte_length",
                "capability_expires_at",
                "activation_contract_hash",
                "publication_receipt",
            )
        )
    ):
        return False
    if exit_code == 130:
        return code == "PROBE_INTERRUPTED"
    return isinstance(code, str) and bool(code)


if __name__ == "__main__":
    run()
