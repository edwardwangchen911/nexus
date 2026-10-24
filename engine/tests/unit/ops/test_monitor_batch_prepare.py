"""Collection archives never relax the existing private source-file boundary."""

import os
import stat

import pytest
from nexus.ops.monitor_batch_prepare import _archive_root


def test_private_archive_root_is_idempotent_and_preserves_files(tmp_path):
    root = _archive_root(tmp_path)
    assert root == tmp_path / ".secrets" / "research-monitor-collections"
    assert root.stat().st_uid == os.getuid()
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    saved = root / "existing"
    saved.write_bytes(b"untouched original")
    assert _archive_root(tmp_path) == root
    assert saved.read_bytes() == b"untouched original"


@pytest.mark.parametrize("mode", ["public", "symlink", "file"])
def test_unsafe_archive_parent_is_rejected_without_repair(tmp_path, mode):
    secret = tmp_path / ".secrets"
    if mode == "public":
        secret.mkdir(mode=0o755)
    elif mode == "symlink":
        secret.symlink_to(tmp_path, target_is_directory=True)
    else:
        secret.write_bytes(b"not a directory")
    with pytest.raises(ValueError, match="MONITOR_COLLECTION_DIRECTORY_INVALID"):
        _archive_root(tmp_path)
    if mode == "public":
        assert stat.S_IMODE(secret.stat().st_mode) == 0o755
