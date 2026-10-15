"""A fresh CI checkout can archive controlled evidence only in private directories."""

import os
import stat

import pytest
from helpers.controlled_browser import _screenshot_directory

REVISION = "a" * 40


def _parts(formal=True):
    return (
        ".secrets",
        "formal-page-readback-20261010" if formal else "candidate-page-readback-20261010",
        REVISION,
        "count-3",
    )


@pytest.mark.parametrize("formal", [False, True])
def test_fresh_checkout_creates_private_parent_and_every_archive_component(tmp_path, formal):
    result = _screenshot_directory(tmp_path, REVISION, 3, formal_readback=formal)
    assert result == tmp_path.joinpath(*_parts(formal))
    current = tmp_path
    for part in _parts(formal):
        current /= part
        info = current.lstat()
        assert stat.S_ISDIR(info.st_mode)
        assert stat.S_IMODE(info.st_mode) == 0o700
        assert info.st_uid == os.getuid()
    # Reusing a valid archive keeps existing evidence, rather than replacing it.
    evidence = result / "existing.txt"
    evidence.write_text("controlled evidence")
    assert _screenshot_directory(tmp_path, REVISION, 3, formal_readback=formal) == result
    assert evidence.read_text() == "controlled evidence"


@pytest.mark.parametrize("level", range(4))
@pytest.mark.parametrize("damage", ["public", "symlink", "file"])
def test_invalid_component_rejected_before_creating_children(tmp_path, level, damage):
    root = tmp_path / "checkout"
    root.mkdir()
    current = root
    for part in _parts()[:level]:
        current /= part
        current.mkdir(mode=0o700)
    bad = current / _parts()[level]
    external = tmp_path / "external"
    external.mkdir(mode=0o700)
    if damage == "public":
        bad.mkdir(mode=0o755)
    elif damage == "symlink":
        bad.symlink_to(external, target_is_directory=True)
    else:
        bad.write_text("preserve this file")
    before = bad.lstat()
    with pytest.raises(AssertionError, match="^CONTROLLED_SCREENSHOT_DIRECTORY_REJECTED$"):
        _screenshot_directory(root, REVISION, 3, formal_readback=True)
    assert bad.lstat() == before
    assert list(external.iterdir()) == []
    if damage == "public":
        assert list(bad.iterdir()) == []
    elif damage == "file":
        assert bad.read_text() == "preserve this file"


def test_foreign_owner_parent_rejected_before_writing_archive(tmp_path, monkeypatch):
    secrets = tmp_path / ".secrets"
    secrets.mkdir(mode=0o700)
    actual_owner = secrets.stat().st_uid
    monkeypatch.setattr(os, "getuid", lambda: actual_owner + 1)
    with pytest.raises(AssertionError, match="^CONTROLLED_SCREENSHOT_DIRECTORY_REJECTED$"):
        _screenshot_directory(tmp_path, REVISION, 3, formal_readback=True)
    assert list(secrets.iterdir()) == []
