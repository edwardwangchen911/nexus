"""A second process cannot consume the intermediate hard-link publication state."""

import subprocess
import sys
import time

import pytest
from nexus.ops import platform_round_registration as registration
from nexus.ops.platform_credentials import PlatformCredentialError


def _crash_after_link(target):
    code = """
import os,sys
from pathlib import Path
from nexus.ops import platform_round_registration as m
original=m.os.link
def crash(*a,**kw):
    original(*a,**kw)
    os._exit(71)
m.os.link=crash
m._publish(Path(sys.argv[1]),b'original immutable content')
"""
    result = subprocess.run([sys.executable, "-c", code, str(target)], timeout=60)
    assert result.returncode == 71 and target.stat().st_nlink == 2


@pytest.mark.parametrize("filename", ["round.json", "registration-attempt.json"])
def test_hard_exit_after_link_recovers_exact_bytes_without_new_publication(tmp_path, filename):
    target = tmp_path / filename
    _crash_after_link(target)
    inode = target.stat().st_ino
    assert registration._private_bytes(target, 100) == b"original immutable content"
    assert target.stat().st_nlink == 1 and target.stat().st_ino == inode
    assert not registration._publish(target, b"must not replace or grant a new DB attempt")
    assert registration._private_bytes(target, 100) == b"original immutable content"


@pytest.mark.parametrize("damage", ["extra_alias", "changed_bytes", "public_permissions"])
def test_crashed_alias_recovery_keeps_unproven_files_untouched(tmp_path, damage):
    import os

    target = tmp_path / "round.json"
    _crash_after_link(target)
    if damage == "extra_alias":
        os.link(target, tmp_path / "unknown-link")
    elif damage == "changed_bytes":
        target.write_bytes(b"content does not match publication provenance")
    else:
        target.chmod(0o644)
    before = {
        p.name: (p.stat().st_ino, p.stat().st_nlink, p.read_bytes()) for p in tmp_path.iterdir()
    }
    with pytest.raises(PlatformCredentialError, match="PLATFORM_PROOF_UNSAFE"):
        registration._private_bytes(target, 100)
    after = {
        p.name: (p.stat().st_ino, p.stat().st_nlink, p.read_bytes()) for p in tmp_path.iterdir()
    }
    assert after == before


def test_unattributed_hard_link_is_preserved_and_rejected(tmp_path):
    import os

    target, unknown = tmp_path / "round.json", tmp_path / "unattributed-alias"
    assert registration._publish(target, b"original")
    os.link(target, unknown)
    with pytest.raises(PlatformCredentialError, match="PLATFORM_PROOF_UNSAFE"):
        registration._private_bytes(target, 100)
    assert unknown.read_bytes() == b"original" and target.stat().st_nlink == 2


def test_missing_managed_file_preserves_fixed_private_read_error(tmp_path):
    registration._publish(tmp_path / "existing.json", b"original")
    with pytest.raises(PlatformCredentialError, match="^PLATFORM_PROOF_UNSAFE$"):
        registration._private_bytes(tmp_path / "missing.json", 100)


def test_other_process_waits_for_single_link_private_publication(tmp_path):
    target, linked, release, attempted = [
        tmp_path / name for name in ("round.json", "linked", "release", "attempted")
    ]
    publisher_code = """
import sys,time
from pathlib import Path
from nexus.ops import platform_round_registration as m
target,linked,release=map(Path,sys.argv[1:])
original=m.os.link
def pause(*a,**kw):
    original(*a,**kw)
    linked.write_text('linked')
    deadline=time.monotonic()+60
    while not release.exists():
        if time.monotonic()>deadline: raise TimeoutError('synthetic publisher barrier')
        time.sleep(.01)
m.os.link=pause
assert m._publish(target,b'frozen')
"""
    reader_code = """
import fcntl,sys
from pathlib import Path
from nexus.ops import platform_round_registration as m
target,attempted=map(Path,sys.argv[1:])
original=fcntl.flock
def observe(fd,operation):
    try: return original(fd,operation)
    except BlockingIOError:
        attempted.write_text('blocked on actual publisher lock')
        raise
fcntl.flock=observe
assert m._private_bytes(target,100)==b'frozen'
"""
    publisher = subprocess.Popen(
        [sys.executable, "-c", publisher_code, str(target), str(linked), str(release)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    reader = None
    try:
        deadline = time.monotonic() + 60
        while not linked.exists() and publisher.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert linked.exists() and target.stat().st_nlink == 2
        reader = subprocess.Popen(
            [sys.executable, "-c", reader_code, str(target), str(attempted)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        while not attempted.exists() and reader.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert attempted.exists() and reader.poll() is None
        release.touch()
        assert publisher.communicate(timeout=60) == (b"", b"") and publisher.returncode == 0
        assert reader.communicate(timeout=60) == (b"", b"") and reader.returncode == 0
        assert target.stat().st_nlink == 1
    finally:
        release.touch()
        for process in (reader, publisher):
            if process is not None and process.poll() is None:
                process.terminate()
                process.communicate(timeout=10)
