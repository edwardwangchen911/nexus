"""Local-stack test fixtures with a real, isolated Git ignore boundary."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


@pytest.fixture
def repo_root(tmp_path: Path) -> Path:
    """Return a minimal repository whose local env is ignored and untracked."""
    root = tmp_path / "repository"
    infra = root / "infra"
    infra.mkdir(parents=True)
    (root / ".gitignore").write_text(".env*\ninfra/.env*\n", encoding="utf-8")
    (infra / "compose.yaml").write_text("services: {}\n", encoding="utf-8")
    (infra / ".env.local").write_text("NEXUS_ENV=test\n", encoding="utf-8")
    subprocess.run(("git", "init", "--quiet", str(root)), check=True)
    subprocess.run(
        ("git", "-C", str(root), "add", ".gitignore", "infra/compose.yaml"),
        check=True,
    )
    return root
