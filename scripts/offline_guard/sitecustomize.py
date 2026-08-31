"""Auto-install the guard only when CI opts into offline provider execution."""

from __future__ import annotations

import os

from nexus_offline_guard import install_socket_guard

if os.environ.get("NEXUS_OFFLINE_SOCKET_GUARD") == "1":
    install_socket_guard()
    # PYTHONPATH is used only to bootstrap this CI guard. Remove it before any
    # Provider code can evaluate its immutable-runtime environment contract.
    os.environ.pop("PYTHONPATH", None)
