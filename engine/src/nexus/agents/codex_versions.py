"""Exact SDK runtime versions; a budget revision does not upgrade research semantics."""

CONFIG_VERSION = "codex-sdk-chatgpt-v1"
STAGED_CONFIG_VERSION = "codex-sdk-chatgpt-v2"
STRUCTURED_CONFIG_VERSION = "codex-sdk-chatgpt-v3"
STRUCTURED_96K_CONFIG_VERSION = "codex-sdk-chatgpt-v3-input96k"
STRUCTURED_CONFIG_VERSIONS = frozenset({STRUCTURED_CONFIG_VERSION, STRUCTURED_96K_CONFIG_VERSION})
STAGED_CONFIG_VERSIONS = STRUCTURED_CONFIG_VERSIONS | {STAGED_CONFIG_VERSION}
SDK_CONFIG_VERSIONS = STAGED_CONFIG_VERSIONS | {CONFIG_VERSION}


def aggregate_input_limit(version: str) -> int:
    if version not in SDK_CONFIG_VERSIONS:
        raise ValueError("CODEX_CONFIG_VERSION_INVALID")
    return 96000 if version == STRUCTURED_96K_CONFIG_VERSION else 64000
