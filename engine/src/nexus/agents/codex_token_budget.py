"""Local-only payload counting and conservative explicit-request allowance.

This is not a complete runtime-harness token count or a billing limit.
Vocabulary preparation is a separate, explicit operator action.
"""

import base64
import hashlib
import importlib
import json
import os
import stat
from collections.abc import Callable, Mapping
from functools import lru_cache
from importlib import metadata
from pathlib import Path
from typing import Protocol

VOCABULARY_URL = "https://openaipublic.blob.core.windows.net/encodings/o200k_base.tiktoken"
VOCABULARY_SHA256 = "446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d"
MAX_VOCABULARY_BYTES = 4 * 1024 * 1024
INPUT_RESERVE = 8192

# o200k_base configuration copied from tiktoken 0.14.0 openai_public.py.
# Copyright (c) 2022 OpenAI, MIT License: https://github.com/openai/tiktoken/blob/0.14.0/LICENSE
_PATTERN = "|".join(
    [
        r"[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]*[\p{Ll}\p{Lm}\p{Lo}\p{M}]+(?i:'s|'t|'re|'ve|'m|'ll|'d)?",
        r"[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]+[\p{Ll}\p{Lm}\p{Lo}\p{M}]*(?i:'s|'t|'re|'ve|'m|'ll|'d)?",
        r"\p{N}{1,3}",
        r" ?[^\s\p{L}\p{N}]+[\r\n/]*",
        r"\s*[\r\n]+",
        r"\s+(?!\S)",
        r"\s+",
    ]
)
_SPECIAL_TOKENS = {"<|endoftext|>": 199999, "<|endofprompt|>": 200018}


def tokenizer_cache_path() -> Path:
    return Path.home() / ".cache/nexus/tokenizers/o200k_base.tiktoken"


def valid_vocabulary(data: bytes) -> bool:
    return (
        len(data) <= MAX_VOCABULARY_BYTES and hashlib.sha256(data).hexdigest() == VOCABULARY_SHA256
    )


def local_vocabulary() -> bytes | None:
    """Validate the exact bounded bytes used for parsing, without following symlinks."""
    try:
        descriptor = os.open(tokenizer_cache_path(), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_VOCABULARY_BYTES:
                return None
            data = stream.read(MAX_VOCABULARY_BYTES + 1)
        return data if valid_vocabulary(data) else None
    except OSError:
        return None


class _Encoding(Protocol):
    def encode_ordinary(self, text: str) -> list[int]: ...


@lru_cache(maxsize=1)
def _validated_encoding(factory: Callable[..., _Encoding], data: bytes) -> _Encoding:
    """Reuse only the encoder built from exact, freshly validated public bytes."""
    ranks = {}
    for line in data.splitlines():
        token, rank = line.split()
        ranks[base64.b64decode(token, validate=True)] = int(rank)
    return factory(
        name="o200k_base",
        pat_str=_PATTERN,
        mergeable_ranks=ranks,
        special_tokens=_SPECIAL_TOKENS,
    )


def payload_input_units(text: str, *, model: str) -> int:
    """Count ordinary wire-text tokens, or conservatively fall back to UTF-8 bytes."""
    fallback = len(text.encode("utf-8"))
    try:
        if (
            model not in {"gpt-5.4-mini", "gpt-5.6-terra"}
            or metadata.version("tiktoken") != "0.14.0"
        ):
            return fallback
        tiktoken = importlib.import_module("tiktoken")
        data = local_vocabulary()
        if data is None:
            return fallback
        encoding = _validated_encoding(tiktoken.Encoding, data)
        return len(encoding.encode_ordinary(text))
    except Exception:
        # No download/alternate encoding, and no exception text containing inputs.
        return fallback


def input_preflight_units(
    text: str,
    *,
    model: str,
    output_schema: Mapping[str, object],
    base_instructions: str,
    developer_instructions: str,
) -> int:
    """Payload count plus explicit schema/instruction bytes and the existing reserve."""
    schema = json.dumps(output_schema, ensure_ascii=False, separators=(",", ":"))
    return (
        payload_input_units(text, model=model)
        + len(schema.encode("utf-8"))
        + len(base_instructions.encode("utf-8"))
        + len(developer_instructions.encode("utf-8"))
        + INPUT_RESERVE
    )
