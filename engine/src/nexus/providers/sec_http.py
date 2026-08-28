"""Fixed-path transport for SEC Company Facts and submissions evidence."""

from __future__ import annotations

import re
from typing import Protocol

from nexus.providers.sec import SecProviderError

_COMPANY_FACTS = re.compile(r"^/api/xbrl/companyfacts/CIK(?P<cik>[0-9]{10})\.json$")
_CURRENT_SUBMISSIONS = re.compile(r"^/submissions/CIK(?P<cik>[0-9]{10})\.json$")
_ARCHIVED_SUBMISSIONS = re.compile(
    r"^/submissions/(?P<name>CIK[0-9]{10}-submissions-[0-9]{3}\.json)$"
)


class FixedOriginBytes(Protocol):
    async def get_bytes(
        self,
        path_segments: tuple[str, ...],
        query: tuple[tuple[str, str], ...],
        headers: tuple[tuple[str, str], ...],
    ) -> bytes: ...


class SecHttpTransport:
    """Translate only SEC paths already emitted by `SecCompanyFactsClient`."""

    def __init__(self, client: FixedOriginBytes) -> None:
        self._client = client

    async def get(self, path: str, headers: dict[str, str]) -> bytes:
        _validate_headers(headers)
        company_facts = _COMPANY_FACTS.fullmatch(path)
        current = _CURRENT_SUBMISSIONS.fullmatch(path)
        archived = _ARCHIVED_SUBMISSIONS.fullmatch(path)
        segments: tuple[str, ...]
        if company_facts is not None:
            segments = (
                "api",
                "xbrl",
                "companyfacts",
                f"CIK{company_facts.group('cik')}.json",
            )
        elif current is not None:
            segments = ("submissions", f"CIK{current.group('cik')}.json")
        elif archived is not None:
            segments = ("submissions", archived.group("name"))
        else:
            raise SecProviderError("SEC_PATH_NOT_ALLOWED")
        return await self._client.get_bytes(
            segments,
            (),
            tuple(sorted(headers.items())),
        )


def _validate_headers(headers: dict[str, str]) -> None:
    if set(headers) != {"User-Agent", "Accept-Encoding"}:
        raise SecProviderError("SEC_PATH_NOT_ALLOWED")
    user_agent = headers.get("User-Agent", "")
    if (
        not user_agent.startswith("NEXUS ")
        or "@" not in user_agent
        or len(user_agent.encode("utf-8")) > 260
        or any(ord(character) < 32 for character in user_agent)
        or headers.get("Accept-Encoding") != "gzip, deflate"
    ):
        raise SecProviderError("SEC_PATH_NOT_ALLOWED")
