"""Archive the fixed ISO MIC publication page and CSV, without business writes."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from nexus.ops.local_recovery import write_private
from nexus.providers.http import (
    AllowedOrigin,
    FixedOriginHttpClient,
    HttpPolicyError,
    HttpRequestPolicy,
)

INDEX_URL = "https://www.iso20022.org/market-identifier-codes"
CSV_URL = "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"


async def collect_mic(*, output_dir: Path, client: httpx.AsyncClient | None = None) -> None:
    """Two fixed GETs once each; failure never permits an automatic second attempt."""
    try:
        output_dir.mkdir(mode=0o700)
    except FileExistsError:
        raise ValueError("MIC_COLLECTION_NEW_DIRECTORY_REQUIRED") from None
    owned = client is None
    transport = client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    bounded = FixedOriginHttpClient(
        provider_name="iso_mic",
        origin=AllowedOrigin(scheme="https", host="www.iso20022.org"),
        policy=HttpRequestPolicy(max_attempts=1),
        client=transport,
    )
    stage = "publication_fetch"
    statuses: list[int] = []

    async def record_status(response: httpx.Response) -> None:
        statuses.append(response.status_code)

    transport.event_hooks["response"].append(record_status)
    try:
        for filename, url, stage in (
            ("publication.html", INDEX_URL, "publication_fetch"),
            ("ISO10383_MIC.csv", CSV_URL, "csv_fetch"),
        ):
            statuses.clear()
            raw = await bounded.get_bytes(tuple(urlsplit(url).path.lstrip("/").split("/")), (), ())
            if statuses != [200]:
                raise HttpPolicyError("STATUS_REJECTED")
            received = datetime.now(UTC)
            stage = filename + "_archive"
            write_private(output_dir / filename, raw)
            if (output_dir / filename).read_bytes() != raw:
                raise ValueError("MIC_COLLECTION_READBACK_MISMATCH")
            receipt = {
                "source_url": url,
                "received_at": received.isoformat(),
                "sha256": sha256(raw).hexdigest(),
                "byte_length": len(raw),
                "http_status": statuses[0],
            }
            write_private(
                output_dir / (filename + ".receipt.json"),
                json.dumps(receipt, sort_keys=True).encode(),
            )
        write_private(output_dir / "complete.json", b'{"status":"MIC_ORIGINALS_ARCHIVED"}')
    except Exception as error:
        allowed = {
            "STATUS_REJECTED",
            "REDIRECT_REJECTED",
            "RESPONSE_TOO_LARGE",
            "DEADLINE_EXCEEDED",
            "RETRY_BUDGET_EXHAUSTED",
        }
        code = (
            "MIC_COLLECTION_" + str(error)
            if isinstance(error, HttpPolicyError) and str(error) in allowed
            else "MIC_COLLECTION_FAILED"
        )
        write_private(
            output_dir / "failure.json",
            json.dumps({"stage": stage, "code": code}, sort_keys=True).encode(),
        )
        raise ValueError(code) from None
    finally:
        transport.event_hooks["response"].remove(record_status)
        if owned:
            await transport.aclose()
