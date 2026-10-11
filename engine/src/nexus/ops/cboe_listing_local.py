"""Archive exact Cboe US listed-symbol sources; no quote or business import."""

import json
import re
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


async def collect_listing(
    *, symbol: str, output_dir: Path, client: httpx.AsyncClient | None = None
) -> None:
    if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,15}", symbol):
        raise ValueError("CBOE_LISTING_SYMBOL_INVALID")
    try:
        output_dir.mkdir(mode=0o700)
    except FileExistsError:
        raise ValueError("CBOE_LISTING_NEW_DIRECTORY_REQUIRED") from None
    owned = client is None
    transport = client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    bounded = FixedOriginHttpClient(
        provider_name="cboe_listing",
        origin=AllowedOrigin(scheme="https", host="www.cboe.com"),
        policy=HttpRequestPolicy(max_attempts=1),
        client=transport,
    )
    root = "https://www.cboe.com/us/equities/"
    stage = "listed.html_fetch"
    statuses: list[int] = []

    async def record_status(response: httpx.Response) -> None:
        statuses.append(response.status_code)

    transport.event_hooks["response"].append(record_status)
    try:
        for filename, url in (
            ("listed.html", root + "market_statistics/listed_symbols/"),
            ("listed.csv", root + "market_statistics/listed_symbols/csv"),
            ("product.html", root + f"listings/listed_products/symbols/{symbol}/"),
        ):
            stage = filename + "_fetch"
            path = urlsplit(url).path
            statuses.clear()
            raw = await bounded.get_bytes(
                tuple(path.strip("/").split("/")), (), (), trailing_slash=path.endswith("/")
            )
            if statuses != [200]:
                raise HttpPolicyError("STATUS_REJECTED")
            received = datetime.now(UTC)
            stage = filename + "_archive"
            write_private(output_dir / filename, raw)
            if (output_dir / filename).read_bytes() != raw:
                raise ValueError("CBOE_LISTING_READBACK_MISMATCH")
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
        write_private(output_dir / "complete.json", b'{"status":"CBOE_LISTING_ORIGINALS_ARCHIVED"}')
    except Exception as error:
        allowed = {
            "STATUS_REJECTED",
            "REDIRECT_REJECTED",
            "RESPONSE_TOO_LARGE",
            "DEADLINE_EXCEEDED",
            "RETRY_BUDGET_EXHAUSTED",
        }
        code = (
            "CBOE_LISTING_" + str(error)
            if isinstance(error, HttpPolicyError) and str(error) in allowed
            else "CBOE_LISTING_FAILED"
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
