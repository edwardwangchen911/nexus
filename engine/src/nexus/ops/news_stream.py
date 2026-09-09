"""One bounded, explicit Alpaca news subscription; never reconnects."""

import argparse
import asyncio
import json
import logging
import os
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import Any

from websockets.asyncio.client import connect

from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.providers.news import NewsError, _unique, aware, parse_news, validate_symbol

URL = "wss://stream.data.alpaca.markets/v1beta1/news"
SCHEMA = "nexus_news_stream_receipt_v1"
MAX_BYTES = 8 * 1024 * 1024
MAX_FRAME = 2 * 1024 * 1024


class _FixedConnect(connect):
    def process_redirect(self, exc: Exception) -> Exception:
        return exc  # Never forward authentication headers to a redirected origin.


def _messages(raw: bytes) -> list[dict[str, Any]]:
    try:
        value = json.loads(raw, object_pairs_hook=_unique)
        if not isinstance(value, list) or not value or any(not isinstance(x, dict) for x in value):
            raise ValueError
        return value
    except (ValueError, TypeError):
        raise NewsError("NEWS_STREAM_FRAME_INVALID") from None


def parse_stream_archive(receipt: dict[str, Any], frames: list[bytes]) -> dict[str, Any]:
    """Revalidate exact inbound originals; return observations, never REST qualification."""
    if receipt.get("schema") != SCHEMA or receipt.get("source") != "alpaca":
        raise NewsError("NEWS_STREAM_ARCHIVE_INVALID")
    symbol = validate_symbol(receipt["symbol"])
    start = aware(datetime.fromisoformat(receipt["started_at"]))
    end = aware(datetime.fromisoformat(receipt["ended_at"]))
    if (
        start > end
        or receipt.get("transport") != "websocket"
        or receipt.get("status") not in {"bounded_complete", "partial", "failed"}
    ):
        raise NewsError("NEWS_STREAM_ARCHIVE_INVALID")
    entries = receipt["frames"]
    if (
        len(entries) != len(frames)
        or len(frames) > 100
        or receipt.get("frame_count") != len(frames)
        or sum(map(len, frames)) > MAX_BYTES
    ):
        raise NewsError("NEWS_STREAM_ARCHIVE_INVALID")
    articles: list[Any] = []
    authenticated = subscribed = False
    for index, (entry, raw) in enumerate(zip(entries, frames, strict=True)):
        if (
            entry["file"] != f"frame-{index:05}.json"
            or entry["raw_sha256"] != sha256(raw).hexdigest()
            or entry["raw_byte_length"] != len(raw)
        ):
            raise NewsError("NEWS_STREAM_ARCHIVE_INVALID")
        received = aware(datetime.fromisoformat(entry["received_at"]))
        if not start <= received <= end:
            raise NewsError("NEWS_STREAM_ARCHIVE_INVALID")
        for item in _messages(raw):
            if item.get("T") == "success" and item.get("msg") == "authenticated":
                authenticated = True
            if item.get("T") == "subscription":
                if not authenticated or item.get("news") != [symbol]:
                    raise NewsError("NEWS_STREAM_SUBSCRIPTION_INVALID")
                subscribed = True
            if item.get("T") == "n":
                if not subscribed:
                    raise NewsError("NEWS_STREAM_SUBSCRIPTION_INVALID")
                articles.extend(
                    parse_news(
                        json.dumps({"news": [item]}).encode(), symbol=symbol, received_at=received
                    ).articles
                )
    if (
        receipt["authenticated"] != authenticated
        or receipt["subscription_confirmed"] != subscribed
        or receipt["article_count"] != len(articles)
    ):
        raise NewsError("NEWS_STREAM_ARCHIVE_INVALID")
    return {
        "articles": tuple(articles),
        "authenticated": authenticated,
        "subscription_confirmed": subscribed,
        "status": receipt["status"],
        "stop_reason": receipt["stop_reason"],
        "receipt": receipt,
        "frames": tuple(frames),
    }


def load_stream_archive(directory: Path) -> dict[str, Any]:
    """Load a private bounded stream archive without database or external calls."""
    from nexus.research.fmp_profile_archive import _read

    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if os.fstat(fd).st_mode & 0o077:
            raise NewsError("NEWS_STREAM_ARCHIVE_INVALID")
        receipt_raw = _read(fd, "receipt.json", 256 * 1024)
        receipt = json.loads(receipt_raw, object_pairs_hook=_unique)
        entries = receipt["frames"]
        if len(entries) > 100 or set(os.listdir(fd)) != {
            "receipt.json",
            *(f"frame-{i:05}.json" for i in range(len(entries))),
        }:
            raise NewsError("NEWS_STREAM_ARCHIVE_INVALID")
        frames = [_read(fd, f"frame-{i:05}.json", MAX_FRAME) for i in range(len(entries))]
    finally:
        os.close(fd)
    if sum(map(len, frames)) > MAX_BYTES:
        raise NewsError("NEWS_STREAM_ARCHIVE_INVALID")
    return {**parse_stream_archive(receipt, frames), "receipt_raw": receipt_raw}


async def collect_stream(
    *,
    symbol: str,
    directory: Path,
    key: str,
    secret: str,
    duration: float = 60,
    max_frames: int = 100,
    connector: Any = _FixedConnect,
) -> dict[str, Any]:
    validate_symbol(symbol)
    if not 0 < duration <= 300 or not 1 <= max_frames <= 100:
        raise NewsError("NEWS_STREAM_LIMIT_INVALID")
    if any(
        not x or not x.isascii() or any(not 33 <= ord(c) <= 126 for c in x) for x in (key, secret)
    ):
        raise NewsError("NEWS_STREAM_CREDENTIALS_INVALID")
    directory.mkdir(mode=0o700)
    started = datetime.now(UTC)
    deadline = monotonic() + duration
    receipt: dict[str, Any] = {
        "schema": SCHEMA,
        "source": "alpaca",
        "transport": "websocket",
        "symbol": symbol,
        "started_at": started.isoformat(),
        "authenticated": False,
        "subscription_confirmed": False,
        "status": "failed",
        "stop_reason": "CONNECT_FAILED",
        "frames": [],
        "article_count": 0,
    }
    byte_count = 0
    # Dedicated logger prevents websocket debug logs from exposing auth headers or frames.
    logger = logging.Logger("nexus.news_stream.private", level=logging.CRITICAL + 1)
    logger.propagate = False
    try:
        async with connector(
            URL,
            additional_headers={
                "APCA-API-KEY-ID": key,
                "APCA-API-SECRET-KEY": secret,
                "Content-Type": "application/json",
            },
            open_timeout=min(10, duration),
            close_timeout=1,
            max_size=MAX_FRAME,
            max_queue=1,
            compression=None,
            logger=logger,
        ) as socket:
            receipt["stop_reason"] = "DISCONNECTED"
            while len(receipt["frames"]) < max_frames:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError
                frame = await asyncio.wait_for(socket.recv(), timeout=remaining)
                raw = frame.encode() if isinstance(frame, str) else frame
                byte_count += len(raw)
                if byte_count > MAX_BYTES:
                    raise NewsError("BYTE_LIMIT")
                # Decode before any storage so escaped secret reflections are rejected too.
                messages = _messages(raw)
                decoded = json.dumps(messages, ensure_ascii=False)
                if any(x.encode() in raw or x in decoded for x in (key, secret)):
                    raise NewsError("SECRET_REFLECTION")
                received = datetime.now(UTC)
                name = f"frame-{len(receipt['frames']):05}.json"
                write_private(directory / name, raw)
                receipt["frames"].append(
                    {
                        "file": name,
                        "received_at": received.isoformat(),
                        "raw_sha256": sha256(raw).hexdigest(),
                        "raw_byte_length": len(raw),
                    }
                )
                for item in messages:
                    kind = item.get("T")
                    if kind == "error":
                        code = item.get("code")
                        raise NewsError(
                            f"PROVIDER_{code}"
                            if type(code) is int
                            and code in {400, 401, 402, 403, 404, 405, 406, 407, 409, 410, 500}
                            else "PROVIDER_ERROR"
                        )
                    if kind == "success" and item.get("msg") == "authenticated":
                        if receipt["authenticated"]:
                            raise NewsError("PROTOCOL_INVALID")
                        receipt["authenticated"] = True
                        await socket.send(json.dumps({"action": "subscribe", "news": [symbol]}))
                    elif kind == "subscription":
                        if not receipt["authenticated"] or item.get("news") != [symbol]:
                            raise NewsError("SUBSCRIPTION_INVALID")
                        receipt["subscription_confirmed"] = True
                    elif kind == "n":
                        if not receipt["subscription_confirmed"]:
                            raise NewsError("SUBSCRIPTION_INVALID")
                        parse_news(
                            json.dumps({"news": [item]}).encode(),
                            symbol=symbol,
                            received_at=received,
                        )
                        receipt["article_count"] += 1
                    elif kind != "success":
                        raise NewsError("PROTOCOL_INVALID")
            receipt["stop_reason"] = "FRAME_LIMIT"
    except TimeoutError:
        receipt["stop_reason"] = "DURATION_LIMIT"
    except NewsError as error:
        receipt["stop_reason"] = str(error)
    except Exception:
        pass  # No upstream exception text, close reason, headers, or credentials escape.
    receipt["status"] = (
        "bounded_complete"
        if receipt["subscription_confirmed"]
        and receipt["stop_reason"] in {"DURATION_LIMIT", "FRAME_LIMIT"}
        else "partial"
        if receipt["subscription_confirmed"]
        else "failed"
    )
    receipt["ended_at"] = datetime.now(UTC).isoformat()
    receipt["frame_count"] = len(receipt["frames"])
    write_private(directory / "receipt.json", json.dumps(receipt, sort_keys=True).encode())
    return {
        k: receipt[k]
        for k in (
            "status",
            "stop_reason",
            "authenticated",
            "subscription_confirmed",
            "article_count",
            "frame_count",
        )
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--max-frames", type=int, default=100)
    args = parser.parse_args()
    try:
        env = load_local_environment(args.repo_root, os.environ)
        result = asyncio.run(
            collect_stream(
                symbol=args.symbol,
                directory=args.output_dir,
                key=env.get("ALPACA_API_KEY_ID", ""),
                secret=env.get("ALPACA_API_SECRET_KEY", ""),
                duration=args.duration,
                max_frames=args.max_frames,
            )
        )
    except (Exception, KeyboardInterrupt):
        result = {"status": "failed", "stop_reason": "NEWS_STREAM_FAILED"}
    print(json.dumps(result))
    if result["status"] != "bounded_complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
