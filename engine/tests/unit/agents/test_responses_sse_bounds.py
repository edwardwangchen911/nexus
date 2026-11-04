"""Isolated SSE wire limits, including the real local HTTP client and owned close."""

import asyncio
import json
from decimal import Decimal

import httpx
import pytest
from nexus.agents.http_failure import http_failure_facts
from nexus.agents.responses_request import send_response_stage
from nexus.agents.responses_stage_stream import collect_response_stage
from unit.agents.test_http_stage_lifecycle import terminal
from unit.agents.test_platform_usage import usage
from unit.agents.test_responses_stage_stream import response
from unit.agents.test_sol_pricing import sol_price

MIB = 1024 * 1024


class Fragments(httpx.AsyncByteStream):
    def __init__(self, data, step):
        self.data, self.step, self.closed = data, step, 0

    async def __aiter__(self):
        for i in range(0, len(self.data), self.step):
            yield self.data[i : i + self.step]

    async def aclose(self):
        self.closed += 1


def frame(event):
    return b"data: " + json.dumps(event).encode() + b"\r\n\r\n"


@pytest.mark.asyncio
@pytest.mark.parametrize("step", [37, 65536])
@pytest.mark.parametrize("extra", [-1, 0, 1])
async def test_wire_event_bound_includes_framing_and_is_independent_of_fragmentation(step, extra):
    value = terminal()
    value["response"]["instructions"] = ""
    value["response"]["instructions"] = "x" * (MIB + extra - len(frame(value)))
    data = frame(value)
    assert len(data) == MIB + extra
    body = Fragments(data, step)
    now = asyncio.get_running_loop().time()
    result = await collect_response_stage(
        response(body), work_deadline=now + 10, cleanup_deadline=now + 15
    )
    assert result.cleanup.confirmed and body.closed == 1
    if extra > 0:
        assert str(result.error) == "RESPONSES_EVENT_TOO_LARGE"
        assert result.facts.usage is None
    else:
        assert result.error is None and result.facts.usage == (20, 9)


@pytest.mark.asyncio
@pytest.mark.parametrize("post_terminal", [False, True])
async def test_total_wire_limit_counts_keepalives_and_retains_only_observed_usage(post_terminal):
    comment = b":" + b"x" * (MIB - 3) + b"\n\n"
    body = Fragments((frame(terminal()) if post_terminal else b"") + comment * 16 + b"\n", 65536)
    now = asyncio.get_running_loop().time()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: response(body)))
    request = client.build_request("POST", "https://example.invalid/responses")
    observed = await send_response_stage(
        client, request, work_deadline=now + 10, cleanup_deadline=now + 15
    )
    assert str(observed.error) == "RESPONSES_STREAM_TOO_LARGE"
    assert observed.stage.facts.usage == ((20, 9) if post_terminal else None)
    persisted = http_failure_facts(observed, observed.error)
    assert persisted.phase == "stream" and persisted.protocol_code == "RESPONSES_EVENT_TOO_LARGE"
    assert persisted.terminal_seen == post_terminal
    assert body.closed == 1 and observed.cleanup.confirmed and client.is_closed


@pytest.mark.asyncio
async def test_large_intermediate_and_terminal_over_loopback_keep_original_charge_and_close():
    value = terminal()
    receipt = value["response"]
    receipt.update(
        model="gpt-6.1-sol", service_tier="default", usage=usage(), instructions="x" * 150000
    )
    item = dict(
        type="reasoning",
        id="rs_fixture",
        status="completed",
        summary=[],
        encrypted_content="y" * 140000,
    )
    receipt["output"].insert(0, item)
    data = frame(
        dict(type="response.output_item.done", output_index=0, sequence_number=1, item=item)
    ) + frame(value)
    calls = []

    async def serve(reader, writer):
        try:
            calls.append(await reader.readuntil(b"\r\n\r\n"))
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n"
            )
            for i in range(0, len(data), 4000):
                writer.write(data[i : i + 4000])
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async with await asyncio.start_server(serve, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = httpx.AsyncClient(trust_env=False)
        request = client.build_request("POST", f"http://127.0.0.1:{port}/responses")
        now = asyncio.get_running_loop().time()
        observed = await send_response_stage(
            client, request, work_deadline=now + 10, cleanup_deadline=now + 15
        )
    assert len(calls) == 1 and observed.cleanup.confirmed and client.is_closed
    assert observed.error is None
    assert observed.stage.terminal == receipt
    price = sol_price(
        observed.stage.terminal["usage"],
        model=receipt["model"],
        service_tier=receipt["service_tier"],
    )
    assert (
        price.ordinary_input_tokens,
        price.cached_tokens,
        price.cache_write_tokens,
        price.output_tokens,
    ) == (1000, 4000, 7000, 1000)
    assert price.exact_cost_usd == Decimal("0.0299")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "suffix,expected",
    [
        (b'data: {"type":"response.completed"', "RESPONSES_STREAM_TRUNCATED"),
        (b"data: [DONE]\n\n", "RESPONSES_TERMINAL_MISSING"),
    ],
)
async def test_large_intermediate_never_supplies_missing_terminal_usage(suffix, expected):
    body = Fragments(
        frame({"type": "response.output_text.done", "text": "x" * 140000}) + suffix, 77
    )
    now = asyncio.get_running_loop().time()
    result = await collect_response_stage(
        response(body), work_deadline=now + 5, cleanup_deadline=now + 10
    )
    assert str(result.error) == expected
    assert result.terminal is None and result.facts.usage is None
    assert result.cleanup.confirmed and body.closed == 1
