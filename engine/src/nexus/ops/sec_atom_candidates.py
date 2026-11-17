"""Once-bucket official candidate custody before the existing authoritative refresh.

This publisher has no event, outbox, Job, budget or research writer. A feed hint
can only precede the unchanged primary SEC capture/eligibility path.
"""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import parse_qsl, urlsplit
from uuid import UUID, uuid4

import httpx
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert

from nexus.audit.service import append_audit_event
from nexus.monitoring.durable_sec_host import current_identity
from nexus.monitoring.durable_sec_storage import _lock, rule_state
from nexus.ops.local_recovery import write_private
from nexus.ops.monitor_batch_prepare import _archive_root
from nexus.ops.monitor_content_local import _contact
from nexus.ops.monitor_disclosure_refresh import _bucket, _client, _result, _scope
from nexus.ops.research_identity_local import _Exact200Client
from nexus.paper.account_transaction_turn import account_transaction_turn
from nexus.paper.host_probe_state import require_host_admitting
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.models.sec_monitor import SecMonitorRule
from nexus.persistence.session import session_factory
from nexus.providers.http import AllowedOrigin, HttpPolicyError, HttpRequestPolicy
from nexus.research.sec_atom_candidates import MAX_BYTES, parse_atom, source_url
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.settings import EngineSettings

SOURCE = "sec_official_atom_candidates"
SCHEMA = "sec_atom_candidates_v1"
REQUEST_TIMEOUT_SECONDS = 10
CLAIM = "research.monitor.sec_atom_candidates_claimed"
log = logging.getLogger(__name__)


def _failure_code(error: BaseException) -> str:
    if isinstance(error, asyncio.CancelledError):
        return "cancelled"
    allowed = {
        "SEC_ATOM_SOURCE_INVALID",
        "SEC_ATOM_CUSTODY_INVALID",
        "SEC_ATOM_SCOPE_CHANGED",
        "SEC_ATOM_CLOCK_INVALID",
        "SEC_ATOM_CONTACT_REFLECTION",
        "SEC_ATOM_BUCKET_EXPIRED",
        "DEADLINE_EXCEEDED",
        "STATUS_REJECTED",
        "RETRY_BUDGET_EXHAUSTED",
        "REDIRECT_REJECTED",
        "RESPONSE_TOO_LARGE",
    }
    return (
        str(error)
        if isinstance(error, ValueError | HttpPolicyError) and str(error) in allowed
        else "unverified"
    )


class _SendDeadline:
    """Guard again after pool/TLS/original trace callbacks, before real headers."""

    def __init__(self, deadline: float, permit: Callable[[], Awaitable[None]]):
        self.deadline, self.permit = deadline, permit

    def require_time(self) -> None:
        if asyncio.get_running_loop().time() >= self.deadline:
            raise TimeoutError("SEC_ATOM_BUCKET_EXPIRED")

    async def attach(self, request: httpx.Request) -> None:
        self.require_time()
        await self.permit()
        self.require_time()
        original = request.extensions.get("trace")

        async def traced(name: str, info: dict[str, Any]) -> None:
            if original is not None:
                await original(name, info)
            if name in {
                "http11.send_request_headers.started",
                "http2.send_request_headers.started",
            }:
                self.require_time()
                await self.permit()
                self.require_time()

        request.extensions["trace"] = traced


def _quarantine(repo_root: Path, operation: str, raw: bytes, receipt: bytes) -> None:
    archive = _archive_root(repo_root) / ("atom-" + operation)
    archive.mkdir(mode=0o700)
    write_private(archive / "feed.xml", raw)
    write_private(archive / "receipt.json", receipt)


async def refresh_candidates(
    settings: EngineSettings,
    *,
    calendar_authority_id: UUID,
    scheduled_at: datetime | None = None,
    environment: dict[str, str],
    repo_root: Path,
) -> dict[str, object]:
    if (
        not settings.agent_paper_cycle_enabled
        or settings.agent_paper_account_id is None
        or settings.agent_paper_formal_model_config_id is None
    ):
        return _result("disabled")
    factory = session_factory(settings)
    account_id = settings.agent_paper_account_id
    items: list[dict[str, object]] = []
    requests = 0
    try:
        async with account_transaction_turn(settings), factory() as session, session.begin():
            if scheduled_at is None:
                at = await session.scalar(select(func.clock_timestamp()))
                if not isinstance(at, datetime):
                    raise ValueError("SEC_ATOM_CLOCK_INVALID")
                scheduled_at = _bucket(at)
            _, instruments = await _scope(
                session, settings, account_id, calendar_authority_id, scheduled_at
            )
            await require_host_admitting(session, account_id)
            rules = tuple(
                await session.scalars(
                    select(SecMonitorRule).where(
                        SecMonitorRule.account_id == account_id,
                        SecMonitorRule.instrument_id.in_(instruments),
                    )
                )
            )
        for rule in rules:
            async with account_transaction_turn(settings), factory() as session, session.begin():
                now, instruments = await _scope(
                    session, settings, account_id, calendar_authority_id, scheduled_at
                )
                await require_host_admitting(session, account_id)
                await _lock(session, rule)
                if (
                    rule.instrument_id not in instruments
                    or await rule_state(session, rule) != "active"
                ):
                    continue
                prior = await session.scalar(
                    select(AuditEvent.id)
                    .where(
                        AuditEvent.action == CLAIM,
                        AuditEvent.payload["rule_id"].astext == str(rule.id),
                        AuditEvent.payload["scheduled_at"].astext == scheduled_at.isoformat(),
                    )
                    .limit(1)
                )
                if prior is not None:
                    continue
                identity_id = await current_identity(session, rule, now)
                sampled = asyncio.get_running_loop().time()
                now, instruments = await _scope(
                    session, settings, account_id, calendar_authority_id, scheduled_at
                )
                send_deadline = (
                    sampled + (scheduled_at + timedelta(minutes=5) - now).total_seconds()
                )
                if rule.instrument_id not in instruments:
                    continue
                contact = environment.get("SEC_CONTACT", "")
                _contact(contact)
                claim: dict[str, object] = {
                    "rule_id": str(rule.id),
                    "identity_snapshot_id": str(identity_id),
                    "scheduled_at": scheduled_at.isoformat(),
                    "claimed_at": now.isoformat(),
                    "operation_id": str(uuid4()),
                }
                await append_audit_event(
                    session,
                    actor_id="research-monitor",
                    action=CLAIM,
                    schema_version=SCHEMA,
                    occurred_at=now,
                    payload=claim,
                )
            # Never hold a DB transaction during public HTTP. An ambiguous or
            # failed collection occupies this bucket and is not retried.
            stage = "request_setup"
            started = monotonic()
            try:
                url = source_url(rule.issuer_cik)
                parts = urlsplit(url)
                requests += 1

                async def permit() -> None:
                    async with factory() as read, read.begin():
                        await read.execute(text("SET TRANSACTION READ ONLY"))
                        await read.execute(text("SET LOCAL statement_timeout='2s'"))
                        at = await read.scalar(select(func.clock_timestamp()))
                        if not isinstance(at, datetime) or _bucket(at) != scheduled_at:
                            raise ValueError("SEC_ATOM_BUCKET_EXPIRED")

                fence = _SendDeadline(send_deadline, permit)
                async with (
                    asyncio.timeout_at(send_deadline),
                    asyncio.timeout(REQUEST_TIMEOUT_SECONDS),
                    _client() as client,
                ):
                    client.event_hooks["request"].append(fence.attach)
                    transport = _Exact200Client(
                        provider_name="sec",
                        origin=AllowedOrigin("https", "www.sec.gov"),
                        policy=HttpRequestPolicy(
                            deadline_seconds=REQUEST_TIMEOUT_SECONDS,
                            max_attempts=1,
                            max_response_bytes=MAX_BYTES,
                        ),
                        client=client,
                    )
                    stage = "public_http"
                    raw = await transport.get_bytes(
                        ("cgi-bin", "browse-edgar"),
                        tuple(parse_qsl(parts.query)),
                        (("User-Agent", f"NEXUS {contact}"),),
                    )
                if contact.encode() in raw:
                    raise ValueError("SEC_ATOM_CONTACT_REFLECTION")
                received = datetime.now(UTC)
                receipt = json.dumps(
                    {
                        "claim": claim,
                        "source_url": url,
                        "received_at": received.isoformat(),
                        "sha256": sha256(raw).hexdigest(),
                        "byte_length": len(raw),
                        "http_status": 200,
                    },
                    sort_keys=True,
                ).encode()
                stage = "quarantine"
                await asyncio.to_thread(
                    _quarantine, repo_root, str(claim["operation_id"]), raw, receipt
                )
                stage = "parse"
                observation = parse_atom(raw, issuer_cik=rule.issuer_cik, received_at=received)
                stage = "publication"
                async with (
                    account_transaction_turn(settings),
                    factory() as session,
                    session.begin(),
                ):
                    now, instruments = await _scope(
                        session, settings, account_id, calendar_authority_id, scheduled_at
                    )
                    await require_host_admitting(session, account_id)
                    await _lock(session, rule)
                    if (
                        rule.instrument_id not in instruments
                        or await rule_state(session, rule) != "active"
                        or identity_id != await current_identity(session, rule, now)
                    ):
                        raise ValueError("SEC_ATOM_SCOPE_CHANGED")
                    store = configured_exhibit_store(session)
                    originals = []
                    for data, mime, schema, suffix in (
                        (raw, "application/atom+xml", "sec_atom_raw_v1", "xml"),
                        (receipt, "application/json", "sec_atom_receipt_v1", "json"),
                    ):
                        digest = sha256(data).hexdigest()
                        uri = await store.put_if_absent(
                            key=f"raw/sec/atom-candidates/{digest}.{suffix}", content=data
                        )
                        await session.execute(
                            insert(DataArtifact)
                            .values(
                                uri=uri,
                                sha256=digest,
                                byte_size=len(data),
                                mime_type=mime,
                                schema_version=schema,
                                source="sec",
                                observed_at=received,
                                available_at=now,
                                received_at=now,
                            )
                            .on_conflict_do_nothing(index_elements=[DataArtifact.sha256])
                        )
                        artifact = await session.scalar(
                            select(DataArtifact).where(DataArtifact.sha256 == digest)
                        )
                        if artifact is None or (
                            artifact.uri,
                            artifact.byte_size,
                            artifact.mime_type,
                            artifact.schema_version,
                            artifact.source,
                        ) != (uri, len(data), mime, schema, "sec"):
                            raise ValueError("SEC_ATOM_CUSTODY_INVALID")
                        originals.append(
                            {
                                "artifact_id": str(artifact.id),
                                "uri": artifact.uri,
                                "sha256": digest,
                                "byte_size": len(data),
                            }
                        )
                    published_at = await session.scalar(select(func.clock_timestamp()))
                    if (
                        not isinstance(published_at, datetime)
                        or not datetime.fromisoformat(str(claim["claimed_at"]))
                        <= received
                        <= published_at
                        or _bucket(datetime.fromisoformat(str(claim["claimed_at"]))) != scheduled_at
                    ):
                        raise ValueError("SEC_ATOM_CLOCK_INVALID")
                    payload = {
                        "schema_version": SCHEMA,
                        "rule_id": str(rule.id),
                        "instrument_id": str(rule.instrument_id),
                        "identity_snapshot_id": str(identity_id),
                        "source_url": url,
                        "observation": observation.model_dump(mode="json"),
                        "originals": originals,
                        "claim": claim,
                    }
                    revision = sha256(
                        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
                    ).hexdigest()
                    row = DataSnapshot(
                        source=SOURCE,
                        schema_version=SCHEMA,
                        provider_object_id=f"{rule.instrument_id}:{sha256(receipt).hexdigest()}",
                        revision=revision,
                        observed_at=received,
                        available_at=published_at,
                        received_at=published_at,
                        payload=payload,
                    )
                    session.add(row)
                    await session.flush()
                    await append_audit_event(
                        session,
                        actor_id="research-monitor",
                        action="research.monitor.sec_atom_candidates_received",
                        schema_version=SCHEMA,
                        occurred_at=published_at,
                        payload={
                            "snapshot_id": str(row.id),
                            "snapshot_hash": revision,
                            **claim,
                            "negative_conclusion_authorized": False,
                        },
                    )
                    # updated_at is only a candidate hint; no accepted_at is made
                    # from it. The caller next runs the unchanged primary refresh.
                    hints = [
                        e.accession_number
                        for e in observation.entries
                        if e.source_updated_at > rule.effective_at
                    ]
                    completed = {
                        "snapshot_id": str(row.id),
                        "instrument_id": str(rule.instrument_id),
                        "candidate_hints": hints,
                        "negative_conclusion_authorized": False,
                        "primary_confirmation_required": True,
                    }
                items.append(completed)
            except BaseException as error:
                # Fixed vocabulary only: no URL, headers, response text or credentials.
                code = _failure_code(error)
                log.warning(
                    "sec_atom_candidate_unverified stage=%s elapsed_ms=%d code=%s",
                    stage,
                    int((monotonic() - started) * 1000),
                    code,
                )
                if not isinstance(error, Exception):
                    raise
                items.append(
                    {
                        "status": "unknown",
                        "reason": "SEC_ATOM_CANDIDATES_UNVERIFIED",
                        "negative_conclusion_authorized": False,
                    }
                )
        return _result(
            "observed",
            provider_requests=requests,
            items=items,
            negative_conclusion_authorized=False,
        )
    finally:
        await factory.kw["bind"].dispose()


async def inspect_snapshot(settings: EngineSettings, snapshot_id: UUID) -> dict[str, object]:
    """Read exact saved hints without refreshing, changing a rule or granting effects."""
    from nexus.research.sec_atom_candidates import AtomObservation
    from nexus.research.sec_exhibit_storage import _read_identity
    from nexus.research.sec_index_storage import IndexArchiveBinding, _original, _receipt

    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            now = await session.scalar(select(func.clock_timestamp()))
            row = await session.get(DataSnapshot, snapshot_id)
            if (
                not isinstance(now, datetime)
                or row is None
                or (row.source, row.schema_version) != (SOURCE, SCHEMA)
                or max(row.created_at, row.available_at, row.received_at, row.observed_at) > now
            ):
                raise ValueError("SEC_ATOM_CAPTURE_UNAVAILABLE")
            p = row.payload
            if (
                set(p)
                != {
                    "schema_version",
                    "rule_id",
                    "instrument_id",
                    "identity_snapshot_id",
                    "source_url",
                    "observation",
                    "originals",
                    "claim",
                }
                or sha256(json.dumps(p, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                != row.revision
            ):
                raise ValueError("SEC_ATOM_CUSTODY_INVALID")
            claim = p["claim"]
            if (
                not isinstance(claim, dict)
                or claim.get("rule_id") != p["rule_id"]
                or claim.get("identity_snapshot_id") != p["identity_snapshot_id"]
            ):
                raise ValueError("SEC_ATOM_CUSTODY_INVALID")
            claimed = await session.scalar(
                select(AuditEvent.id)
                .where(
                    AuditEvent.actor_id == "research-monitor",
                    AuditEvent.action == CLAIM,
                    AuditEvent.schema_version == SCHEMA,
                    AuditEvent.payload == claim,
                    AuditEvent.created_at <= row.available_at,
                    AuditEvent.occurred_at == datetime.fromisoformat(str(claim["claimed_at"])),
                )
                .limit(1)
            )
            received_audit = await session.scalar(
                select(AuditEvent.id)
                .where(
                    AuditEvent.actor_id == "research-monitor",
                    AuditEvent.action == "research.monitor.sec_atom_candidates_received",
                    AuditEvent.schema_version == SCHEMA,
                    AuditEvent.occurred_at == row.available_at,
                    AuditEvent.created_at <= now,
                    AuditEvent.payload
                    == {
                        "snapshot_id": str(row.id),
                        "snapshot_hash": row.revision,
                        **claim,
                        "negative_conclusion_authorized": False,
                    },
                )
                .limit(1)
            )
            if claimed is None or received_audit is None:
                raise ValueError("SEC_ATOM_CUSTODY_INVALID")
            rule = await session.get(SecMonitorRule, UUID(str(p["rule_id"])))
            if (
                rule is None
                or rule.account_id != settings.agent_paper_account_id
                or str(rule.instrument_id) != p["instrument_id"]
                or p["source_url"] != source_url(rule.issuer_cik)
            ):
                raise ValueError("SEC_ATOM_SCOPE_CHANGED")
            store = configured_exhibit_store(session)
            identity = await _read_identity(
                session,
                store,
                snapshot_id=UUID(str(p["identity_snapshot_id"])),
                as_of=row.available_at,
            )
            if (identity.instrument_id, identity.identity.cik) != (
                rule.instrument_id,
                rule.issuer_cik,
            ):
                raise ValueError("SEC_ATOM_SCOPE_CHANGED")
            bindings = p["originals"]
            if not isinstance(bindings, list) or len(bindings) != 2:
                raise ValueError("SEC_ATOM_CUSTODY_INVALID")
            originals = []
            for i, value in enumerate(bindings):
                binding = IndexArchiveBinding.model_validate(value)
                artifact = await session.get(DataArtifact, binding.artifact_id)
                if (
                    artifact is None
                    or (
                        artifact.uri,
                        artifact.sha256,
                        artifact.byte_size,
                        artifact.source,
                        artifact.schema_version,
                    )
                    != (
                        binding.uri,
                        binding.sha256,
                        binding.byte_size,
                        "sec",
                        "sec_atom_raw_v1" if i == 0 else "sec_atom_receipt_v1",
                    )
                    or max(artifact.created_at, artifact.available_at, artifact.received_at)
                    > row.available_at
                ):
                    raise ValueError("SEC_ATOM_CUSTODY_INVALID")
                originals.append(await _original(store, binding))
            receipt = json.loads(originals[1])
            if not isinstance(receipt, dict) or receipt.pop("claim", None) != claim:
                raise ValueError("SEC_ATOM_CUSTODY_INVALID")
            received = _receipt(
                originals[0],
                json.dumps(receipt).encode(),
                source_url=str(p["source_url"]),
                now=row.available_at,
            )
            claimed_at = datetime.fromisoformat(str(claim["claimed_at"]))
            if received < claimed_at or _bucket(claimed_at).isoformat() != claim["scheduled_at"]:
                raise ValueError("SEC_ATOM_CUSTODY_INVALID")
            rebuilt = parse_atom(originals[0], issuer_cik=rule.issuer_cik, received_at=received)
            if (
                rebuilt != AtomObservation.model_validate(p["observation"])
                or rebuilt.received_at != row.observed_at
                or row.provider_object_id
                != f"{rule.instrument_id}:{sha256(originals[1]).hexdigest()}"
            ):
                raise ValueError("SEC_ATOM_CUSTODY_INVALID")
            return _result(
                "verified",
                snapshot_id=str(row.id),
                source=SOURCE,
                received_at=received.isoformat(),
                entry_count=len(rebuilt.entries),
                has_next_page=rebuilt.has_next_page,
                negative_conclusion_authorized=False,
                primary_confirmation_required=True,
                candidate_hints=[
                    e.accession_number
                    for e in rebuilt.entries
                    if e.source_updated_at > rule.effective_at
                ],
            )
    finally:
        await factory.kw["bind"].dispose()
