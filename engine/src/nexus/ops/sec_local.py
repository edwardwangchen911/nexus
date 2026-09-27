"""One-company SEC read: bounded GETs, local persistence, no Agent or broker calls."""

import asyncio
import json
import re
from datetime import UTC, date, datetime
from hashlib import sha256
from typing import Literal, cast
from urllib.parse import urlencode, urlsplit
from uuid import UUID

import httpx
from sqlalchemy import func, or_, select
from sqlalchemy.engine import make_url

from nexus.agents.snapshots import prepare_research_snapshot
from nexus.jobs.provider_collection import RawEvidenceStore, collect_sec_filings
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.providers.sec import SecCompanyFactsClient, SecInstrumentIdentity
from nexus.providers.sec_http import SecHttpTransport
from nexus.providers.settings import ProviderWorkerSettings
from nexus.research.sec_amendment_storage import AmendmentStore, persist_amendment
from nexus.research.sec_amendments import SecAmendmentDisclosure
from nexus.research.sec_disclosures import SecDisclosure, disclosure_excerpt, persist_disclosure
from nexus.research.sec_fact_projection import SavedSecEvidenceReader, append_saved_sec_facts
from nexus.research.sec_funds import fetch_fund_disclosure, resolve_fund
from nexus.settings import EngineSettings

_EXCHANGES = {"Nasdaq": "XNAS", "NYSE": "XNYS", "NYSE American": "XASE"}
_LOOPBACK = {"localhost", "127.0.0.1", "::1"}


class _Budget:
    def __init__(self, maximum: int = 3) -> None:
        self.requests = 0
        self.maximum = maximum


class _BoundedBytes:
    def __init__(self, client: httpx.AsyncClient, host: str, budget: _Budget):
        self.inner = FixedOriginHttpClient(
            provider_name="sec",
            origin=AllowedOrigin(scheme="https", host=host),
            policy=HttpRequestPolicy(max_attempts=1),
            client=client,
        )
        self.budget = budget

    async def get_bytes(
        self,
        path_segments: tuple[str, ...],
        query: tuple[tuple[str, str], ...],
        headers: tuple[tuple[str, str], ...],
    ) -> bytes:
        if self.budget.requests >= self.budget.maximum:
            raise ValueError("SEC_LOCAL_REQUEST_BUDGET_EXHAUSTED")
        self.budget.requests += 1
        return await self.inner.get_bytes(path_segments, query, headers)


class _IdentityTransport(SecHttpTransport):
    def __init__(
        self,
        client: _BoundedBytes,
        identity: SecInstrumentIdentity,
        store: RawEvidenceStore,
        received_on: date,
    ):
        super().__init__(client)
        self.identity = identity
        self.store = store
        self.received_on = received_on

    async def get(self, path: str, headers: dict[str, str]) -> bytes:
        payload = await super().get(path, headers)
        if path.startswith("/submissions/"):
            await self.store.put_if_absent(
                key=(
                    f"raw/sec/{self.received_on.isoformat()}/submissions/"
                    f"{sha256(payload).hexdigest()}.json"
                ),
                content=payload,
            )
            doc = json.loads(payload)
            tickers, exchanges = doc.get("tickers"), doc.get("exchanges")
            if (
                str(doc.get("cik")).zfill(10) != self.identity.cik
                or not isinstance(tickers, list)
                or not isinstance(exchanges, list)
                or len(tickers) != len(exchanges)
                or (self.identity.symbol, self.identity.exchange)
                not in {
                    (ticker, _EXCHANGES.get(exchange))
                    for ticker, exchange in zip(tickers, exchanges, strict=True)
                }
            ):
                raise ValueError("SEC_LOCAL_ISSUER_MISMATCH")
        return payload


async def project_saved_sec_facts(
    settings: EngineSettings,
    filing_id: UUID,
    *,
    store: SavedSecEvidenceReader | None = None,
) -> dict[str, object]:
    """Explicit local repair of missing facts; no collector, prepare, model or old-row writes."""
    if not settings.database_url or make_url(settings.database_url).host not in _LOOPBACK:
        raise ValueError("SEC_LOCAL_DATABASE_REQUIRED")
    if store is None:
        if urlsplit(settings.object_store_endpoint or "").hostname not in _LOOPBACK:
            raise ValueError("SEC_LOCAL_OBJECT_STORE_REQUIRED")
        store = S3ProviderEvidenceStore(settings)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            created = await append_saved_sec_facts(session, filing_id, store)
        return {
            "status": "SEC_FACTS_PROJECTED",
            "facts_created": created,
            "provider_requests": 0,
            "model_requests": 0,
            "execution_locked": True,
            "research_completed": False,
        }
    finally:
        await factory.kw["bind"].dispose()


async def refresh_sec_research(
    settings: EngineSettings,
    instrument_id: UUID,
    *,
    contact: str | None,
    http_client: httpx.AsyncClient | None = None,
    store: RawEvidenceStore | None = None,
    disclosure_only: bool = False,
    disclosure_form: Literal["6-K", "8-K"] = "6-K",
    fund_accession: str | None = None,
) -> dict[str, object]:
    """Freeze company facts, latest selected 8-K/6-K, or one 497K; never enqueue."""
    if disclosure_form not in {"6-K", "8-K"} or (disclosure_form != "6-K" and not disclosure_only):
        raise ValueError("SEC_LOCAL_DISCLOSURE_FORM_INVALID")
    if fund_accession is not None and (
        disclosure_only or not re.fullmatch(r"[0-9]{10}-[0-9]{2}-[0-9]{6}", fund_accession)
    ):
        raise ValueError("SEC_LOCAL_FUND_ACCESSION_REQUIRED")
    if (
        not contact
        or contact != contact.strip()
        or "@" not in contact
        or len(contact.encode()) > 254
        or any(ord(ch) < 32 for ch in contact)
    ):
        raise ValueError("SEC_CONTACT_REQUIRED")
    if not settings.database_url or make_url(settings.database_url).host not in _LOOPBACK:
        raise ValueError("SEC_LOCAL_DATABASE_REQUIRED")
    if store is None:
        if urlsplit(settings.object_store_endpoint or "").hostname not in _LOOPBACK:
            raise ValueError("SEC_LOCAL_OBJECT_STORE_REQUIRED")
        store = S3ProviderEvidenceStore(
            ProviderWorkerSettings.model_validate(
                {
                    "PROVIDER_DATABASE_URL": settings.database_url,
                    "PROVIDER_OBJECT_STORE_ENDPOINT": settings.object_store_endpoint,
                    "PROVIDER_OBJECT_STORE_REGION": settings.object_store_region,
                    "PROVIDER_OBJECT_STORE_BUCKET": settings.object_store_bucket,
                    "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID": settings.object_store_access_key_id,
                    "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY": (
                        settings.object_store_secret_access_key
                    ),
                }
            )
        )
    factory = session_factory(settings)
    today = datetime.now(UTC).date()
    async with factory() as session:
        instrument = await session.get(Instrument, instrument_id)
        symbols = list(
            await session.scalars(
                select(InstrumentAlias.symbol).where(
                    InstrumentAlias.instrument_id == instrument_id,
                    InstrumentAlias.alias_type == "ticker",
                    InstrumentAlias.valid_from <= today,
                    or_(InstrumentAlias.valid_to.is_(None), InstrumentAlias.valid_to >= today),
                )
            )
        )
        if (
            instrument is None
            or instrument.asset_type not in {"equity", "common_stock", "equity_etf"}
            or instrument.currency != "USD"
            or instrument.exchange not in US_EQUITY_EXCHANGES
            or len(symbols) != 1
            or symbols[0].endswith(".HK")
        ):
            raise ValueError("SEC_LOCAL_US_INSTRUMENT_REQUIRED")
        symbol, exchange = symbols[0], instrument.exchange
    budget = _Budget(4 if fund_accession or (disclosure_only and disclosure_form == "8-K") else 3)
    owned_client = http_client is None
    client = http_client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    try:
        async with asyncio.timeout(60):
            directory = _BoundedBytes(client, "www.sec.gov", budget)
            mapping_raw = await directory.get_bytes(
                (
                    "files",
                    "company_tickers_mf.json"
                    if fund_accession
                    else "company_tickers_exchange.json",
                ),
                (),
                (("User-Agent", f"NEXUS {contact}"), ("Accept-Encoding", "gzip, deflate")),
            )
            mapping = json.loads(mapping_raw)
            fund_identity = resolve_fund(mapping, symbol) if fund_accession else None
            sec_exchange = None
            if fund_identity:
                cik = fund_identity[0]
            else:
                if mapping.get("fields") != ["cik", "name", "ticker", "exchange"]:
                    raise ValueError("SEC_LOCAL_TICKER_MAPPING_INVALID")
                rows = mapping.get("data")
                if not isinstance(rows, list):
                    raise ValueError("SEC_LOCAL_TICKER_MAPPING_INVALID")
                matches = [
                    row
                    for row in rows
                    if isinstance(row, list) and len(row) == 4 and row[2] == symbol
                ]
                if (
                    len(matches) != 1
                    or isinstance(matches[0][0], bool)
                    or not isinstance(matches[0][0], int)
                    or not 0 < matches[0][0] < 10**10
                ):
                    raise ValueError("SEC_LOCAL_TICKER_MAPPING_UNAVAILABLE")
                # Resolve the actual listing; the local XNYS placeholder is not evidence.
                sec_exchange = _EXCHANGES.get(matches[0][3])
                if sec_exchange not in US_EQUITY_EXCHANGES:
                    raise ValueError("SEC_LOCAL_TICKER_MAPPING_UNAVAILABLE")
                cik = str(matches[0][0]).zfill(10)
            identity = SecInstrumentIdentity(
                instrument_id,
                cik,
                symbol,
                sec_exchange or exchange,
                "USD",
            )
            await store.put_if_absent(
                key=f"raw/sec/{today.isoformat()}/ticker-directory/{sha256(mapping_raw).hexdigest()}.json",
                content=mapping_raw,
            )
            data_client = SecCompanyFactsClient(
                SecHttpTransport(_BoundedBytes(client, "data.sec.gov", budget))
                if fund_identity
                else _IdentityTransport(
                    _BoundedBytes(client, "data.sec.gov", budget), identity, store, today
                ),
                contact=contact,
                max_attempts=1,
            )
            async with factory() as session, session.begin():
                if fund_accession and fund_identity:
                    document, originals = await fetch_fund_disclosure(
                        instrument_id=instrument_id,
                        symbol=symbol,
                        identity=fund_identity,
                        accession=fund_accession,
                        client=data_client,
                        directory=directory,
                        contact=contact,
                    )
                    for kind, original in originals:
                        await store.put_if_absent(
                            key=f"raw/sec/{today.isoformat()}/{kind}/{sha256(original).hexdigest()}.bin",
                            content=original,
                        )
                    content = document.canonical_bytes()
                    location = await store.put_if_absent(
                        key=f"evidence/sec/{fund_accession}/{sha256(content).hexdigest()}.json",
                        content=content,
                    )
                    inserted = await persist_disclosure(
                        session, document, location=location, received_at=datetime.now(UTC)
                    )
                    created, replayed = int(inserted), int(not inserted)
                elif disclosure_only:
                    current = await data_client.fetch_submissions(identity.cik)
                    recent = json.loads(current.raw_payload)["filings"]["recent"]
                    fields = (
                        "accessionNumber",
                        "acceptanceDateTime",
                        "filingDate",
                        "form",
                        "primaryDocument",
                    )
                    arrays = []
                    for field in fields:
                        array = recent.get(field)
                        if not isinstance(array, list):
                            raise ValueError("SEC_LOCAL_SUBMISSIONS_INVALID")
                        arrays.append(array)
                    if len({len(array) for array in arrays}) != 1:
                        raise ValueError("SEC_LOCAL_SUBMISSIONS_INVALID")
                    candidates: list[tuple[datetime, str, date, str, str]] = []
                    for accession, accepted, filed, form, filename in zip(*arrays, strict=True):
                        is_amendment = disclosure_form == "8-K" and form == "8-K/A"
                        if form != disclosure_form and not is_amendment:
                            continue
                        if not all(
                            isinstance(value, str)
                            for value in (accession, accepted, filed, filename)
                        ):
                            raise ValueError("SEC_LOCAL_SUBMISSIONS_INVALID")
                        try:
                            accepted_at = datetime.fromisoformat(accepted.replace("Z", "+00:00"))
                            filed_date = date.fromisoformat(filed)
                        except ValueError:
                            raise ValueError("SEC_LOCAL_ACCEPTANCE_TIME_INVALID") from None
                        if accepted_at.tzinfo is None:
                            raise ValueError("SEC_LOCAL_ACCEPTANCE_TIME_INVALID")
                        if (
                            accepted_at > current.received_at
                            or filed_date > current.received_at.date()
                        ):
                            raise ValueError("SEC_LOCAL_ACCEPTANCE_TIME_INVALID")
                        candidates.append((accepted_at, accession, filed_date, filename, form))
                    selected = []
                    for form in (
                        (disclosure_form, "8-K/A")
                        if disclosure_form == "8-K"
                        else (disclosure_form,)
                    ):
                        group = {row for row in candidates if row[4] == form}
                        if not group:
                            continue
                        latest_at = max(row[0] for row in group)
                        latest = [row for row in group if row[0] == latest_at]
                        if len(latest) != 1:
                            raise ValueError("SEC_LOCAL_DISCLOSURE_AMBIGUOUS")
                        selected.append(latest[0])
                    if not selected:
                        raise ValueError("SEC_LOCAL_DISCLOSURE_UNAVAILABLE")
                    # Validate both selected paths before any main-document request.
                    for _, accession, _, filename, _ in selected:
                        if not re.fullmatch(
                            r"[0-9]{10}-[0-9]{2}-[0-9]{6}", accession
                        ) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*\.html?", filename):
                            raise ValueError("SEC_LOCAL_DISCLOSURE_PATH_INVALID")
                    await store.put_if_absent(
                        key=f"raw/sec/{today.isoformat()}/submissions/{sha256(current.raw_payload).hexdigest()}.json",
                        content=current.raw_payload,
                    )
                    created, replayed = 0, 0
                    for accepted_at, accession, filed, filename, form in selected:
                        segments = (
                            "Archives",
                            "edgar",
                            "data",
                            str(int(identity.cik)),
                            accession.replace("-", ""),
                            filename,
                        )
                        raw = await directory.get_bytes(
                            segments,
                            (),
                            (
                                ("User-Agent", f"NEXUS {contact}"),
                                ("Accept-Encoding", "gzip, deflate"),
                            ),
                        )
                        received_at = datetime.now(UTC)
                        excerpt, truncated = disclosure_excerpt(raw)
                        common = dict(
                            instrument_id=instrument_id,
                            accession_number=accession,
                            accepted_at=accepted_at,
                            filed_at=datetime.combine(filed, datetime.min.time(), tzinfo=UTC),
                            source_url="https://www.sec.gov/" + "/".join(segments),
                            raw_content_hash=sha256(raw).hexdigest(),
                            excerpt=excerpt,
                            truncated=truncated,
                        )
                        if form == "8-K/A":
                            # Timestamp local custody in the same database clock used
                            # by availability/freeze validation, after response receipt.
                            receipt_clock = await session.scalar(select(func.clock_timestamp()))
                            if not isinstance(receipt_clock, datetime):
                                raise ValueError("SEC_AMENDMENT_RECEIPT_TIME_INVALID")
                            amendment = SecAmendmentDisclosure.model_validate(
                                {
                                    **common,
                                    "form": "8-K/A",
                                    "issuer_cik": identity.cik,
                                    "received_at": receipt_clock,
                                    "raw_byte_length": len(raw),
                                    "http_status": 200,
                                }
                            )
                            inserted = await persist_amendment(
                                session, cast(AmendmentStore, store), amendment, raw=raw
                            )
                        else:
                            document = SecDisclosure.model_validate(
                                {
                                    **common,
                                    "form": disclosure_form,
                                    "issuer_cik": identity.cik
                                    if disclosure_form == "8-K"
                                    else None,
                                }
                            )
                            content = document.canonical_bytes()
                            digest = sha256(content).hexdigest()
                            await store.put_if_absent(
                                key=f"raw/sec/{today.isoformat()}/disclosure/{document.raw_content_hash}.html",
                                content=raw,
                            )
                            location = await store.put_if_absent(
                                key=f"evidence/sec/{accession}/{digest}.json", content=content
                            )
                            inserted = await persist_disclosure(
                                session, document, location=location, received_at=received_at
                            )
                        created += int(inserted)
                        replayed += int(not inserted)
                else:
                    created, replayed = await collect_sec_filings(
                        session,
                        client=data_client,
                        store=store,
                        instruments=(identity,),
                        latest_only=True,
                    )
                as_of = datetime.now(UTC)
                snapshot = await prepare_research_snapshot(session, instrument_id, as_of=as_of)
                if snapshot is None:
                    raise ValueError("SEC_LOCAL_SNAPSHOT_UNAVAILABLE")
                result = {
                    "status": "RESEARCH_INPUTS_PREPARED",
                    "data_snapshot_id": str(snapshot.id),
                    "filings_created": created,
                    "filings_replayed": replayed,
                    "provider_requests": budget.requests,
                    "model_requests": 0,
                    "execution_locked": True,
                    "research_completed": False,
                    "instrument_exchange": exchange,
                    "sec_exchange": sec_exchange,
                    "research_path": f"/research/{instrument_id}?"
                    + urlencode(
                        {
                            "as_of": as_of.isoformat().replace("+00:00", "Z"),
                        }
                    ),
                }
            return result
    finally:
        if owned_client:
            await client.aclose()
