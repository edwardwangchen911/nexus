"""One-company SEC read: three GETs, local persistence, no Agent or broker calls."""

import asyncio
import json
from datetime import UTC, datetime
from hashlib import sha256
from urllib.parse import urlencode, urlsplit
from uuid import UUID

import httpx
from sqlalchemy import or_, select
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
from nexus.settings import EngineSettings

_EXCHANGES = {"Nasdaq": "XNAS", "NYSE": "XNYS", "NYSE American": "XASE"}
_LOOPBACK = {"localhost", "127.0.0.1", "::1"}


class _Budget:
    requests = 0


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
        if self.budget.requests >= 3:
            raise ValueError("SEC_LOCAL_REQUEST_BUDGET_EXHAUSTED")
        self.budget.requests += 1
        return await self.inner.get_bytes(path_segments, query, headers)


class _IdentityTransport(SecHttpTransport):
    def __init__(self, client: _BoundedBytes, identity: SecInstrumentIdentity):
        super().__init__(client)
        self.identity = identity

    async def get(self, path: str, headers: dict[str, str]) -> bytes:
        payload = await super().get(path, headers)
        if path.startswith("/submissions/"):
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


async def refresh_sec_research(
    settings: EngineSettings,
    instrument_id: UUID,
    *,
    contact: str | None,
    http_client: httpx.AsyncClient | None = None,
    store: RawEvidenceStore | None = None,
) -> dict[str, object]:
    """Read one latest 10-K/10-Q and freeze its inputs, without enqueuing an Agent run."""
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
            or instrument.asset_type not in {"equity", "common_stock"}
            or instrument.currency != "USD"
            or instrument.exchange not in US_EQUITY_EXCHANGES
            or len(symbols) != 1
            or symbols[0].endswith(".HK")
        ):
            raise ValueError("SEC_LOCAL_US_INSTRUMENT_REQUIRED")
        symbol, exchange = symbols[0], instrument.exchange
    budget = _Budget()
    owned_client = http_client is None
    client = http_client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    try:
        async with asyncio.timeout(60):
            directory = _BoundedBytes(client, "www.sec.gov", budget)
            mapping_raw = await directory.get_bytes(
                ("files", "company_tickers_exchange.json"),
                (),
                (("User-Agent", f"NEXUS {contact}"), ("Accept-Encoding", "gzip, deflate")),
            )
            mapping = json.loads(mapping_raw)
            if mapping.get("fields") != ["cik", "name", "ticker", "exchange"]:
                raise ValueError("SEC_LOCAL_TICKER_MAPPING_INVALID")
            rows = mapping.get("data")
            if not isinstance(rows, list):
                raise ValueError("SEC_LOCAL_TICKER_MAPPING_INVALID")
            matches = [
                row for row in rows if isinstance(row, list) and len(row) == 4 and row[2] == symbol
            ]
            if (
                len(matches) != 1
                or isinstance(matches[0][0], bool)
                or not isinstance(matches[0][0], int)
                or not 0 < matches[0][0] < 10**10
            ):
                raise ValueError("SEC_LOCAL_TICKER_MAPPING_UNAVAILABLE")
            # The Fosun local importer uses XNYS as a US-market placeholder.
            # Resolve the actual listing from SEC and corroborate it in submissions;
            # never rewrite the immutable instrument master from a research read.
            sec_exchange = _EXCHANGES.get(matches[0][3])
            if sec_exchange not in US_EQUITY_EXCHANGES:
                raise ValueError("SEC_LOCAL_TICKER_MAPPING_UNAVAILABLE")
            assert sec_exchange is not None
            identity = SecInstrumentIdentity(
                instrument_id,
                str(matches[0][0]).zfill(10),
                symbol,
                sec_exchange,
                "USD",
            )
            await store.put_if_absent(
                key=f"raw/sec/{today.isoformat()}/ticker-directory/{sha256(mapping_raw).hexdigest()}.json",
                content=mapping_raw,
            )
            data_client = SecCompanyFactsClient(
                _IdentityTransport(_BoundedBytes(client, "data.sec.gov", budget), identity),
                contact=contact,
                max_attempts=1,
            )
            async with factory() as session, session.begin():
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
                    "sec_exchange": identity.exchange,
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
