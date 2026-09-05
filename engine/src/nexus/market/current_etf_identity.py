"""Current equity ETF proof from a bound 497K; no historical or other fund inference."""

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from nexus.backtest.universe import UniverseDefinitionMember, load_us_liquid_v1_definition
from nexus.domain.time import require_utc
from nexus.market.current_identity import MAX_SEC_BYTES, cik
from nexus.providers.fmp_profile import _invalid_constant, _unique, parse_profile
from nexus.providers.sec import SecCompanyFactsClient
from nexus.providers.sec_http import SecHttpTransport
from nexus.research.sec_disclosures import _VisibleText
from nexus.research.sec_funds import fetch_fund_disclosure, resolve_fund

SOURCE = "current_etf_identity"
SCHEMA = "current_etf_identity_v1"


def member_for(symbol: str) -> UniverseDefinitionMember:
    for member in load_us_liquid_v1_definition(date(2026, 1, 1)).members:
        if member.symbol == symbol and member.asset_type == "equity_etf":
            return member
    raise ValueError("CURRENT_ETF_IDENTITY_INVALID")


class CurrentEtfIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    symbol: str
    cik: str
    series_id: str
    class_id: str
    legal_name: str
    asset_type: Literal["equity_etf"] = "equity_etf"
    currency: Literal["USD"] = "USD"
    exchange: Literal["ARCX"] = "ARCX"
    fmp_exchange: str
    listing_basis: Literal["bound_sec_497k_cover"] = "bound_sec_497k_cover"
    fmp_exchange_precision: Literal["provider_label_only"] = "provider_label_only"
    objective: str
    legal_type_statement: str
    mapping_stored_at: datetime
    mapping_original_received_at: None = None


class CurrentEtfShareClassIdentity(CurrentEtfIdentity):
    """SEC legal share-class name stays authoritative; conflicting provider label stays visible."""

    fmp_name: str
    fmp_name_matches_sec: bool


def _share_class_identity(
    *,
    text: str,
    index: bytes,
    symbol: str,
    identifier: str,
    series: str,
    share_class: str,
    fmp_name: str,
    fmp_exchange: str,
    stored_at: datetime,
) -> CurrentEtfShareClassIdentity:
    parser = _VisibleText()
    parser.feed(index.decode("utf-8-sig"))
    index_text = " ".join(" ".join(parser.parts).split())
    match = re.search(
        rf"Series {series} (.+?) Class/Contract {share_class} ETF Shares {symbol}\b", index_text
    )
    if not match or not 1 < len(match[1]) < 200:
        raise ValueError
    fund_name = match[1]
    legal_name = fund_name + " ETF Shares"
    statement = (
        "Exchange-traded fund shares that are not individually redeemable "
        "and are listed on NYSE Arca"
    )
    cover = re.search(
        rf"Summary Prospectus (.+?) {statement} {re.escape(legal_name)} "
        rf"\({re.escape(symbol)}\)",
        text[:1200],
    )
    objective = re.search(r"Investment Objective (.+?) Fees and Expenses", text)
    strategy = re.search(r"Principal Investment Strategies (.+?) Principal Risks", text)
    if (
        not cover
        or not objective
        or not strategy
        or not objective[1].startswith(
            cover[1] + ", an exchange-traded share class of " + fund_name
        )
        or "seeks to track the performance of a benchmark index" not in objective[1]
        or "investment return of the overall stock market" not in objective[1]
        or "investable U.S. stock market" not in strategy[1]
        or "Fund invests at least 80% of its net assets" not in strategy[1]
        or "in the stocks that make up the Target Index" not in strategy[1]
    ):
        raise ValueError
    return CurrentEtfShareClassIdentity(
        symbol=symbol,
        cik=identifier,
        series_id=series,
        class_id=share_class,
        legal_name=legal_name,
        fmp_name=fmp_name,
        fmp_name_matches_sec=fmp_name in {legal_name, cover[1]},
        fmp_exchange=fmp_exchange,
        objective=objective[1] + " Principal Investment Strategies " + strategy[1],
        legal_type_statement=statement,
        mapping_stored_at=stored_at,
    )


@dataclass(frozen=True)
class EtfInputs:
    identity: CurrentEtfIdentity
    contents: tuple[bytes, ...]
    sec_url: str
    received: tuple[datetime | None, ...]

    @property
    def revision(self) -> str:
        return sha256(
            (
                SCHEMA
                + self.identity.symbol
                + ":".join(sha256(raw).hexdigest() for raw in self.contents)
            ).encode()
        ).hexdigest()


def _json(raw: bytes) -> Any:
    return json.loads(raw, object_pairs_hook=_unique, parse_constant=_invalid_constant)


class _Replay:
    """Only already archived paths; cannot issue HTTP or fall back to a network source."""

    def __init__(self, values: dict[tuple[str, ...], bytes]):
        self.values = values
        self.used: set[tuple[str, ...]] = set()

    async def get_bytes(
        self,
        path_segments: tuple[str, ...],
        query: tuple[tuple[str, str], ...],
        headers: tuple[tuple[str, str], ...],
    ) -> bytes:
        if query or path_segments not in self.values or path_segments in self.used:
            raise ValueError
        self.used.add(path_segments)
        return self.values[path_segments]


async def validate_etf_originals(
    *, symbol: str, contents: tuple[bytes, ...], now: datetime
) -> EtfInputs:
    try:
        member_for(symbol)
        cutoff = require_utc(now)
        if len(contents) != 8 or any(not b or len(b) > MAX_SEC_BYTES for b in contents):
            raise ValueError
        fmp, fm_receipt, mapping, submissions, index, raw, receipts_raw, mapping_receipt = contents
        row = parse_profile(fmp, symbol)
        identifier, series, share_class = resolve_fund(_json(mapping), symbol)
        if (
            cik(row.get("cik")) != identifier
            or row.get("currency") != "USD"
            or row.get("isEtf") is not True
            or row.get("exchange") not in {"AMEX", "ARCA", "NYSE Arca", "NYSEARCA"}
        ):
            raise ValueError
        f = _json(fm_receipt)
        if (
            f
            != dict(
                schema="fmp_profile_receipt_v1",
                source="fmp",
                origin="https://financialmodelingprep.com",
                path="/stable/profile",
                symbol=symbol,
                raw_sha256=sha256(fmp).hexdigest(),
                raw_byte_length=len(fmp),
                received_at=f["received_at"],
            )
            or type(f["raw_byte_length"]) is not int
            or require_utc(datetime.fromisoformat(f["received_at"])) > cutoff
        ):
            raise ValueError
        binding = _json(mapping_receipt)
        stored_at = require_utc(datetime.fromisoformat(binding["stored_at"]))
        if (
            binding["sha256"] != sha256(mapping).hexdigest()
            or binding["original_received_at"] is not None
            or stored_at > cutoff
        ):
            raise ValueError
        receipts = _json(receipts_raw)
        if not isinstance(receipts, list) or len(receipts) != 3:
            raise ValueError
        paths = []
        for number, (receipt, content) in enumerate(
            zip(receipts, (submissions, index, raw), strict=True)
        ):
            origin = "https://data.sec.gov/" if number == 0 else "https://www.sec.gov/"
            if (
                not receipt["url"].startswith(origin)
                or receipt
                != dict(
                    url=receipt["url"],
                    status=200,
                    received_at=receipt["received_at"],
                    sha256=sha256(content).hexdigest(),
                    file=f"response-{number + 1}.raw",
                )
                or type(receipt["status"]) is not int
                or require_utc(datetime.fromisoformat(receipt["received_at"])) > cutoff
            ):
                raise ValueError
            paths.append(tuple(receipt["url"][len(origin) :].split("/")))
        accession_match = re.fullmatch(r"([0-9]{10}-[0-9]{2}-[0-9]{6})-index.html", paths[1][-1])
        if not accession_match or paths[0] != ("submissions", f"CIK{identifier}.json"):
            raise ValueError
        replay = _Replay(dict(zip(paths, (submissions, index, raw), strict=True)))
        disclosure, _ = await fetch_fund_disclosure(
            instrument_id=UUID(int=0),
            symbol=symbol,
            identity=(identifier, series, share_class),
            accession=accession_match[1],
            client=SecCompanyFactsClient(
                SecHttpTransport(replay),
                contact="offline@example.invalid",
                max_attempts=1,
                now=lambda: datetime.fromisoformat(receipts[0]["received_at"]),
            ),
            directory=replay,
            contact="offline@example.invalid",
        )
        if disclosure.source_url != receipts[2]["url"] or len(replay.used) != 3:
            raise ValueError
        visible = _VisibleText()
        visible.feed(raw.decode("utf-8-sig"))
        text = " ".join(" ".join(visible.parts).split())
        name = row.get("companyName")
        if not isinstance(name, str) or not name.strip():
            raise ValueError
        name = " ".join(name.split())
        cover = re.search(
            rf"FORM 497K {re.escape(name)} {re.escape(symbol)} \| NYSE Arca Summary Prospectus\b",
            text[:1000],
        )
        objective = re.search(r"Investment Objective (.+?) Fees and Expenses", text)
        legal = re.search(
            r"Purchase and Sale of Fund Shares (The Fund is an exchange-traded fund\b[^.]*\.)", text
        )
        if (
            not cover
            or not objective
            or not legal
            or name not in objective[1]
            or not re.search(r"index composed of [^.]*U\.S\. equities\.", objective[1])
            or len(re.findall(r"\bFORM 497K\b", text)) != 1
        ):
            identity: CurrentEtfIdentity = _share_class_identity(
                text=text,
                index=index,
                symbol=symbol,
                identifier=identifier,
                series=series,
                share_class=share_class,
                fmp_name=name,
                fmp_exchange=row["exchange"],
                stored_at=stored_at,
            )
        else:
            identity = CurrentEtfIdentity(
                symbol=symbol,
                cik=identifier,
                series_id=series,
                class_id=share_class,
                legal_name=name,
                fmp_exchange=row["exchange"],
                objective=objective[1],
                legal_type_statement=legal[1],
                mapping_stored_at=stored_at,
            )
        fmp_received = require_utc(datetime.fromisoformat(f["received_at"]))
        sec_received = tuple(
            require_utc(datetime.fromisoformat(r["received_at"])) for r in receipts
        )
        if tuple(sorted(sec_received)) != sec_received:
            raise ValueError
        return EtfInputs(
            identity,
            contents,
            disclosure.source_url,
            (fmp_received, fmp_received, None, *sec_received, sec_received[-1], None),
        )
    except (ValueError, TypeError, KeyError, IndexError, UnicodeError, RecursionError) as error:
        raise ValueError("CURRENT_ETF_IDENTITY_INVALID") from error
