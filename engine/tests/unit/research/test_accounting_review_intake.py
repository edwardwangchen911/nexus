"""Review material and submitted opinions never confer accounting authority."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import UUID

import pytest

NOW = datetime(2026, 10, 23, tzinfo=UTC)
FACT = "00000000-0000-0000-0000-000000000001"
AMENDMENT = "00000000-0000-0000-0000-000000000002"


def manifest(tmp_path):
    raw = b"Public filing clause: awards effective September 1."
    (tmp_path / "source.txt").write_bytes(raw)
    return {
        "schema_version": "accounting_material_v1",
        "instrument_id": FACT,
        "issuer_cik": "0000320193",
        "facts": [
            {
                "fact_id": FACT,
                "binding": {"period_end": "2025-09-27", "unit": "USD/shares", "value": "7.46"},
                "original_source_sha256": sha256(raw).hexdigest(),
                "context_locator": "Note 3 / c-1 / f-120",
                "context_sha256": "b" * 64,
            }
        ],
        "amendments": [
            {
                "evidence_ref_id": AMENDMENT,
                "source_sha256": sha256(raw).hexdigest(),
                "accession_number": "0001140361-26-035325",
            }
        ],
        "sources": [
            {
                "relative_path": "source.txt",
                "sha256": sha256(raw).hexdigest(),
                "byte_length": len(raw),
                "source_url": "https://www.sec.gov/Archives/edgar/data/320193/000114036126035325/doc.txt",
                "received_at": "2026-10-08T01:33:36Z",
            }
        ],
        "unresolved_questions": [
            "Assess historical compensation expense and diluted denominator; "
            "source acceptance times conflict."
        ],
    }


def test_material_verifies_original_bytes_and_returns_no_clearance(tmp_path):
    from nexus.research.accounting_review import build_material, material_digest

    data = manifest(tmp_path)
    package = build_material(data, source_root=tmp_path, now=NOW)
    assert package.model_dump(mode="json")["facts"][0]["binding"] == data["facts"][0]["binding"]
    assert len(material_digest(package)) == 64
    assert package.accounting_authority is False
    (tmp_path / "source.txt").write_bytes(b"changed")
    with pytest.raises(ValueError, match="ACCOUNTING_SOURCE_INVALID"):
        build_material(data, source_root=tmp_path, now=NOW)


@pytest.mark.parametrize(
    "change", ["traversal", "symlink", "future", "wrong_source", "duplicate_fact", "authority"]
)
def test_material_rejects_invalid_bindings_and_unsafe_sources(tmp_path, change):
    from nexus.research.accounting_review import build_material

    data = manifest(tmp_path)
    if change == "traversal":
        data["sources"][0]["relative_path"] = "../source.txt"
    elif change == "symlink":
        (tmp_path / "link.txt").symlink_to(tmp_path / "source.txt")
        data["sources"][0]["relative_path"] = "link.txt"
    elif change == "future":
        data["sources"][0]["received_at"] = (NOW + timedelta(seconds=1)).isoformat()
    elif change == "wrong_source":
        data["facts"][0]["original_source_sha256"] = "c" * 64
    elif change == "duplicate_fact":
        data["facts"].append(deepcopy(data["facts"][0]))
    else:
        data["accounting_authority"] = True
    with pytest.raises(ValueError):
        build_material(data, source_root=tmp_path, now=NOW)


def submission(package):
    from nexus.research.accounting_review import material_digest

    return {
        "schema_version": "accounting_submission_v1",
        "submission_id": str(UUID(int=3)),
        "material_sha256": material_digest(package),
        "reviewer_name": "Synthetic reviewer",
        "reviewer_capacity": "Test qualification claim, not trusted authority",
        "reviewed_at": NOW.isoformat(),
        "findings": [
            {
                "fact_id": FACT,
                "amendment_evidence_ref_id": AMENDMENT,
                "conclusion": "as_disclosed_not_replaced",
                "reasoning": "Review applies only to this exact historical fact.",
                "citations": [
                    {
                        "source_sha256": package.sources[0].sha256,
                        "locator": "paragraph 1",
                        "quote": "awards effective September 1",
                    }
                ],
                "timing_conflict_assessment": (
                    "Unknown conflict does not establish historical availability."
                ),
            }
        ],
    }


def test_submission_requires_exact_complete_pairs_and_evidence(tmp_path):
    from nexus.research.accounting_review import build_material, validate_submission

    package = build_material(manifest(tmp_path), source_root=tmp_path, now=NOW)
    data = submission(package)
    checked = validate_submission(data, package=package, source_root=tmp_path, now=NOW)
    assert checked.findings[0].conclusion == "as_disclosed_not_replaced"
    assert checked.authority_status == "unverified"
    for mutate in (
        lambda d: d.update(material_sha256="a" * 64),
        lambda d: d.update(findings=[]),
        lambda d: d["findings"][0].update(fact_id=str(UUID(int=8))),
        lambda d: d["findings"][0]["citations"][0].update(quote="invented quote"),
        lambda d: d.update(reviewed_at=(NOW + timedelta(seconds=1)).isoformat()),
        lambda d: d.update(authority_status="verified"),
    ):
        invalid = deepcopy(data)
        mutate(invalid)
        with pytest.raises(ValueError):
            validate_submission(invalid, package=package, source_root=tmp_path, now=NOW)
    assert json.loads(checked.model_dump_json())["authority_status"] == "unverified"


def test_xbrl_check_rejects_semantic_mismatch_even_when_file_hash_is_correct(tmp_path):
    import xml.etree.ElementTree as ET

    from nexus.research.accounting_review import build_material, verify_xbrl_contexts

    raw = (
        b'<html xmlns:x="http://www.xbrl.org/2003/instance" '
        b'xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">'
        b'<x:context id="c"><x:entity><x:identifier scheme="http://www.sec.gov/CIK">'
        b"0000320193</x:identifier></x:entity><x:period><x:startDate>2024-09-29</x:startDate>"
        b"<x:endDate>2025-09-27</x:endDate></x:period></x:context>"
        b'<x:unit id="u"><x:divide><x:unitNumerator><x:measure>iso4217:USD</x:measure>'
        b"</x:unitNumerator><x:unitDenominator><x:measure>xbrli:shares</x:measure>"
        b"</x:unitDenominator></x:divide></x:unit>"
        b'<ix:nonFraction id="eps" name="us-gaap:EarningsPerShareDiluted" '
        b'contextRef="c" unitRef="u" scale="0" decimals="2">7.46</ix:nonFraction></html>'
    )
    data = manifest(tmp_path)
    (tmp_path / "source.txt").write_bytes(raw)
    source_hash = sha256(raw).hexdigest()
    data["sources"][0].update(sha256=source_hash, byte_length=len(raw))
    data["amendments"][0]["source_sha256"] = source_hash
    context = ET.fromstring(raw).find("{http://www.xbrl.org/2003/instance}context")
    context_hash = sha256(ET.tostring(context, encoding="unicode").encode()).hexdigest()
    fact = data["facts"][0]
    fact.update(original_source_sha256=source_hash, context_sha256=context_hash)
    fact["binding"].update(
        period_start="2024-09-29",
        raw_tag="EarningsPerShareDiluted",
        raw_taxonomy="us-gaap",
        xbrl_nodes_json=json.dumps(
            [
                {
                    "attributes": {"id": "eps", "contextRef": "c", "unitRef": "u"},
                    "context_xml_sha256": context_hash,
                }
            ]
        ),
    )
    package = build_material(data, source_root=tmp_path, now=NOW)
    assert verify_xbrl_contexts(package, source_root=tmp_path, now=NOW) == 1
    for field, value in (("value", "7.47"), ("period_start", "2024-10-01"), ("unit", "EUR/shares")):
        wrong = deepcopy(data)
        wrong["facts"][0]["binding"][field] = value
        invalid = build_material(wrong, source_root=tmp_path, now=NOW)
        with pytest.raises(ValueError, match="ACCOUNTING_CONTEXT_INVALID"):
            verify_xbrl_contexts(invalid, source_root=tmp_path, now=NOW)


@pytest.mark.asyncio
async def test_database_binding_rejects_same_value_wrong_revision_and_wrong_hash(tmp_path):
    from datetime import date
    from decimal import Decimal
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from nexus.research.accounting_review import build_material
    from nexus.research.accounting_review_storage import verify_database_scope

    fact = SimpleNamespace(
        instrument_id=UUID(FACT),
        canonical_fact="diluted_eps",
        raw_taxonomy="us-gaap",
        raw_tag="EarningsPerShareDiluted",
        period_kind="duration",
        period_start=date(2024, 9, 29),
        period_end=date(2025, 9, 27),
        unit="USD/shares",
        value=Decimal("7.460000000000"),
        accession_number="0000320193-25-000079",
        revision="original",
        evidence_ref_id=UUID(int=4),
        created_at=NOW,
    )
    ref = SimpleNamespace(
        source="sec", content_hash="a" * 64, revision="original", available_at=NOW
    )
    data = manifest(tmp_path)
    data["facts"][0]["binding"] = {
        "instrument_id": FACT,
        "canonical_fact": "diluted_eps",
        "raw_taxonomy": "us-gaap",
        "raw_tag": "EarningsPerShareDiluted",
        "period_kind": "duration",
        "period_start": "2024-09-29",
        "period_end": "2025-09-27",
        "unit": "USD/shares",
        "value": "7.460000000000",
        "accession_number": "0000320193-25-000079",
        "revision": "original",
        "evidence_ref_id": str(UUID(int=4)),
        "evidence_content_hash": "a" * 64,
        "evidence_revision": "original",
        "evidence_available_at": NOW.isoformat(),
        "fact_created_at": NOW.isoformat(),
    }
    package = build_material(data, source_root=tmp_path, now=NOW)
    session = AsyncMock()
    session.get.side_effect = [fact, ref]
    session.scalar.return_value = object()
    await verify_database_scope(session, package)
    for obj, field, value in ((fact, "revision", "new"), (ref, "content_hash", "b" * 64)):
        old = getattr(obj, field)
        setattr(obj, field, value)
        session.get.side_effect = [fact, ref]
        with pytest.raises(ValueError, match="ACCOUNTING_DATABASE_SCOPE_INVALID"):
            await verify_database_scope(session, package)
        setattr(obj, field, old)
    session.get.side_effect = [None]
    with pytest.raises(ValueError, match="ACCOUNTING_DATABASE_SCOPE_INVALID"):
        await verify_database_scope(session, package)
