"""Prepare or check bounded accounting review material, without provider calls."""

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from sqlalchemy import text

from nexus.ops.local_recovery import write_private
from nexus.research.accounting_review import (
    build_material,
    canonical_bytes,
    material_digest,
    validate_submission,
    verify_xbrl_contexts,
)


async def database_action(args: argparse.Namespace) -> dict[str, object]:
    from nexus.ops.agent_paper_once import _validate_local_database
    from nexus.ops.local_stack import _validate_local_env
    from nexus.ops.openai_local import local_settings
    from nexus.persistence.session import session_factory
    from nexus.research.accounting_review_storage import (
        read_receipt,
        receive_submission,
        verify_database_scope,
    )

    root = args.repo_root.resolve()
    _validate_local_env(root, root / "infra/.env.local")
    settings = local_settings(root, backend="codex_sdk")
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            if args.action in ("designate", "adopt", "inspect-adoption"):
                from sqlalchemy import func, select

                from nexus.persistence.models.research import ResearchEvidenceRef
                from nexus.research.accounting_adoption import SOURCE as ADOPTION_SOURCE
                from nexus.research.accounting_adoption_storage import (
                    adopt_review,
                    read_adoptions,
                    register_designation,
                )

                if args.submission_id is None:
                    raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
                if args.action == "designate":
                    if not args.operator_confirmed:
                        raise ValueError("ACCOUNTING_OWNER_OPERATOR_CONFIRMATION_REQUIRED")
                    if args.designation is None:
                        raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
                    designation = await register_designation(
                        session, args.submission_id, read_json(args.designation)
                    )
                    return {
                        "status": "owner_designation_registered",
                        "designation_id": designation.provider_object_id,
                        "registered_at": designation.created_at.isoformat(),
                        "model_calls": 0,
                        "provider_calls": 0,
                    }
                if args.designation_id is None:
                    raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
                ref: ResearchEvidenceRef | None
                if args.action == "adopt":
                    if args.sources is None:
                        raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
                    ref = await adopt_review(
                        session, args.submission_id, args.designation_id, source_root=args.sources
                    )
                else:
                    await session.execute(text("SET TRANSACTION READ ONLY"))
                    ref = await session.scalar(
                        select(ResearchEvidenceRef).where(
                            ResearchEvidenceRef.source == ADOPTION_SOURCE,
                            ResearchEvidenceRef.provider_object_id
                            == f"{args.submission_id}:{args.designation_id}",
                        )
                    )
                    receipt = await read_receipt(session, args.submission_id)
                    if ref is None or receipt is None:
                        raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
                    from nexus.research.accounting_review import ReviewMaterial

                    material = ReviewMaterial.model_validate(receipt.payload["material"])
                    now = await session.scalar(select(func.clock_timestamp()))
                    if not isinstance(now, datetime):
                        raise ValueError("ACCOUNTING_RECEIPT_TIME_INVALID")
                    await read_adoptions(
                        session,
                        instrument_id=material.instrument_id,
                        as_of=now,
                        evidence_ids=(ref.id,),
                    )
                assert ref is not None
                return {
                    "status": "adopted_as_disclosed_scope",
                    "evidence_ref_id": str(ref.id),
                    "content_hash": ref.content_hash,
                    "available_at": ref.available_at.isoformat(),
                    "accounting_authority": False,
                    "model_calls": 0,
                    "provider_calls": 0,
                }
            if args.action == "check-bindings":
                await session.execute(text("SET TRANSACTION READ ONLY"))
                if args.material is None or args.sources is None:
                    raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
                now = datetime.now(UTC)
                material = build_material(
                    read_json(args.material), source_root=args.sources, now=now
                )
                count = verify_xbrl_contexts(material, source_root=args.sources, now=now)
                await verify_database_scope(session, material)
                return {
                    "status": "database_bindings_match",
                    "fact_count": len(material.facts),
                    "xbrl_contexts_verified": count,
                    "accounting_authority": False,
                    "model_calls": 0,
                    "provider_calls": 0,
                }
            if args.action == "inspect":
                await session.execute(text("SET TRANSACTION READ ONLY"))
                if args.submission_id is None:
                    raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
                receipt = await read_receipt(session, args.submission_id)
                if receipt is None:
                    raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
            else:
                if args.material is None or args.sources is None or args.submission is None:
                    raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
                now = datetime.now(UTC)
                material = build_material(
                    read_json(args.material), source_root=args.sources, now=now
                )
                opinion = validate_submission(
                    read_json(args.submission), package=material, source_root=args.sources, now=now
                )
                receipt = await receive_submission(
                    session, material, opinion, source_root=args.sources
                )
            return {
                "status": receipt.payload["status"],
                "receipt_id": str(receipt.id),
                "received_at": receipt.received_at.isoformat(),
                "submission_sha256": receipt.payload["submission_sha256"],
                "accounting_authority": False,
                "model_calls": 0,
                "provider_calls": 0,
            }
    finally:
        await factory.kw["bind"].dispose()


def read_json(path: Path) -> object:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 8 * 1024 * 1024:
        raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
    return json.loads(path.read_bytes())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=(
            "export",
            "check",
            "check-bindings",
            "receive",
            "inspect",
            "designate",
            "adopt",
            "inspect-adoption",
        ),
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--material", type=Path)
    parser.add_argument("--sources", type=Path)
    parser.add_argument("--submission", type=Path)
    parser.add_argument("--submission-id", type=UUID)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--designation", type=Path)
    parser.add_argument("--designation-id", type=UUID)
    parser.add_argument(
        "--operator-confirmed",
        action="store_true",
        help="Local operator has independently verified the owner instruction and report source",
    )
    args = parser.parse_args()
    try:
        if args.action in (
            "receive",
            "inspect",
            "check-bindings",
            "designate",
            "adopt",
            "inspect-adoption",
        ):
            print(json.dumps(asyncio.run(database_action(args))))
            return 0
        if args.material is None or args.sources is None:
            raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
        now = datetime.now(UTC)
        material = build_material(read_json(args.material), source_root=args.sources, now=now)
        checked_contexts = verify_xbrl_contexts(material, source_root=args.sources, now=now)
        if args.submission is not None:
            validate_submission(
                read_json(args.submission), package=material, source_root=args.sources, now=now
            )
        if args.action == "export":
            if args.output is None:
                raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
            write_private(args.output, canonical_bytes(material))
        elif args.output is not None:
            raise ValueError("ACCOUNTING_REVIEW_INPUT_INVALID")
    except (OSError, ValueError):
        print(json.dumps({"status": "rejected", "code": "ACCOUNTING_REVIEW_INPUT_INVALID"}))
        return 1
    except Exception:
        print(
            json.dumps({"status": "unavailable", "code": "ACCOUNTING_REVIEW_STORAGE_UNAVAILABLE"})
        )
        return 1
    print(
        json.dumps(
            {
                "status": "submission_validated_pending_authority"
                if args.submission
                else "material_ready",
                "material_sha256": material_digest(material),
                "fact_count": len(material.facts),
                "amendment_count": len(material.amendments),
                "xbrl_contexts_verified": checked_contexts,
                "accounting_authority": False,
                "model_calls": 0,
                "provider_calls": 0,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
