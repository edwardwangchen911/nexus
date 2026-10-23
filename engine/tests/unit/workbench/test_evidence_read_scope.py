"""Read-local frozen bytes reuse cannot cross versions, stores or requests."""

from types import SimpleNamespace

import pytest
from nexus.workbench.evidence_read_scope import evidence_read_scope


class Store:
    def __init__(self):
        self.calls = 0
        self.value = b"original"
        self.fail = False

    async def read_raw(self, key, *, uri):
        self.calls += 1
        if self.fail:
            raise ValueError("original unavailable")
        return self.value

    read_evidence = read_raw


@pytest.mark.asyncio
async def test_exact_frozen_bytes_reused_only_within_scope():
    source = Store()
    session = SimpleNamespace(info={"sec_exhibit_store": source})
    uri = "s3://bucket/evidence/a?versionId=v1"
    with evidence_read_scope(session):
        reader = session.info["sec_exhibit_store"]
        assert await reader.read_evidence("evidence/a", uri=uri) == b"original"
        assert await reader.read_evidence("evidence/a", uri=uri) == b"original"
        assert source.calls == 1
        await reader.read_evidence("evidence/a", uri=uri.replace("v1", "v2"))
        assert source.calls == 2
        with pytest.raises(ValueError, match="WORKBENCH_READ_ONLY"):
            await reader.put_if_absent(key="evidence/a", content=b"changed")
    assert session.info["sec_exhibit_store"] is source
    source.value = b"changed"
    with evidence_read_scope(session):
        assert (
            await session.info["sec_exhibit_store"].read_evidence("evidence/a", uri=uri)
            == b"changed"
        )
    assert source.calls == 3
    # A retained wrapper cannot carry successful cached bytes past its scope.
    assert await reader.read_evidence("evidence/a", uri=uri) == b"changed"
    assert source.calls == 4


@pytest.mark.asyncio
async def test_unversioned_and_failed_reads_are_never_cached():
    source = Store()
    session = SimpleNamespace(info={"sec_exhibit_store": source})
    with evidence_read_scope(session):
        reader = session.info["sec_exhibit_store"]
        for uri in ("s3://bucket/raw/a", "s3://bucket/raw/a?versionId=null"):
            await reader.read_raw("raw/a", uri=uri)
            await reader.read_raw("raw/a", uri=uri)
        assert source.calls == 4
        source.fail = True
        for _ in range(2):
            with pytest.raises(ValueError, match="original unavailable"):
                await reader.read_raw("raw/a", uri="s3://bucket/raw/a?versionId=v1")
        assert source.calls == 6
    assert session.info["sec_exhibit_store"] is source


@pytest.mark.asyncio
async def test_bounded_cache_and_exception_restoration():
    source = Store()
    session = SimpleNamespace(info={"sec_exhibit_store": source})
    with pytest.raises(RuntimeError):
        with evidence_read_scope(session, max_bytes=8, max_entries=1):
            reader = session.info["sec_exhibit_store"]
            for version in ("a", "b", "b"):
                await reader.read_raw("raw/a", uri=f"s3://bucket/raw/a?versionId={version}")
            assert source.calls == 3
            raise RuntimeError()
    assert session.info["sec_exhibit_store"] is source


def test_no_store_configuration_preserves_existing_empty_state_reads():
    from nexus.settings import EngineSettings

    settings = EngineSettings().model_copy(update={"object_store_endpoint": None})
    session = SimpleNamespace(info={"engine_settings": settings})
    with evidence_read_scope(session):
        assert session.info == {"engine_settings": settings}
    assert session.info == {"engine_settings": settings}
