"""The local importer cannot proxy originals or substitute a different object version."""

import json
import subprocess
import sys
from hashlib import sha256
from io import BytesIO
from pathlib import Path

import pytest
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.settings import EngineSettings


def settings():
    return EngineSettings(
        object_store_endpoint="http://127.0.0.1:19000",
        object_store_region="us-east-1",
        object_store_bucket="synthetic",
        object_store_access_key_id="synthetic-key",
        object_store_secret_access_key="synthetic-secret",
    )


@pytest.mark.asyncio
async def test_evidence_reader_requires_exact_namespace_and_version_without_other_reads():
    raw = b"synthetic canonical"
    key = f"evidence/alpaca/action-observation/{sha256(raw).hexdigest()}.json"
    calls = []

    class Client:
        def put_object(self, **kwargs):
            return {"VersionId": "synthetic-v1"}

        def get_object(self, **kwargs):
            calls.append(kwargs)
            assert kwargs["Bucket"] == "synthetic"
            assert kwargs["Key"] == "provider/market/" + key
            assert kwargs["VersionId"] == "synthetic-v1"
            return {"Body": BytesIO(raw), "VersionId": "synthetic-v1"}

    store = S3ProviderEvidenceStore(settings(), client=Client())
    uri = await store.put_if_absent(key=key, content=raw)
    assert await store.read_evidence(key, uri=uri) == raw
    for bad_key, bad_uri in [
        (key.replace("evidence/", "raw/"), uri),
        ("evidence/../secret", uri),
        (key, uri.replace("s3://synthetic/", "s3://other/")),
        (key, uri.replace("provider/market/", "provider/other/")),
        (key, uri.split("?")[0]),
    ]:
        with pytest.raises(ValueError):
            await store.read_evidence(bad_key, uri=bad_uri)
    assert len(calls) == 1


@pytest.mark.parametrize("scheme", ["http", "https"])
def test_local_opt_in_disables_all_environment_proxies_without_changing_default(tmp_path, scheme):
    env = {
        "PYTHONPATH": str(Path(__file__).resolve().parents[3] / "src"),
        "AWS_CONFIG_FILE": "/dev/null",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_EC2_METADATA_DISABLED": "true",
        **{
            key: "http://proxy.example.invalid:8080"
            for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")
        },
    }
    code = """
import json, sys
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.settings import EngineSettings
def deny_network(event,args):
    if event in {"socket.connect","socket.getaddrinfo"}: raise AssertionError("network forbidden")
sys.addaudithook(deny_network)
url = sys.argv[1]+"://127.0.0.1:19000"
settings = EngineSettings(object_store_endpoint=url,object_store_region="us-east-1",
    object_store_bucket="synthetic",object_store_access_key_id="synthetic-key",
    object_store_secret_access_key="synthetic-secret")
local = S3ProviderEvidenceStore(settings,disable_environment_proxies=True)._client
legacy = S3ProviderEvidenceStore(settings)._client
try:
    local_proxy = local._endpoint.http_session._proxy_config.proxy_url_for(url)
    legacy_proxy = legacy._endpoint.http_session._proxy_config.proxy_url_for(url)
    print(json.dumps({"local_direct":local_proxy is None,
        "legacy_proxy":legacy_proxy=="http://proxy.example.invalid:8080"}))
finally:
    local.close(); legacy.close()
"""
    result = subprocess.run(
        [sys.executable, "-c", code, scheme],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"local_direct": True, "legacy_proxy": True}
