"""Build immutable local Web assets, then serve them without request-time compilation."""

import argparse
import hashlib
import io
import json
import os
import shutil
import socket
import stat
import subprocess
import tarfile
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import dotenv_values

RUNTIME_KEYS = ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SSL_CERT_FILE", "NODE_EXTRA_CA_CERTS")
BFF_KEYS = (
    "NEXUS_ENV",
    "NEXUS_ENGINE_BASE_URL",
    "NEXUS_ENGINE_HMAC_CURRENT_KEY_ID",
    "NEXUS_ENGINE_HMAC_CURRENT_SECRET",
)
MANIFEST = "nexus-web-release.json"


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def private_directory(path):
    if (
        path.is_symlink()
        or not path.is_dir()
        or path.stat().st_uid != os.getuid()
        or stat.S_IMODE(path.stat().st_mode) != 0o700
    ):
        raise ValueError("LOCAL_WEB_RELEASE_DIRECTORY_UNSAFE")


def artifact_hashes(release):
    result = {}
    for path in sorted((release / "dist").rglob("*")):
        if path.is_symlink():
            raise ValueError("LOCAL_WEB_RELEASE_ARTIFACT_UNSAFE")
        if path.is_file():
            result[path.relative_to(release).as_posix()] = digest(path)
    if (
        "dist/server/index.js" not in result
        or "dist/server/wrangler.json" not in result
        or not any(p.startswith("dist/client/") for p in result)
    ):
        raise ValueError("LOCAL_WEB_RELEASE_INCOMPLETE")
    return result


def verify_release(root, release):
    private_directory(release)
    value = json.loads((release / MANIFEST).read_bytes())
    if (
        value.get("schema") != "nexus_local_web_release_v1"
        or value.get("revision") != git(root, "rev-parse", "HEAD")
        or value.get("tree") != git(root, "rev-parse", "HEAD^{tree}")
    ):
        raise ValueError("LOCAL_WEB_RELEASE_VERSION_MISMATCH")
    if value.get("artifacts") != artifact_hashes(release):
        raise ValueError("LOCAL_WEB_RELEASE_ARTIFACT_MISMATCH")
    return value


def build(root, release):
    if release.exists():
        return verify_release(root, release)
    private_directory(release.parent)
    if shutil.disk_usage(release.parent).free < 3 * 1024**3:
        raise ValueError("LOCAL_WEB_RELEASE_SPACE_REQUIRED")
    revision = git(root, "rev-parse", "HEAD")
    release.mkdir(mode=0o700)
    raw = subprocess.check_output(["git", "-C", str(root), "archive", revision])
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        archive.extractall(release, filter="data")
    (release / "node_modules").symlink_to(root / "node_modules", target_is_directory=True)
    environment = {key: os.environ[key] for key in RUNTIME_KEYS if key in os.environ}
    environment.update(WRANGLER_WRITE_LOGS="false", WRANGLER_SEND_METRICS="false")
    with (release / "build.log").open("x") as log:
        subprocess.run(
            [shutil.which("npm") or "npm", "run", "build"],
            cwd=release,
            env=environment,
            stdout=log,
            stderr=log,
            timeout=180,
            check=True,
        )
    value = dict(
        schema="nexus_local_web_release_v1",
        revision=revision,
        tree=git(root, "rev-parse", "HEAD^{tree}"),
        artifacts=artifact_hashes(release),
    )
    (release / MANIFEST).write_text(json.dumps(value, sort_keys=True, indent=2))
    return verify_release(root, release)


def bff_bindings(root, env_file, revision):
    path = env_file if env_file.is_absolute() else root / env_file
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as stream:
        info = os.fstat(stream.fileno())
        if (
            info.st_uid != os.getuid()
            or not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise ValueError("LOCAL_WEB_ENV_UNSAFE")
        source = dotenv_values(stream=stream, interpolate=False)
    values = {key: source[key] for key in BFF_KEYS if source.get(key)}
    values["NEXUS_ENGINE_HMAC_CURRENT_SECRET"] = source.get(
        "NEXUS_ENGINE_HMAC_CURRENT_SECRET"
    ) or source.get("NEXUS_ENGINE_HMAC_SECRET")
    base = urlsplit(values.get("NEXUS_ENGINE_BASE_URL", ""))
    if (
        base.scheme not in {"http", "https"}
        or base.hostname not in {"localhost", "127.0.0.1", "::1"}
        or base.username
        or base.password
        or base.query
        or base.fragment
        or base.path not in {"", "/"}
        or not values["NEXUS_ENGINE_HMAC_CURRENT_SECRET"]
    ):
        raise ValueError("LOCAL_WEB_BFF_CONFIG_INVALID")
    values["NEXUS_RELEASE_REVISION"] = revision
    if any("\n" in value or "\r" in value for value in values.values()):
        raise ValueError("LOCAL_WEB_BFF_CONFIG_INVALID")
    return values


def private_write(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        info = os.fstat(stream.fileno())
        if (
            info.st_uid != os.getuid()
            or not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise ValueError("LOCAL_WEB_RUNTIME_FILE_UNSAFE")
        os.ftruncate(stream.fileno(), 0)
        stream.write(value)


def serve(root, release, env_file, port, check=False):
    if not 1 <= port <= 65535:
        raise ValueError("LOCAL_WEB_PORT_INVALID")
    manifest = verify_release(root, release)
    bindings = bff_bindings(root, env_file, manifest["revision"])
    config = json.loads((release / "dist/server/wrangler.json").read_bytes())
    for name in (
        "kv_namespaces",
        "r2_buckets",
        "d1_databases",
        "services",
        "workflows",
        "hyperdrive",
        "mtls_certificates",
    ):
        if config.get(name):
            raise ValueError("LOCAL_WEB_REMOTE_BINDINGS_UNSUPPORTED")
    if check:
        return dict(
            code="LOCAL_BUILT_WEB_READY", revision=manifest["revision"], external_requests=0
        )
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))
    runtime = release / "runtime"
    runtime.mkdir(mode=0o700, exist_ok=True)
    private_directory(runtime)
    local_config = dict(
        name="nexus-local-built",
        main=str(release / "dist/server/index.js"),
        compatibility_date=config["compatibility_date"],
        compatibility_flags=config["compatibility_flags"],
        assets={**config.get("assets", {}), "directory": str(release / "dist/client")},
        no_bundle=True,
        base_dir=str(release / "dist/server"),
        rules=config.get("rules", []),
    )
    config_path = runtime / "wrangler.json"
    private_write(config_path, json.dumps(local_config))
    env_path = runtime / ".dev.vars"
    # JSON string syntax preserves values without shell interpolation or argv secrets.
    private_write(
        env_path, "\n".join(key + "=" + json.dumps(value) for key, value in bindings.items()) + "\n"
    )
    environment = {key: os.environ[key] for key in RUNTIME_KEYS if key in os.environ}
    environment.update(
        WRANGLER_SEND_METRICS="false",
        WRANGLER_WRITE_LOGS="false",
        WRANGLER_LOG_PATH=str(runtime / "logs"),
    )
    node = shutil.which("node")
    if not node:
        raise ValueError("LOCAL_WEB_NODE_REQUIRED")
    os.chdir(root)
    os.execve(
        node,
        [
            node,
            str(release / "node_modules/wrangler/bin/wrangler.js"),
            "dev",
            "--config",
            str(config_path),
            "--local",
            "--no-bundle",
            "--ip",
            "127.0.0.1",
            "--port",
            str(port),
            "--inspector-port",
            "0",
            "--persist-to",
            str(runtime / "state"),
        ],
        environment,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build", "serve"))
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=Path("infra/.env.local"))
    parser.add_argument("--port", type=int, default=5173)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        root, release = args.repo_root.resolve(), args.release_dir.absolute()
        result = (
            build(root, release)
            if args.action == "build"
            else serve(root, release, args.env_file, args.port, args.check)
        )
        print(
            json.dumps(
                {
                    "code": "LOCAL_BUILT_WEB_READY",
                    "revision": result.get("revision"),
                }
            )
        )
        return 0
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        code = (
            str(error)
            if isinstance(error, ValueError) and str(error).startswith("LOCAL_WEB_")
            else "LOCAL_WEB_RELEASE_FAILED"
        )
        print(json.dumps({"code": code}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
