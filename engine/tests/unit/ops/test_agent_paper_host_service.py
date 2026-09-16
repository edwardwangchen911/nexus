"""Exercise the real adapter with an isolated launchctl executable, never launchd."""

import json
import os
import plistlib
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[4]
SOURCE = ROOT / "scripts/macos/agent-paper-loop-service.py"
LABEL = f"gui/{os.getuid()}/com.nexus.agent-paper-loop"
RUNTIME = str(Path(sys.executable).resolve())
SERVICE_WORKING_DIRECTORY = str(Path(RUNTIME).parent)


def _executable(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"#!{sys.executable}\n" + source)
    path.chmod(0o755)


def _service_root(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    root = tmp_path / "repo"
    home = tmp_path / "home"
    (home / "Library/Logs").mkdir(parents=True)
    script = root / "scripts/macos" / SOURCE.name
    script.parent.mkdir(parents=True)
    shutil.copy2(SOURCE, script)
    (root / "infra").mkdir()
    (root / "infra/compose.yaml").touch()
    (root / "infra/.env.test").write_text("AGENT_RUNTIME_MODE=codex_sdk\n")
    (root / ".gitignore").write_text("infra/.env*\n.secrets/\n")
    subprocess.run(["git", "init", "-q", str(root)], check=True, capture_output=True)
    control, state, record = [tmp_path / name for name in ("control", "state", "calls")]
    control.write_text("{}")
    state.write_text("{}")
    common = (
        "import json, os, plistlib, sys\nfrom pathlib import Path\n"
        f"record = Path({str(record)!r})\ncontrol = Path({str(control)!r})\n"
        "options = json.loads(control.read_text())\n"
        "def log(kind, args):\n"
        "    with record.open('a') as f: f.write(json.dumps([kind, *args]) + '\\n')\n"
    )
    _executable(
        root / ".venv/bin/nexus-engine-agent-paper-loop",
        common
        + """
log('engine', sys.argv[1:])
if options.get('record_launcher'):
    log('prefix', [sys.prefix])
if '--service-preflight' in sys.argv: sys.exit(options.get('preflight_exit', 0))
log('environment', sorted(os.environ))
sys.exit(options.get('run_exit', 0))
""",
    )
    (root / ".venv/pyvenv.cfg").write_text(
        f"home = {Path(RUNTIME).parent}\ninclude-system-site-packages = false\n"
    )
    _executable(
        tmp_path / "bin/launchctl",
        common
        + f"state = Path({str(state)!r})\n"
        + """
job = json.loads(state.read_text())
action = sys.argv[1]
log('launchctl', sys.argv[1:])
if action == 'bootstrap':
    with open(sys.argv[3], 'rb') as f: job = plistlib.load(f)
    job['state'] = 'inactive'
elif action == 'kickstart':
    job['state'] = options.get('kickstart_state', 'running')
    for field in ('exit', 'pid'):
        if field in options: job[field] = options[field]
    state.write_text(json.dumps(job))
    sys.exit(options.get('kickstart_exit', 0))
elif action == 'print':
    if not job: sys.exit(113)
    if job['state'] == 'error': sys.exit(64)
    display_state = {'delayed_config_error': 'running', 'config_error': 'not running'}
    print('state = ' + display_state.get(job['state'], job['state']))
    if 'pid' in job: print('pid = ' + str(job['pid']))
    indent = options.get('indent', '')
    print(indent + 'working directory = ' + job['WorkingDirectory'])
    print(indent + 'arguments = {')
    for arg in job['ProgramArguments']: print(indent + '\\t' + arg)
    print(indent + '}')
    if options.get('duplicate_owner'):
        print(indent + 'working directory = ' + job['WorkingDirectory'])
    print('environment = {\\n\\tCANARY => private-launchctl-canary\\n}')
    if 'exit' in job: print('last exit code = ' + str(job['exit']))
    if job['state'] == 'config_error': print('last exit code = 78: EX_CONFIG')
    if job['state'] == 'delayed_config_error': job['state'] = 'config_error'
elif action == 'kill':
    job['state'] = 'inactive'
    job['exit'] = options.get('stop_exit', 0)
elif action == 'bootout': job = {}
state.write_text(json.dumps(job))
""",
    )
    return root, {
        **os.environ,
        "HOME": str(home),
        "PATH": f"{tmp_path / 'bin'}:/usr/bin:/bin",
    }


def _invoke(root, environment, action, *extra):
    return subprocess.run(
        [
            sys.executable,
            str(root / "scripts/macos" / SOURCE.name),
            action,
            "--repo-root",
            str(root),
            "--env-file",
            "infra/.env.test",
            "--project-name",
            "nexus-test",
            *extra,
        ],
        cwd=root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


def _calls(root):
    path = root.parent / "calls"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def _register(root, *, state="running", legacy=False, exit_code=None):
    args = [RUNTIME, str(root / "scripts/macos" / SOURCE.name), "run"]
    if not legacy:
        args += [
            "--repo-root",
            str(root),
            "--env-file",
            str(root / "infra/.env.test"),
            "--project-name",
            "nexus-test",
        ]
    job = {
        "state": state,
        "ProgramArguments": args,
        "WorkingDirectory": str(root) if legacy else SERVICE_WORKING_DIRECTORY,
    }
    if exit_code is not None:
        job["exit"] = exit_code
    (root.parent / "state").write_text(json.dumps(job))
    return job


def test_start_binds_exact_config_and_repeated_start_is_idempotent(tmp_path):
    root, env = _service_root(tmp_path)
    result = _invoke(root, env, "start")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["status"] == "started"
    assert _calls(root)[:2] == [
        ["launchctl", "print", LABEL],
        [
            "engine",
            "--service-preflight",
            "--repo-root",
            str(root),
            "--env-file",
            str(root / "infra/.env.test"),
        ],
    ]
    path = root / ".secrets/nexus-agent-paper/service.plist"
    service = plistlib.loads(path.read_bytes())
    assert service["ProgramArguments"] == [
        RUNTIME,
        str(root / "scripts/macos" / SOURCE.name),
        "run",
        "--repo-root",
        str(root),
        "--env-file",
        str(root / "infra/.env.test"),
        "--project-name",
        "nexus-test",
    ]
    assert service["WorkingDirectory"] == SERVICE_WORKING_DIRECTORY
    log_directory = Path(env["HOME"]) / "Library/Logs/nexus-agent-paper"
    assert service["StandardOutPath"] == str(log_directory / "service.stdout.log")
    assert service["StandardErrorPath"] == str(log_directory / "service.stderr.log")
    assert stat.S_IMODE(log_directory.stat().st_mode) == 0o700
    assert stat.S_IMODE((log_directory / "service.stdout.log").stat().st_mode) == 0o600
    assert stat.S_IMODE((log_directory / "service.stderr.log").stat().st_mode) == 0o600
    assert service["KeepAlive"] is False and service["RunAtLoad"] is False
    assert "EnvironmentVariables" not in service
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    before, count = path.stat().st_mtime_ns, len(_calls(root))
    assert _invoke(root, env, "start").returncode == 0
    assert _calls(root)[count:] == [["launchctl", "print", LABEL]]
    assert path.stat().st_mtime_ns == before


@pytest.mark.parametrize("action", ["start", "status", "stop"])
@pytest.mark.parametrize("different", ["repo", "env", "project", "script", "cwd"])
def test_owner_mismatch_has_zero_mutation_or_signal(tmp_path, action, different):
    root, env = _service_root(tmp_path)
    job = _register(root)
    if different == "cwd":
        job["WorkingDirectory"] = str(tmp_path / "other")
    else:
        index = {"repo": 4, "env": 6, "project": 8, "script": 1}[different]
        job["ProgramArguments"][index] = str(tmp_path / "other")
    state = root.parent / "state"
    state.write_text(json.dumps(job))
    before = state.read_bytes()
    result = _invoke(root, env, action)
    assert result.returncode == 1
    assert json.loads(result.stdout) == {"error_code": "AGENT_PAPER_SERVICE_OWNER_MISMATCH"}
    assert _calls(root) == [["launchctl", "print", LABEL]]
    assert not (root / ".secrets").exists()
    assert state.read_bytes() == before
    assert "private-launchctl-canary" not in result.stdout + result.stderr


@pytest.mark.parametrize("action", ["status", "stop"])
@pytest.mark.parametrize("different", [None, "script", "cwd"])
def test_legacy_requires_both_script_and_cwd_proof(tmp_path, action, different):
    root, env = _service_root(tmp_path)
    job = _register(root, legacy=True)
    if different == "cwd":
        job["WorkingDirectory"] = str(tmp_path / "other")
    elif different == "script":
        job["ProgramArguments"][1] = str(tmp_path / "other/scripts/macos" / SOURCE.name)
    (root.parent / "state").write_text(json.dumps(job))
    result = _invoke(root, env, action)
    assert result.returncode == (0 if different is None else 1)
    if different:
        assert json.loads(result.stdout)["error_code"] == "AGENT_PAPER_SERVICE_OWNER_MISMATCH"
        assert _calls(root) == [["launchctl", "print", LABEL]]
    elif action == "stop":
        assert _calls(root)[-1] == ["launchctl", "bootout", LABEL]
        assert _invoke(root, env, "start").returncode == 0
        assert (
            json.loads((root.parent / "state").read_text())["ProgramArguments"][-1] == "nexus-test"
        )


def test_start_never_replaces_legacy_or_exit_75_registration(tmp_path):
    root, env = _service_root(tmp_path)
    _register(root, legacy=True)
    assert _invoke(root, env, "start").returncode == 1
    _register(root, state="inactive", exit_code=75)
    result = _invoke(root, env, "start")
    assert result.returncode == 1
    assert json.loads(result.stdout)["error_code"] == "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"
    assert _calls(root) == [["launchctl", "print", LABEL]] * 2


@pytest.mark.parametrize("initial", ["running", "inactive"])
def test_exit_75_survives_repeated_stop_until_explicit_acknowledgement(tmp_path, initial):
    root, env = _service_root(tmp_path)
    _register(root, state=initial, exit_code=75 if initial == "inactive" else None)
    (root.parent / "control").write_text(json.dumps({"stop_exit": 75}))
    for _ in range(2):
        result = _invoke(root, env, "stop")
        assert result.returncode == 1
        assert json.loads(result.stdout) == {"error_code": "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED"}
    calls = _calls(root)
    assert not any(call[1] == "bootout" for call in calls)
    assert sum(call[1] == "kill" for call in calls) == (initial == "running")
    result = _invoke(root, env, "stop", "--acknowledge-unconfirmed")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _calls(root)[len(calls) :] == [
        ["launchctl", "print", LABEL],
        ["launchctl", "bootout", LABEL],
    ]


@pytest.mark.parametrize(("state", "exit_code"), [("running", 75), ("inactive", 0)])
def test_acknowledgement_requires_inactive_exit_75(tmp_path, state, exit_code):
    root, env = _service_root(tmp_path)
    _register(root, state=state, exit_code=exit_code)
    assert _invoke(root, env, "stop", "--acknowledge-unconfirmed").returncode == 1
    assert _calls(root) == [["launchctl", "print", LABEL]]


@pytest.mark.parametrize("ambiguous", ["starting", "pid"])
def test_acknowledgement_never_assumes_an_ambiguous_registration_has_exited(tmp_path, ambiguous):
    root, env = _service_root(tmp_path)
    job = _register(root, state="inactive" if ambiguous == "pid" else "starting", exit_code=75)
    if ambiguous == "pid":
        job["pid"] = 123
    (root.parent / "state").write_text(json.dumps(job))
    result = _invoke(root, env, "stop", "--acknowledge-unconfirmed")
    assert result.returncode == 1
    assert _calls(root) == [["launchctl", "print", LABEL]]


@pytest.mark.parametrize("action", ["status", "stop", "start"])
def test_invoking_from_second_repo_cannot_replace_existing_owner_plist(tmp_path, action):
    owner, env = _service_root(tmp_path / "owner")
    other, _ = _service_root(tmp_path / "other")
    _register(owner)
    private = owner / ".secrets/nexus-agent-paper"
    private.mkdir(parents=True, mode=0o700)
    plist = private / "service.plist"
    plist.write_bytes(b"registered owner plist")
    result = _invoke(other, env, action)
    assert result.returncode == 1
    assert json.loads(result.stdout)["error_code"] == "AGENT_PAPER_SERVICE_OWNER_MISMATCH"
    assert _calls(owner) == [["launchctl", "print", LABEL]]
    assert _calls(other) == []
    assert plist.read_bytes() == b"registered owner plist"
    assert not (other / ".secrets").exists()


def test_start_checks_existing_owner_before_local_runtime_availability(tmp_path):
    root, env = _service_root(tmp_path)
    job = _register(root)
    job["ProgramArguments"][-1] = "another-project"
    (root.parent / "state").write_text(json.dumps(job))
    (root / ".venv/bin/nexus-engine-agent-paper-loop").unlink()
    result = _invoke(root, env, "start")
    assert json.loads(result.stdout) == {"error_code": "AGENT_PAPER_SERVICE_OWNER_MISMATCH"}
    assert _calls(root) == [["launchctl", "print", LABEL]]


@pytest.mark.parametrize(
    ("options", "code"),
    [
        ({"preflight_exit": 7}, "AGENT_PAPER_SERVICE_PREFLIGHT_FAILED"),
        (
            {"kickstart_state": "delayed_config_error"},
            "AGENT_PAPER_SERVICE_BACKGROUND_ACCESS_REQUIRED",
        ),
    ],
)
def test_start_fails_closed_without_leaving_failed_plist(tmp_path, options, code):
    root, env = _service_root(tmp_path)
    (root.parent / "control").write_text(json.dumps(options))
    result = _invoke(root, env, "start")
    assert result.returncode == 1
    assert json.loads(result.stdout) == {"error_code": code}
    assert not (root / ".secrets/nexus-agent-paper/service.plist").exists()
    if "preflight_exit" in options:
        assert not any(call[1] == "bootstrap" for call in _calls(root))


@pytest.mark.parametrize(
    "options",
    [
        {"kickstart_state": "error"},
        {"kickstart_state": "starting", "exit": 75},
        {"kickstart_state": "running", "pid": 123, "exit": 78},
        {"kickstart_state": "running", "pid": 123, "exit": 1, "kickstart_exit": 1},
    ],
)
def test_start_retains_registration_and_plist_without_positive_inactive_proof(tmp_path, options):
    root, env = _service_root(tmp_path)
    (root.parent / "control").write_text(json.dumps(options))
    result = _invoke(root, env, "start")
    assert result.returncode == 1
    assert json.loads(result.stdout)["error_code"] in {
        "AGENT_PAPER_SERVICE_START_FAILED",
        "AGENT_PAPER_SERVICE_STOP_UNCONFIRMED",
        "AGENT_PAPER_SERVICE_BACKGROUND_ACCESS_REQUIRED",
    }
    job = json.loads((root.parent / "state").read_text())
    assert job["state"] == options["kickstart_state"]
    if "exit" in options:
        assert job["exit"] == options["exit"]
    assert (root / ".secrets/nexus-agent-paper/service.plist").is_file()
    assert not any(
        call[0] == "launchctl" and call[1] in {"kill", "bootout"} for call in _calls(root)
    )


@pytest.mark.parametrize("action", ["start", "status", "stop"])
@pytest.mark.parametrize("field", ["env", "cwd", "script"])
def test_owner_comparison_preserves_trailing_space_content(tmp_path, action, field):
    root, env = _service_root(tmp_path)
    job = _register(root)
    if field == "cwd":
        job["WorkingDirectory"] += " "
    else:
        job["ProgramArguments"][6 if field == "env" else 1] += " "
    state = root.parent / "state"
    state.write_text(json.dumps(job))
    before = state.read_bytes()
    result = _invoke(root, env, action)
    assert result.returncode == 1
    assert json.loads(result.stdout) == {"error_code": "AGENT_PAPER_SERVICE_OWNER_MISMATCH"}
    assert _calls(root) == [["launchctl", "print", LABEL]]
    assert state.read_bytes() == before
    assert not (root / ".secrets").exists()


def test_owner_parser_accepts_only_unambiguous_launchctl_indentation(tmp_path):
    root, env = _service_root(tmp_path)
    _register(root)
    control = root.parent / "control"
    control.write_text(json.dumps({"indent": "\t"}))
    assert _invoke(root, env, "status").returncode == 0
    control.write_text(json.dumps({"indent": "\t", "duplicate_owner": True}))
    result = _invoke(root, env, "stop")
    assert result.returncode == 1
    assert json.loads(result.stdout) == {"error_code": "AGENT_PAPER_SERVICE_OWNER_MISMATCH"}
    assert _calls(root) == [["launchctl", "print", LABEL]] * 2


@pytest.mark.parametrize("unsafe", ["parent_symlink", "log_symlink", "log_hardlink", "plist"])
def test_start_rejects_unsafe_private_paths(tmp_path, unsafe):
    root, env = _service_root(tmp_path)
    target = tmp_path / "outside"
    if unsafe == "parent_symlink":
        target.mkdir(mode=0o700)
        (root / ".secrets").symlink_to(target, target_is_directory=True)
    else:
        private = root / ".secrets/nexus-agent-paper"
        private.mkdir(parents=True, mode=0o700)
        private.parent.chmod(0o700)
        if unsafe == "plist":
            (private / "service.plist").mkdir()
        else:
            log_directory = Path(env["HOME"]) / "Library/Logs/nexus-agent-paper"
            log_directory.mkdir(mode=0o700)
            target.write_text("outside")
            target.chmod(0o600)
            if unsafe == "log_symlink":
                (log_directory / "service.stdout.log").symlink_to(target)
            else:
                os.link(target, log_directory / "service.stdout.log")
    result = _invoke(root, env, "start")
    assert result.returncode == 1
    assert json.loads(result.stdout) == {"error_code": "AGENT_PAPER_SERVICE_DIRECTORY_UNSAFE"}
    assert result.stderr == ""
    assert not any(call[1] == "bootstrap" for call in _calls(root))


def test_run_uses_exact_config_and_only_runtime_environment(tmp_path):
    root, env = _service_root(tmp_path)
    env.update(
        AGENT_RUNTIME_MODE="responses",
        DATABASE_URL="private-canary",
        OPENAI_API_KEY="secret",
        HTTP_PROXY="http://proxy.invalid:8080",
        HTTPS_PROXY="http://proxy.invalid:8080",
        NO_PROXY="localhost,127.0.0.1",
        http_proxy="http://proxy.invalid:8081",
        https_proxy="http://proxy.invalid:8081",
        no_proxy="localhost,127.0.0.1",
        ALL_PROXY="socks5h://proxy.invalid:1080",
        all_proxy="socks5h://proxy.invalid:1081",
    )
    result = _invoke(root, env, "run")
    assert result.returncode == 0, result.stdout + result.stderr
    explicit = ["--repo-root", str(root), "--env-file", str(root / "infra/.env.test")]
    calls = _calls(root)
    assert calls[:2] == [["engine", "--service-preflight", *explicit], ["engine", *explicit]]
    names = set(calls[2][1:])
    assert {
        "PATH",
        "HOME",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "no_proxy",
    } <= names
    assert not names & {
        "AGENT_RUNTIME_MODE",
        "DATABASE_URL",
        "OPENAI_API_KEY",
        "ALL_PROXY",
        "all_proxy",
    }


@pytest.mark.parametrize("action", ["start", "run"])
def test_engine_entrypoint_is_always_invoked_through_resolved_python(tmp_path, action):
    root, env = _service_root(tmp_path)
    (root.parent / "control").write_text(json.dumps({"record_launcher": True}))
    engine = root / ".venv/bin/nexus-engine-agent-paper-loop"
    source = engine.read_text()
    engine.write_text("#!/missing/python\n" + source.split("\n", 1)[1])

    result = _invoke(root, env, action)

    assert result.returncode == 0, result.stdout + result.stderr
    assert any(call[0] == "engine" for call in _calls(root))
    assert ["prefix", str(root / ".venv")] in _calls(root)


def test_status_normalizes_without_echoing_launchctl_details(tmp_path):
    root, env = _service_root(tmp_path)
    _register(root)
    result = _invoke(root, env, "status")
    assert result.returncode == 0
    assert json.loads(result.stdout) == {"label": "com.nexus.agent-paper-loop", "status": "running"}
    _register(root, state="error")
    result = _invoke(root, env, "status")
    assert result.returncode == 1
    assert json.loads(result.stdout) == {"error_code": "AGENT_PAPER_SERVICE_STATUS_UNAVAILABLE"}


def test_stop_interrupts_before_removing_registration(tmp_path):
    root, env = _service_root(tmp_path)
    _register(root)
    result = _invoke(root, env, "stop")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _calls(root) == [
        ["launchctl", "print", LABEL],
        ["launchctl", "kill", "SIGINT", LABEL],
        ["launchctl", "print", LABEL],
        ["launchctl", "bootout", LABEL],
    ]


@pytest.mark.parametrize("action", ["start", "status", "stop", "run"])
def test_every_action_requires_explicit_arguments(tmp_path, action):
    root, env = _service_root(tmp_path)
    result = subprocess.run(
        [sys.executable, str(root / "scripts/macos" / SOURCE.name), action],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert _calls(root) == []


@pytest.mark.parametrize(
    "invalid", ["missing", "tracked", "symlink", "outside", "not_ignored", "project", "root"]
)
def test_invalid_config_fails_before_launchctl_or_engine(tmp_path, invalid):
    root, environment = _service_root(tmp_path)
    env, extra = root / "infra/.env.test", []
    if invalid == "missing":
        env.unlink()
    elif invalid == "tracked":
        subprocess.run(["git", "-C", str(root), "add", "-f", str(env)], check=True)
    elif invalid == "symlink":
        target = root / "infra/.env.other"
        env.rename(target)
        env.symlink_to(target)
    elif invalid == "outside":
        target = tmp_path / ".env.outside"
        target.write_text("secret=value")
        extra = ["--env-file", str(target)]
    elif invalid == "not_ignored":
        (root / ".gitignore").write_text("")
    elif invalid == "project":
        extra = ["--project-name", "Invalid project"]
    else:
        (root / "infra/compose.yaml").unlink()
    assert _invoke(root, environment, "start", *extra).returncode == 1
    assert _calls(root) == []
    assert not (root / ".secrets").exists()
