"""Unit tests for the weekly pip-audit CI helper (.github/scripts/pip_audit_issues.py).

The script lives outside the ``scrape_kit`` package, so it is loaded via
importlib from its path. Every gh CLI interaction is faked — an in-process
callable for function-level tests and a recording shell stub on PATH for the
CLI (subprocess) tests — so the suite runs fully offline in well under a
second.

Failure contract: library functions raise ``GhCommandError``; only the CLI
entry point maps that to exit code 1 with the message on stderr.
"""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

pytestmark = pytest.mark.p0

SCRIPT_PATH = Path(__file__).resolve().parents[1] / ".github" / "scripts" / "pip_audit_issues.py"

GH_STUB = """\
#!/bin/sh
if [ "$GH_MODE" = "fail" ]; then
    echo "gh: auth failed" >&2
    exit 1
fi
echo "$*" >> "$GH_LOG"
if [ "$1" = "issue" ] && [ "$2" = "list" ]; then
    printf '%s' "$GH_OPEN"
fi
"""


# ── Loading and factories ──────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def script() -> ModuleType:
    """The pip_audit_issues module, imported once by file path."""
    spec = importlib.util.spec_from_file_location("pip_audit_issues_under_test", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_vuln(
    vuln_id: str = "GHSA-aaaa-1111",
    fix_versions: list[str] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    """Build one pip-audit vulnerability record with optional overrides."""
    vuln = {
        "id": vuln_id,
        "fix_versions": fix_versions if fix_versions is not None else ["1.0.1"],
        "description": "RCE",
        "url": "https://example.com",
    }
    vuln.update(overrides)
    return vuln


def make_dep(
    name: str = "requests",
    version: str = "2.0.0",
    vulns: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build one pip-audit dependency record carrying a vulnerability list."""
    return {
        "name": name,
        "version": version,
        "vulns": vulns if vulns is not None else [make_vuln()],
    }


def make_report(tmp_path: Path, payload: dict[str, Any]) -> Path:
    """Write a pip-audit JSON report and return its path."""
    report = tmp_path / "pip-audit-report.json"
    report.write_text(json.dumps(payload), encoding="utf-8")
    return report


class FakeGH:
    """Stand-in for the script's ``gh()``: records calls, serves canned stdout."""

    def __init__(self, open_issues: str = "") -> None:
        """Store the canned ``gh issue list`` output (empty means nothing open)."""
        self.calls: list[list[str]] = []
        self.open_issues = open_issues

    def __call__(self, *args: str) -> str:
        """Record the invocation and reply with canned stdout per subcommand."""
        self.calls.append(list(args))
        if args[:2] == ("issue", "list"):
            return self.open_issues
        return ""


def wire_gh(monkeypatch: pytest.MonkeyPatch, script: ModuleType, fake: FakeGH) -> None:
    """Point the script's ``gh`` calls at ``fake``."""
    monkeypatch.setattr(script, "gh", fake)


def run_cli(
    gh_bin: Path,
    cwd: Path,
    args: list[str],
    extra_env: dict[str, str],
    inherit_path: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run the script as a subprocess with ``gh_bin`` first on PATH."""
    base = os.environ.get("PATH", "")
    env = {
        **os.environ,
        "PATH": f"{gh_bin}{os.pathsep}{base}" if inherit_path else str(gh_bin),
        "PYTHONDONTWRITEBYTECODE": "1",
        **extra_env,
    }
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


@pytest.fixture(scope="module")
def gh_stub_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A temp bin dir containing an executable fake ``gh`` shell stub."""
    bin_dir = tmp_path_factory.mktemp("gh_bin")
    stub = bin_dir / "gh"
    stub.write_text(GH_STUB, encoding="utf-8")
    stub.chmod(0o755)
    return bin_dir


# ── gh() helper ────────────────────────────────────────────────────────────────


class TestGhHelper:
    """``gh()`` returns stdout on success and raises GhCommandError on failure."""

    def test_returns_stdout_on_success(self, script, monkeypatch):
        """A zero returncode yields the command's stdout."""
        monkeypatch.setattr(
            script.subprocess,
            "run",
            lambda *_a, **_k: SimpleNamespace(returncode=0, stdout="ok", stderr=""),
        )
        monkeypatch.setattr(script.shutil, "which", lambda _name: "/usr/local/bin/gh")
        assert script.gh("auth", "status") == "ok"

    def test_uses_absolute_gh_path_and_timeout(self, script, monkeypatch):
        """The subprocess call uses the resolved absolute path and a timeout."""
        calls: list[tuple[list[str], dict[str, Any]]] = []

        def fake_run(cmd: list[str], **kwargs: Any) -> SimpleNamespace:
            calls.append((cmd, kwargs))
            return SimpleNamespace(returncode=0, stdout="ok", stderr="")

        monkeypatch.setattr(script.subprocess, "run", fake_run)
        monkeypatch.setattr(script.shutil, "which", lambda _name: "/usr/local/bin/gh")
        assert script.gh("label", "create", "security") == "ok"
        cmd, kwargs = calls[0]
        assert cmd[0] == "/usr/local/bin/gh"
        assert kwargs["timeout"] == script.GH_TIMEOUT_S

    def test_raises_gh_command_error_on_failure(self, script, monkeypatch):
        """A nonzero returncode raises GhCommandError naming the command and stderr."""
        monkeypatch.setattr(
            script.subprocess,
            "run",
            lambda *_a, **_k: SimpleNamespace(returncode=1, stdout="", stderr="boom\n"),
        )
        monkeypatch.setattr(script.shutil, "which", lambda _name: "/usr/local/bin/gh")
        with pytest.raises(script.GhCommandError, match="gh issue list failed: boom"):
            script.gh("issue", "list")

    def test_raises_when_gh_missing(self, script, monkeypatch):
        """A missing gh binary raises GhCommandError before any subprocess call."""
        monkeypatch.setattr(script.shutil, "which", lambda _name: None)
        with pytest.raises(script.GhCommandError, match="gh CLI not available"):
            script.gh("issue", "list")


# ── ensure_labels / issue_exists ─────────────────────────────────────────────


class TestLabelAndDedupHelpers:
    """Label setup and open-issue lookup talk to gh with exact arguments."""

    def test_ensure_labels_creates_security_and_p1(self, script, monkeypatch):
        """``ensure_labels`` issues the two ``--force`` label-create commands."""
        fake = FakeGH()
        monkeypatch.setattr(script, "gh", fake)
        script.ensure_labels()
        assert fake.calls == [
            [
                "label",
                "create",
                "security",
                "--color",
                "b60205",
                "--description",
                "Dependency vulnerability (pip-audit)",
                "--force",
            ],
            [
                "label",
                "create",
                "P1",
                "--color",
                "d93f0b",
                "--description",
                "Priority 1",
                "--force",
            ],
        ]

    def test_issue_exists_matches_open_issue_by_substring(self, script, monkeypatch):
        """Lookup hits open issues via ``--state open --search`` substring match."""
        fake = FakeGH(open_issues="| #42 | security: GHSA-aaaa-1111 requests 1.0.1 |")
        monkeypatch.setattr(script, "gh", fake)
        assert script.issue_exists("GHSA-aaaa-1111") is True
        assert script.issue_exists("GHSA-zzzz-9999") is False
        assert fake.calls[-1] == [
            "issue",
            "list",
            "--state",
            "open",
            "--search",
            "GHSA-zzzz-9999",
        ]


# ── main() ────────────────────────────────────────────────────────────────────


class TestMain:
    """``main`` files new CVEs, skips duplicates and reports a summary."""

    def test_creates_issue_with_title_body_and_labels(self, script, monkeypatch, tmp_path, capsys):
        """A new vulnerability gets one issue with joined fix versions and labels."""
        fake = FakeGH()
        wire_gh(monkeypatch, script, fake)
        report = make_report(
            tmp_path,
            {
                "dependencies": [
                    make_dep(
                        version="2.0.0",
                        vulns=[make_vuln(fix_versions=["1.0.1", "2.0.0"])],
                    )
                ]
            },
        )

        assert script.main(str(report)) == 0
        creates = [c for c in fake.calls if c[:2] == ["issue", "create"]]
        assert len(creates) == 1
        create = creates[0]
        assert create[create.index("--title") + 1] == "security: GHSA-aaaa-1111 requests 1.0.1, 2.0.0"
        body = create[create.index("--body") + 1]
        assert "| ID | GHSA-aaaa-1111 |" in body
        assert "| Package | requests |" in body
        assert "| Installed version | 2.0.0 |" in body
        assert "| Fix versions | 1.0.1, 2.0.0 |" in body
        assert "| Description | RCE |" in body
        assert "| Link | https://example.com |" in body
        assert create[create.index("--label") :] == [
            "--label",
            "security",
            "--label",
            "P1",
        ]
        out = capsys.readouterr().out
        assert "| ID | Package | Action |" in out
        assert "| GHSA-aaaa-1111 | requests | created |" in out

    @pytest.mark.smoke
    def test_skips_vulnerability_when_issue_already_open(self, script, monkeypatch, tmp_path, capsys):
        """An advisory id already present in open issues is not re-filed."""
        fake = FakeGH(open_issues="security: GHSA-old requests ...")
        wire_gh(monkeypatch, script, fake)
        dep = make_dep(
            vulns=[
                make_vuln(vuln_id="GHSA-old", fix_versions=["2.31.0"]),
                make_vuln(vuln_id="GHSA-new", fix_versions=["9.9.9"]),
            ]
        )
        report = make_report(tmp_path, {"dependencies": [dep]})

        assert script.main(str(report)) == 0
        creates = [c for c in fake.calls if c[:2] == ["issue", "create"]]
        assert len(creates) == 1
        assert creates[0][creates[0].index("--title") + 1] == "security: GHSA-new requests 9.9.9"
        out = capsys.readouterr().out
        assert "| GHSA-old | requests | duplicate-skipped |" in out
        assert "| GHSA-new | requests | created |" in out

    def test_defaults_for_missing_fields(self, script, monkeypatch, tmp_path):
        """Missing id/name/version/fixes fall back to documented placeholders."""
        fake = FakeGH()
        wire_gh(monkeypatch, script, fake)
        report = make_report(tmp_path, {"dependencies": [{"vulns": [{}]}]})

        script.main(str(report))
        create = next(c for c in fake.calls if c[:2] == ["issue", "create"])
        assert create[create.index("--title") + 1] == "security: unknown ? no fix available"
        body = create[create.index("--body") + 1]
        assert "| ID | unknown |" in body
        assert "| Package | ? |" in body
        assert "| Installed version | ? |" in body
        assert "| Fix versions | no fix available |" in body

    @pytest.mark.parametrize(
        "payload",
        [
            {"other": 1},
            {"dependencies": []},
            {"dependencies": [{"name": "clean", "version": "1.0", "vulns": []}]},
        ],
        ids=["no-dependencies-key", "empty-dependencies", "dependency-without-vulns"],
    )
    def test_zero_findings_file_no_issues(self, script, monkeypatch, tmp_path, capsys, payload):
        """Reports without vulnerabilities file nothing and print the zero summary."""
        fake = FakeGH()
        wire_gh(monkeypatch, script, fake)
        report = make_report(tmp_path, payload)

        assert script.main(str(report)) == 0
        assert not [c for c in fake.calls if c[:2] == ["issue", "create"]]
        assert fake.calls == [c for c in fake.calls if c[:2] == ["label", "create"]]
        assert "Vulnerability records: **0** — no issues filed." in capsys.readouterr().out

    def test_main_raises_when_gh_missing(self, script, monkeypatch, tmp_path):
        """Without gh on PATH, main aborts on the first gh call."""
        monkeypatch.setattr(script.shutil, "which", lambda _name: None)
        report = make_report(tmp_path, {"dependencies": []})

        with pytest.raises(script.GhCommandError, match="gh CLI not available"):
            script.main(str(report))

    def test_propagates_corrupt_and_missing_report(self, script, monkeypatch, tmp_path):
        """Corrupt JSON and absent report files surface their raw errors."""
        wire_gh(monkeypatch, script, FakeGH())
        corrupt = tmp_path / "corrupt.json"
        corrupt.write_text("{not json", encoding="utf-8")
        with pytest.raises(json.JSONDecodeError):
            script.main(str(corrupt))
        with pytest.raises(FileNotFoundError):
            script.main(str(tmp_path / "missing.json"))


# ── CLI (subprocess) ──────────────────────────────────────────────────────────


class TestCli:
    """The script's command-line entry point wires report path and exit codes."""

    def test_exit_zero_and_summary_table(self, gh_stub_dir, tmp_path):
        """A normal run exits 0, creates one issue and prints the summary table."""
        report = make_report(tmp_path, {"dependencies": [make_dep()]})
        proc = run_cli(
            gh_stub_dir,
            tmp_path,
            [str(report)],
            {"GH_LOG": str(tmp_path / "gh.log"), "GH_OPEN": ""},
        )
        assert proc.returncode == 0
        assert "## pip-audit weekly — dependency CVE issues" in proc.stdout
        assert "| GHSA-aaaa-1111 | requests | created |" in proc.stdout

    def test_default_report_path_when_arg_omitted(self, gh_stub_dir, tmp_path):
        """Without argv[1] the script reads ./pip-audit-report.json."""
        (tmp_path / "pip-audit-report.json").write_text(json.dumps({"dependencies": [make_dep()]}), encoding="utf-8")
        proc = run_cli(
            gh_stub_dir,
            tmp_path,
            [],
            {"GH_LOG": str(tmp_path / "gh.log"), "GH_OPEN": ""},
        )
        assert proc.returncode == 0
        assert "| GHSA-aaaa-1111 | requests | created |" in proc.stdout

    def test_exit_nonzero_when_gh_not_on_path(self, tmp_path):
        """A PATH without gh makes the run exit 1 with the availability message."""
        empty_bin = tmp_path / "empty_bin"
        empty_bin.mkdir()
        report = make_report(tmp_path, {"dependencies": [make_dep()]})
        proc = run_cli(
            empty_bin,
            tmp_path,
            [str(report)],
            {"GH_OPEN": "", "GH_LOG": str(tmp_path / "gh.log")},
            inherit_path=False,
        )
        assert proc.returncode == 1
        assert "gh CLI not available" in proc.stderr

    def test_exit_nonzero_when_gh_fails(self, gh_stub_dir, tmp_path):
        """A failing gh command exits 1 with the gh stderr surfaced."""
        report = make_report(tmp_path, {"dependencies": [make_dep()]})
        proc = run_cli(
            gh_stub_dir,
            tmp_path,
            [str(report)],
            {"GH_LOG": str(tmp_path / "gh.log"), "GH_OPEN": "", "GH_MODE": "fail"},
        )
        assert proc.returncode == 1
        assert "gh: auth failed" in proc.stderr
