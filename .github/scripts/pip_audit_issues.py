#!/usr/bin/env python3
"""Turn a pip-audit JSON report into deduplicated GitHub security issues.

Used by the weekly pip-audit workflow (schedule/dispatch only — never PR CI).
Idempotent: existing open issues mentioning an advisory id are skipped.

Library failures raise ``GhCommandError``; the CLI entry point maps that
to exit code 1 with the message on stderr.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

GH_TIMEOUT_S = 60  # generous ceiling: gh talks to GitHub over the network


class GhCommandError(RuntimeError):
    """A gh CLI invocation failed (missing binary or nonzero exit)."""


def gh(*args: str) -> str:
    """Run a gh CLI command and return its stdout.

    Args:
        *args: The gh subcommand and its arguments.

    Returns:
        The command's stdout.

    Raises:
        GhCommandError: If the gh binary is missing or the command exits nonzero.
    """
    gh_bin = shutil.which("gh")  # resolve to an absolute path — no partial-path exec
    if gh_bin is None:
        raise GhCommandError("gh CLI not available")
    proc = subprocess.run([gh_bin, *args], capture_output=True, text=True, timeout=GH_TIMEOUT_S)
    if proc.returncode != 0:
        raise GhCommandError(f"gh {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout


def ensure_labels() -> None:
    """Create the security and P1 labels if missing (idempotent)."""
    gh("label", "create", "security", "--color", "b60205", "--description", "Dependency vulnerability (pip-audit)", "--force")
    gh("label", "create", "P1", "--color", "d93f0b", "--description", "Priority 1", "--force")


def issue_exists(vuln_id: str) -> bool:
    """Return True when an open issue already mentions the advisory id."""
    return vuln_id in gh("issue", "list", "--state", "open", "--search", vuln_id)


def main(report_path: str) -> int:
    """File one GitHub issue per new advisory in a pip-audit JSON report.

    Args:
        report_path: Path to the pip-audit JSON report.

    Returns:
        Exit code: 0 after processing the report.

    Raises:
        GhCommandError: If any gh CLI call fails.
        json.JSONDecodeError: If the report is not valid JSON.
        FileNotFoundError: If the report path does not exist.
    """
    ensure_labels()

    deps = json.loads(Path(report_path).read_text()).get("dependencies", [])
    rows = []
    for dep in deps:
        for vuln in dep.get("vulns", []):
            vid = vuln.get("id", "unknown")
            name = dep.get("name", "?")
            fixes = ", ".join(vuln.get("fix_versions") or []) or "no fix available"
            if issue_exists(vid):
                rows.append(f"| {vid} | {name} | duplicate-skipped |")
                continue
            body = "\n".join(
                [
                    "| Field | Value |",
                    "|---|---|",
                    f"| ID | {vid} |",
                    f"| Package | {name} |",
                    f"| Installed version | {dep.get('version', '?')} |",
                    f"| Fix versions | {fixes} |",
                    f"| Description | {vuln.get('description', '')} |",
                    f"| Link | {vuln.get('url', '')} |",
                ]
            )
            gh(
                "issue",
                "create",
                "--title",
                f"security: {vid} {name} {fixes}",
                "--body",
                body,
                "--label",
                "security",
                "--label",
                "P1",
            )
            rows.append(f"| {vid} | {name} | created |")

    print("## pip-audit weekly — dependency CVE issues")
    print()
    if rows:
        print("| ID | Package | Action |")
        print("|---|---|---|")
        print("\n".join(rows))
    else:
        print("Vulnerability records: **0** — no issues filed.")
    return 0


if __name__ == "__main__":
    try:
        code = main(sys.argv[1] if len(sys.argv) > 1 else "pip-audit-report.json")
    except GhCommandError as exc:
        print(f"error: {exc}", file=sys.stderr)
        code = 1
    raise SystemExit(code)
