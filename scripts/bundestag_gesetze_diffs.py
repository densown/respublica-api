#!/usr/bin/env python3
"""
Klont/aktualisiert bundestag/gesetze, sammelt Dateien mit Commits in den letzten 24 Stunden,
schreibt die zugehörigen git diffs als JSON und gibt eine Kurzstatistik aus.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

REPO_URL = "https://github.com/bundestag/gesetze.git"
REPO_PATH = Path("/root/apps/gesetze/data/bundestag-gesetze")
DIFFS_DIR = Path("/root/apps/gesetze/data/diffs")
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def run_git(
    cwd: Path,
    *args: str,
    check: bool = True,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=check,
        capture_output=True,
        text=True,
        env=env,
    )


def ensure_repo() -> None:
    REPO_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not REPO_PATH.exists():
        run_git(REPO_PATH.parent, "clone", REPO_URL, str(REPO_PATH))
    else:
        run_git(
            REPO_PATH,
            "pull",
            "--no-edit",
            "--quiet",
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        )


def files_changed_last_24h() -> list[str]:
    cp = run_git(
        REPO_PATH,
        "log",
        "--since=24 hours ago",
        "--name-only",
        "--pretty=format:",
    )
    paths = {line.strip() for line in cp.stdout.splitlines() if line.strip()}
    return sorted(paths)


def diff_base_commit() -> str:
    """Commit, auf den sich der Diff bezieht (Stand vor 24h bzw. Anfang des Zeitraums)."""
    cp = run_git(REPO_PATH, "rev-list", "-1", "--before=24 hours ago", "HEAD", check=False)
    base = cp.stdout.strip()
    if base:
        return base
    # Alle Commits liegen in den letzten 24h: ältesten Commit der Periode finden, von dessen Eltern diffen.
    rev = run_git(
        REPO_PATH,
        "rev-list",
        "--reverse",
        "--since=24 hours ago",
        "HEAD",
    )
    hashes = [h for h in rev.stdout.splitlines() if h.strip()]
    if not hashes:
        return EMPTY_TREE
    oldest = hashes[0]
    parent = run_git(REPO_PATH, "rev-parse", f"{oldest}^", check=False)
    p = parent.stdout.strip()
    if parent.returncode == 0 and p:
        return p
    return EMPTY_TREE


def git_diff_for_file(base: str, rel_path: str) -> str:
    cp = run_git(REPO_PATH, "diff", f"{base}..HEAD", "--", rel_path, check=False)
    if cp.returncode != 0:
        return cp.stderr.strip() or f"(git diff fehlgeschlagen, exit {cp.returncode})"
    return cp.stdout


def law_key(rel_path: str) -> str:
    """
    Ein Gesetz entspricht in diesem Repo typischerweise dem Ordner unter dem Buchstabenpräfix,
    z. B. b/badv/... -> b/badv. Root-Dateien zählen als eigener Schlüssel.
    """
    parts = rel_path.split("/")
    if len(parts) >= 2:
        return f"{parts[0]}/{parts[1]}"
    return rel_path


def main() -> int:
    ensure_repo()
    DIFFS_DIR.mkdir(parents=True, exist_ok=True)

    files = files_changed_last_24h()
    base = diff_base_commit()

    entries: list[dict[str, str]] = []
    for rel in files:
        diff_text = git_diff_for_file(base, rel)
        entries.append({"path": rel, "diff": diff_text})

    today = date.today().isoformat()
    out_file = DIFFS_DIR / f"{today}.json"

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo": REPO_URL,
        "repo_path": str(REPO_PATH),
        "diff_base": base,
        "window": "24 hours (git log --since)",
        "file_count": len(files),
        "files": entries,
    }

    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    laws = {law_key(p) for p in files}
    print(f"JSON gespeichert: {out_file}")
    print(f"Geänderte Dateien (24h): {len(files)}")
    print(f"Geänderte Gesetze (geschätzt nach Pfad …/…): {len(laws)}")
    if laws and len(files) <= 30:
        for k in sorted(laws):
            print(f"  - {k}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
