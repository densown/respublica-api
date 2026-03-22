#!/usr/bin/env python3
"""
Backfill: Geht Commits der letzten 30 Tage im kmein/gesetze-Repo durch,
schreibt Diffs für laws/* in respublica_gesetze (gesetze, aenderungen).
Idempotent bei gleichem (gesetz_id, datum, diff).
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
REPO_PATH = Path("/root/apps/gesetze/data/kmein-gesetze")
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def load_env() -> None:
    load_dotenv(ROOT / ".env")


def connect():
    host = os.environ.get("DB_HOST", "localhost")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD", "")
    database = os.environ.get("DB_NAME", "respublica_gesetze")
    if not user:
        print("Fehler: DB_USER fehlt in .env", file=sys.stderr)
        sys.exit(1)
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        autocommit=False,
    )


def kuerzel_und_pfad(rel_path: str) -> tuple[str, str]:
    return Path(rel_path).stem, rel_path


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


def git_commits_last_30_days(repo: Path) -> list[tuple[str, str]]:
    """Liste (commit_hash, author_date_iso) aus git log."""
    cp = run_git(
        repo,
        "log",
        "--since=30 days ago",
        "--format=%H %ai",
        "master",
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )
    out: list[tuple[str, str]] = []
    for line in cp.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(maxsplit=1)
        h = parts[0]
        ai = parts[1].strip() if len(parts) > 1 else ""
        out.append((h, ai))
    return out


def parent_revision(repo: Path, commit_hash: str) -> str:
    cp = run_git(
        repo,
        "rev-parse",
        f"{commit_hash}^",
        check=False,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )
    p = cp.stdout.strip()
    if cp.returncode == 0 and p:
        return p
    return EMPTY_TREE


def files_changed_in_commit(repo: Path, commit_hash: str) -> list[str]:
    cp = run_git(
        repo,
        "diff-tree",
        "--no-commit-id",
        "-r",
        "--name-only",
        commit_hash,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )
    return [
        f.strip()
        for f in cp.stdout.splitlines()
        if f.strip().startswith("laws/")
    ]


def git_diff_file(repo: Path, parent: str, commit: str, rel_path: str) -> str:
    """Diff zwischen Parent- und Commit-Tree für eine Datei (Inhalt dieser Revision)."""
    cp = run_git(
        repo,
        "diff",
        parent,
        commit,
        "--",
        rel_path,
        check=False,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )
    if cp.returncode != 0:
        return ""
    return cp.stdout


def commit_date_from_ai(ai: str) -> date:
    """%ai beginnt mit YYYY-MM-DD."""
    if len(ai) >= 10 and ai[4] == "-" and ai[7] == "-":
        return date.fromisoformat(ai[:10])
    raise ValueError(f"Unerwartetes Datumsformat: {ai!r}")


def main() -> int:
    load_env()

    if not REPO_PATH.is_dir() or not (REPO_PATH / ".git").exists():
        print(
            f"Repository fehlt oder kein Git-Repo: {REPO_PATH}",
            file=sys.stderr,
        )
        return 1

    commits = git_commits_last_30_days(REPO_PATH)
    neue_gesetze = 0
    neue_aenderungen = 0

    conn = connect()
    try:
        cur = conn.cursor()
        for commit_hash, ai in commits:
            try:
                datum = commit_date_from_ai(ai)
            except ValueError:
                continue
            parent = parent_revision(REPO_PATH, commit_hash)
            for rel_path in files_changed_in_commit(REPO_PATH, commit_hash):
                diff_text = git_diff_file(REPO_PATH, parent, commit_hash, rel_path)
                if not diff_text.strip():
                    continue

                kuerzel, pfad = kuerzel_und_pfad(rel_path)

                cur.execute(
                    "INSERT IGNORE INTO gesetze (kuerzel, pfad) VALUES (%s, %s)",
                    (kuerzel, pfad),
                )
                neue_gesetze += cur.rowcount

                cur.execute(
                    "SELECT id FROM gesetze WHERE kuerzel = %s LIMIT 1",
                    (kuerzel,),
                )
                row = cur.fetchone()
                if not row:
                    continue
                gesetz_id = row[0]

                cur.execute(
                    """
                    INSERT INTO aenderungen (gesetz_id, datum, diff)
                    SELECT %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM aenderungen
                        WHERE gesetz_id <=> %s AND datum = %s AND diff <=> %s
                    )
                    """,
                    (gesetz_id, datum, diff_text, gesetz_id, datum, diff_text),
                )
                neue_aenderungen += cur.rowcount

        conn.commit()
    finally:
        conn.close()

    print(
        f"Fertig: {neue_gesetze} neue Gesetze, {neue_aenderungen} neue Änderungen "
        "geschrieben (30-Tage-Backfill)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
