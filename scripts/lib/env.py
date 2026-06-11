"""Projekt-Root und .env-Loading fuer alle Pipeline-Skripte."""
from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent  # /root/apps/gesetze


def load_env() -> None:
    """Laedt ROOT/.env; Fallback auf Standard-Suche von python-dotenv."""
    env_file = ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file)
    else:
        load_dotenv()
