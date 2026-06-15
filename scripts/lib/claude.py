"""Wrapper um die Claude-CLI (`claude --print`) fuer die Summarizer-Skripte (M-008).

Vereinheitlicht die zuvor 3x nahezu identisch duplizierte call_claude()-Funktion.
Nutzt die CLI im Max-Plan-Modus (ANTHROPIC_API_KEY wird aus der Umgebung entfernt,
damit nicht versehentlich die kostenpflichtige API verwendet wird).
"""

from __future__ import annotations

import os
import subprocess
from typing import Callable


def call_claude(
    prompt: str,
    *,
    timeout: int = 120,
    log: Callable[[str], None] | None = None,
) -> str | None:
    """Ruft ``claude --print -p <prompt>`` auf.

    Gibt stdout (gestrippt) zurueck, oder ``None`` bei Nicht-Null-Exit,
    Timeout oder fehlender CLI. ``log`` ist ein optionales Callable(msg)
    fuer Fehlermeldungen (z.B. die log()-Funktion des aufrufenden Skripts).
    """
    _log = log if callable(log) else (lambda _m: None)
    try:
        env = os.environ.copy()
        env.pop("ANTHROPIC_API_KEY", None)  # Max Plan, nicht API
        result = subprocess.run(
            ["claude", "--print", "-p", prompt],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        if result.returncode != 0:
            _log(f"  claude stderr: {result.stderr.strip()[:300]}")
            return None
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        _log(f"  claude timeout ({timeout}s)")
        return None
    except FileNotFoundError:
        _log("  claude CLI nicht gefunden")
        return None
