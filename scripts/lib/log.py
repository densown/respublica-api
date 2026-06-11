"""Logging, PID-Lock und Signal-Handler fuer Pipeline-Skripte.

Patterns aus summarize_gesetze_resilient.py uebernommen.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
from typing import Callable

from .env import ROOT

LOG_DIR = ROOT / "logs"
_FORMAT = "%(asctime)s %(levelname)s %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Logger mit Datei-Handler (logs/{name}.log) und stdout-Handler."""
    logger = logging.getLogger(name)
    if logger.handlers:  # bereits konfiguriert
        return logger
    logger.setLevel(logging.INFO)
    logger.propagate = False
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter(_FORMAT)
    fh = logging.FileHandler(LOG_DIR / f"{name}.log", encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def _pid_file(name: str):
    return LOG_DIR / f"{name}.pid"


def acquire_lock(name: str, logger: logging.Logger | None = None) -> bool:
    """Single-Process-Lock via PID-File.

    True = Lock erhalten. False = anderer Prozess laeuft bereits.
    Stale PID-Files (Prozess existiert nicht mehr) werden ueberschrieben.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    pid_file = _pid_file(name)
    my_pid = os.getpid()
    if pid_file.exists():
        try:
            old_pid = int(pid_file.read_text().strip())
            if old_pid != my_pid:  # nicht die eigene PID
                os.kill(old_pid, 0)  # signal 0 = check ob Prozess existiert
                msg = f"Anderer Prozess laeuft bereits (PID {old_pid}). Abbruch."
                if logger:
                    logger.error(msg)
                else:
                    print(f"FEHLER: {msg}", file=sys.stderr)
                return False
        except (OSError, ValueError):
            pass  # PID-File stale, ok
    pid_file.write_text(str(my_pid))
    return True


def release_lock(name: str) -> None:
    """Entfernt das PID-File (nur wenn es zur eigenen PID gehoert)."""
    pid_file = _pid_file(name)
    try:
        if pid_file.exists() and int(pid_file.read_text().strip()) == os.getpid():
            pid_file.unlink()
    except (OSError, ValueError):
        pass


def install_signal_handlers(callback: Callable[[], None]) -> None:
    """Registriert SIGTERM/SIGINT; callback wird bei Signal aufgerufen."""

    def _handler(signum, frame):
        print(f"\n[SIGNAL] {signum} empfangen, beende sauber...", flush=True)
        callback()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
