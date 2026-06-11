"""Gemeinsamer MySQL-Connect mit Unix-Socket-Fallback (Vorlage: gii_sync.py).

Ruft load_env() NICHT automatisch auf — das machen die Skripte explizit.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import mysql.connector

UNIX_SOCKET_DEFAULT = "/var/run/mysqld/mysqld.sock"


def get_db(autocommit: bool = True):
    """Verbindet zur DB: erst TCP via DB_HOST, bei Fehler Unix-Socket-Fallback."""
    kw: dict = dict(
        user=os.environ.get("DB_USER", "root"),
        password=os.environ.get("DB_PASSWORD", ""),
        database=os.environ.get("DB_NAME", "respublica_gesetze"),
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
        autocommit=autocommit,
    )
    try:
        return mysql.connector.connect(
            host=os.environ.get("DB_HOST", "localhost"), **kw
        )
    except mysql.connector.Error:
        sock = os.environ.get("DB_UNIX_SOCKET", UNIX_SOCKET_DEFAULT)
        if not Path(sock).exists():
            raise
        return mysql.connector.connect(unix_socket=sock, **kw)


@contextmanager
def with_connection(autocommit: bool = True):
    """Contextmanager: yield Verbindung, schliesst sie am Ende garantiert."""
    conn = get_db(autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()
