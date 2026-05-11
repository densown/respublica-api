#!/usr/bin/env python3
"""
Einmaliger oder begrenzter Vollimport (ohne builddate-Skip).
Beispiel: python3 gii_initial_import.py --limit 10
"""

from __future__ import annotations

import argparse
import sys

from gii_sync import run_sync


def main() -> int:
    p = argparse.ArgumentParser(description="GII Initialimport")
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Nur die ersten N Eintraege aus gii-toc.xml",
    )
    args = p.parse_args()
    return run_sync(initial_import=True, limit=args.limit)


if __name__ == "__main__":
    raise SystemExit(main())
