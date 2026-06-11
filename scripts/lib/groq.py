"""Groq-Chat-Client mit voller Retry-Logik (aus summarize_gesetze_resilient.py).

5 Versuche, exponentieller Backoff, 429-Handling via retry-after Header,
Anti-Abuse: bei retry-after > 60 oder x-should-retry: false sofort Exception.
"""
from __future__ import annotations

import os
import time
from typing import Any

import requests

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
MAX_RETRIES = 5
BACKOFF_BASE = 10  # Sekunden, wird exponentiell
UA = "gesetze-lib-groq/1.0"


class GroqError(RuntimeError):
    """Nicht-retrybarer Groq-Fehler (Anti-Abuse, HTTP-Fehler, leere Antwort)."""


def _request(api_key: str, body: dict[str, Any]) -> requests.Response:
    return requests.post(
        GROQ_URL,
        json=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": UA,
        },
        timeout=120,
    )


def _extract_content(payload: dict[str, Any]) -> str:
    choices = payload.get("choices") or []
    if not choices:
        err = payload.get("error") or payload
        raise GroqError(f"Keine Antwort von Groq: {err!r}")
    msg = choices[0].get("message") or {}
    content = msg.get("content")
    if not content or not isinstance(content, str):
        raise GroqError("Antwort ohne Text")
    return content.strip()


def chat_completion(
    messages: list[dict[str, str]],
    model: str | None = None,
    max_tokens: int = 2048,
    temperature: float = 0.3,
) -> str:
    """Sendet einen Chat-Request an Groq und gibt den Antworttext zurueck."""
    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        raise GroqError("GROQ_API_KEY fehlt in .env")

    body: dict[str, Any] = {
        "model": model or os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = _request(api_key, body)
        except requests.RequestException as e:
            last_error = e
            wait = BACKOFF_BASE * (2 ** attempt)
            print(f"  [NETZ] {e}, attempt={attempt+1}/{MAX_RETRIES}, warte {wait}s...", flush=True)
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            retry_after = resp.headers.get("retry-after", "")
            should_retry = resp.headers.get("x-should-retry", "true").lower()
            try:
                ra_int = int(retry_after) if retry_after else 0
            except ValueError:
                ra_int = 0
            # Anti-Abuse-Erkennung: lange Pause oder explizit kein retry
            if ra_int > 60 or should_retry == "false":
                raise GroqError(
                    f"Anti-Abuse-Lock: retry-after={retry_after}s "
                    f"should-retry={should_retry}, kein Retry"
                )
            # Normal: respect retry-after wenn vorhanden, sonst exp backoff
            wait = ra_int if ra_int > 0 else BACKOFF_BASE * (2 ** attempt)
            print(f"  [429] attempt={attempt+1}/{MAX_RETRIES}, warte {wait}s...", flush=True)
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            raise GroqError(f"HTTP {resp.status_code}: {resp.text[:500]}")

        return _extract_content(resp.json())

    raise GroqError(f"Aufgegeben nach {MAX_RETRIES} Versuchen: {last_error}")
