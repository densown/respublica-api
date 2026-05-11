"""Download und XML-Parsing fuer gesetze-im-internet.de (lxml)."""

from __future__ import annotations

import io
import logging
import re
import time
import zipfile
from typing import Any

import requests
from lxml import etree

GII_TOC_URL = "https://www.gesetze-im-internet.de/gii-toc.xml"
GII_XML_ZIP_URL = "https://www.gesetze-im-internet.de/{slug}/xml.zip"
REQUEST_SLEEP_SEC = 0.1

logger = logging.getLogger(__name__)


def slug_from_link(link: str) -> str:
    link = (link or "").strip()
    m = re.search(
        r"gesetze-im-internet\.de/([^/]+)/xml\.zip",
        link,
        flags=re.IGNORECASE,
    )
    if m:
        return m.group(1)
    link = re.sub(r"^https?://(www\.)?", "", link, flags=re.IGNORECASE)
    link = link.replace("http://", "").replace("https://", "")
    if "/" in link:
        parts = link.rstrip("/").split("/")
        slug = parts[-2] if parts[-1].lower() == "xml.zip" else parts[-1].replace(".zip", "")
        return slug.replace("/xml.zip", "")
    return ""


def parse_toc_bytes(content: bytes) -> list[dict[str, str]]:
    root = etree.fromstring(content)
    out: list[dict[str, str]] = []
    for item in root.findall("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        slug = slug_from_link(link)
        if not slug:
            continue
        out.append({"slug": slug, "title": title, "link": link})
    return out


def fetch_toc(session: requests.Session, timeout: int = 60) -> list[dict[str, str]]:
    r = session.get(GII_TOC_URL, timeout=timeout)
    r.raise_for_status()
    time.sleep(REQUEST_SLEEP_SEC)
    return parse_toc_bytes(r.content)


def fetch_law_tree(
    session: requests.Session, slug: str, timeout: int = 90
) -> etree._ElementTree | None:
    url = GII_XML_ZIP_URL.format(slug=slug)
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    time.sleep(REQUEST_SLEEP_SEC)
    buf = io.BytesIO(r.content)
    try:
        with zipfile.ZipFile(buf) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            if not names:
                return None
            names.sort()
            with zf.open(names[0]) as fh:
                return etree.parse(fh)
    except (zipfile.BadZipFile, OSError, etree.XMLSyntaxError) as e:
        logger.warning("ZIP/XML Fehler slug=%s: %s", slug, e)
        return None


def _fundstelle(meta: etree._Element) -> tuple[str, str]:
    fs = meta.find("fundstelle")
    if fs is None:
        return "", ""
    per = (fs.findtext("periodikum") or "").strip()
    zit = (fs.findtext("zitstelle") or "").strip()
    return per, zit


def extract_letzter_stand(meta: etree._Element) -> str | None:
    for stand in meta.findall("standangabe"):
        typ = (stand.findtext("standtyp") or "").strip()
        if typ == "Stand":
            txt = (stand.findtext("standkommentar") or "").strip()
            return txt or None
    return None


def extract_metadata(tree: etree._ElementTree) -> dict[str, Any] | None:
    root = tree.getroot()
    doknr = (root.get("doknr") or "").strip()
    builddate = (root.get("builddate") or "").strip()
    first_norm = root.find("norm")
    if first_norm is None:
        return None
    meta = first_norm.find("metadaten")
    if meta is None:
        return None
    per, zit = _fundstelle(meta)
    ausf = (meta.findtext("ausfertigung-datum") or "").strip()
    return {
        "doknr": doknr or (first_norm.get("doknr") or "").strip(),
        "builddate": builddate or (first_norm.get("builddate") or "").strip(),
        "langue": (meta.findtext("langue") or "").strip(),
        "jurabk": (meta.findtext("jurabk") or "").strip(),
        "amtabk": (meta.findtext("amtabk") or "").strip(),
        "ausfertigung_datum": ausf,
        "fundstelle_periodikum": per,
        "fundstelle_zitstelle": zit,
        "letzter_stand": extract_letzter_stand(meta),
    }


def save_toc_cache(path: str, content: bytes) -> None:
    with open(path, "wb") as f:
        f.write(content)
