from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Optional

import httpx


_WS = re.compile(r"\s+")


def _clean_text(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("\u00a0", " ")
    s = _WS.sub(" ", s).strip()
    return s


def _google_books_description(*, title: str, authors: str, api_key: Optional[str]) -> str:
    """
    Kyobo product pages often require browser JS and can return an empty body to bots.
    Use Google Books as a robust source for the description text.
    """
    t = _clean_text(title)
    a = _clean_text(authors)
    if not t:
        return ""
    q = f"intitle:{t}"
    if a:
        # keep only first author token for better match
        first = a.split(",")[0].strip()
        if first:
            q += f"+inauthor:{first}"
    params = {"q": q, "maxResults": "5", "printType": "books", "orderBy": "relevance"}
    if api_key and api_key.strip():
        params["key"] = api_key.strip()
    try:
        r = httpx.get("https://www.googleapis.com/books/v1/volumes", params=params, timeout=20.0)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return ""
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return ""
    for it in items:
        vol = (it or {}).get("volumeInfo") if isinstance(it, dict) else None
        if not isinstance(vol, dict):
            continue
        desc = vol.get("description")
        if isinstance(desc, str) and desc.strip():
            return desc.strip()
    return ""


def _fallback_summary(description: str) -> str:
    d = (description or "").strip()
    if not d:
        return ""
    # Keep it short: roughly 1–2 lines in Telegram.
    return d[:180].rstrip() + ("…" if len(d) > 180 else "")


def enrich_catalog(
    *,
    catalog_path: str,
    overwrite_description: bool = False,
    overwrite_summary: bool = False,
    timeout_s: float = 20.0,
) -> int:
    p = Path(catalog_path)
    data: dict[str, Any] = {}
    if p.exists():
        data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit(f"Invalid catalog JSON shape: {catalog_path}")

    changed = 0
    api_key = os.environ.get("GOOGLE_BOOKS_API_KEY")
    for month, entry in data.items():
        if not isinstance(entry, dict):
            continue
        _ = month

        cur_desc = (entry.get("description") or "").strip()
        if not cur_desc or overwrite_description:
            title = (entry.get("title") or "").strip()
            authors = (entry.get("authors") or "").strip()
            desc = _google_books_description(title=title, authors=authors, api_key=api_key)
            if desc:
                entry["description"] = desc
                changed += 1

        cur_sum = (entry.get("summary") or "").strip()
        if not cur_sum or overwrite_summary:
            new_sum = _fallback_summary(entry.get("description") or "")
            if new_sum:
                entry["summary"] = new_sum
                changed += 1

    if changed:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return changed


def main() -> None:
    catalog_path = os.environ.get("BOOK_CATALOG_PATH", "./data/book_catalog.json")
    changed = enrich_catalog(catalog_path=catalog_path)
    print(f"updated_fields={changed}")


if __name__ == "__main__":
    main()

