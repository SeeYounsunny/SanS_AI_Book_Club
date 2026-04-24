from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class BookCatalogEntry:
    month: str  # YYYY-MM
    title: Optional[str] = None
    authors: Optional[str] = None
    isbn: Optional[str] = None
    page_count: Optional[str] = None
    published: Optional[str] = None
    publisher: Optional[str] = None
    info_link: Optional[str] = None
    trailer_link: Optional[str] = None  # legacy single link
    trailer_links: Optional[list[str]] = None
    description: Optional[str] = None
    summary: Optional[str] = None
    toc: Optional[str] = None
    meeting_at: Optional[str] = None

    def as_dict(self) -> dict:
        return {
            "month": self.month,
            "title": self.title,
            "authors": self.authors,
            "isbn": self.isbn,
            "page_count": self.page_count,
            "published": self.published,
            "publisher": self.publisher,
            "info_link": self.info_link,
            "trailer_link": self.trailer_link,
            "trailer_links": self.trailer_links,
            "description": self.description,
            "summary": self.summary,
            "toc": self.toc,
            "meeting_at": self.meeting_at,
        }


def load_book_catalog(path: str) -> dict[str, Any]:
    """
    Load catalog JSON.

    Expected shape:
      {
        "2026-04": { "title": "...", "meeting_at": "2026-04-10 20:00", ... },
        "2026-05": { ... }
      }
    """
    p = Path(path)
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def get_book_for_month(catalog: dict[str, Any], *, month: str) -> BookCatalogEntry:
    raw = catalog.get(month)
    if not isinstance(raw, dict):
        return BookCatalogEntry(month=month)

    def _s(key: str) -> Optional[str]:
        v = raw.get(key)
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return str(int(v)) if isinstance(v, int) else str(v)
        if isinstance(v, str):
            st = v.strip()
            return st or None
        return None

    def _sl(key: str) -> Optional[list[str]]:
        v = raw.get(key)
        if v is None:
            return None
        if isinstance(v, str):
            st = v.strip()
            return [st] if st else None
        if isinstance(v, list):
            out: list[str] = []
            for it in v:
                if isinstance(it, str) and it.strip():
                    out.append(it.strip())
            return out or None
        return None

    return BookCatalogEntry(
        month=month,
        title=_s("title"),
        authors=_s("authors"),
        isbn=_s("isbn"),
        page_count=_s("page_count"),
        published=_s("published"),
        publisher=_s("publisher"),
        info_link=_s("info_link"),
        trailer_link=_s("trailer_link"),
        trailer_links=_sl("trailer_links"),
        description=_s("description"),
        summary=_s("summary"),
        toc=_s("toc"),
        meeting_at=_s("meeting_at"),
    )

