from __future__ import annotations
from pathlib import Path
import re
from typing import Optional

from domain.book import Book


class GutenbergHeaderSerializer:
    """
    Extrae metadatos básicos (book_id, title, author, language) de un texto
    de cabecera de Project Gutenberg.

    Admite patrones como:
      - 'Release date: ... [eBook #8388]'
      - 'Release Date: ... [EBook #12345]'
      - 'Project Gutenberg eBook of ... [EBook #12345]' (fallback)
    """

    # Campos típicos en líneas separadas
    _RX_TITLE = re.compile(r"^Title:\s*(.+)$", re.IGNORECASE)
    _RX_AUTHOR = re.compile(r"^Author:\s*(.+)$", re.IGNORECASE)
    _RX_LANGUAGE = re.compile(r"^Language:\s*(.+)$", re.IGNORECASE)

    # book_id en corchetes del tipo [eBook #8388] (robusto a mayúsculas/minúsculas y a 'e-book')
    _RX_EBOOK_ID = re.compile(
        r"\[(?:[^]]*?)\b(?:e[-\s]?book|ebook)\s*#\s*(\d+)\b[^]]*?\]",
        re.IGNORECASE,
    )

    # fallback: a veces aparece sin corchetes
    _RX_EBOOK_ID_FALLBACK = re.compile(
        r"\b(?:project\s+gutenberg.*?)?\b(?:e[-\s]?book|ebook)\s*#\s*(\d+)\b",
        re.IGNORECASE | re.DOTALL,
    )

    @staticmethod
    def _extract_first_line_value(text: str, rx: re.Pattern) -> Optional[str]:
        for line in text.splitlines():
            m = rx.match(line.strip())
            if m:
                return m.group(1).strip() or None
        return None

    @classmethod
    def _extract_book_id(cls, text: str) -> Optional[int]:
        m = cls._RX_EBOOK_ID.search(text)
        if not m:
            m = cls._RX_EBOOK_ID_FALLBACK.search(text)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None
        return None

    @classmethod
    def from_text(cls, text: str) -> Book:
        title = cls._extract_first_line_value(text, cls._RX_TITLE)
        author = cls._extract_first_line_value(text, cls._RX_AUTHOR)
        language = cls._extract_first_line_value(text, cls._RX_LANGUAGE)
        book_id = cls._extract_book_id(text)
        return Book(
            book_id=book_id,
            title=title,
            author=author,
            language=language,
        )

    @classmethod
    def from_file(cls, path: str | Path) -> Book:
        p = Path(path)
        text = p.read_text(encoding="utf-8", errors="ignore")
        return cls.from_text(text)