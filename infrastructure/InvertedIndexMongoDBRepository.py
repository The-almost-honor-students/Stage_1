from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import List, Dict, Optional, Iterable, Tuple

from nltk.stem import PorterStemmer
from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.collection import Collection

from application.InvertedIndexRepository import InvertedIndexRepository
from domain.book import Book


class InvertedIndexMongoDBRepository(InvertedIndexRepository):
    def __init__(
        self,
        uri: str,
        db_name: str,
        datalake_root: str,
        index_collection: str = "inverted_index",
        stopwords_path: Optional[str] = "stopwords.txt",
        use_stemming: bool = True,
    ) -> None:
        self.col: Collection = MongoClient(uri)[db_name][index_collection]
        self.datalake_root = Path(datalake_root)
        if not self.datalake_root.exists():
            raise FileNotFoundError(f"No existe el datalake: {self.datalake_root}")

        self.col.create_index([("term", ASCENDING)], unique=True, name="term_unique")

        self.stopwords = self._load_stopwords(stopwords_path) if stopwords_path else set()
        self.stemmer = PorterStemmer() if use_stemming else None

    def index_book(self, book: Book) -> bool:
        if book.book_id is None:
            return False

        text = self._read_book_body_latest(int(book.book_id))
        if not text:
            return True

        doc_terms = set(self._pipeline_tokens(text))
        if not doc_terms:
            return True

        bid = int(book.book_id)
        ops = [
            UpdateOne({"term": term}, {"$addToSet": {"postings": bid}}, upsert=True)
            for term in doc_terms
        ]
        self.col.bulk_write(ops, ordered=False)
        return True

    def get_index_by_term(self, term: str) -> List[int]:
        t = self._pipeline_single_token(term)
        if not t:
            return []
        doc = self.col.find_one({"term": t}, {"postings": 1})
        return [int(x) for x in (doc.get("postings", []) if doc else [])]

    def get_index_stats(self) -> Dict[str, int]:
        terms = self.col.estimated_document_count()
        agg = list(self.col.aggregate([
            {"$project": {"n": {"$size": {"$ifNull": ["$postings", []]}}}},
            {"$group": {"_id": None, "total": {"$sum": "$n"}}},
        ]))
        return {"terms": int(terms), "total_postings": int(agg[0]["total"]) if agg else 0}

    def reset_index(self) -> None:
        self.col.delete_many({})

    def _read_book_body_latest(self, book_id: int) -> str:
        body_path = self._pick_latest(self.datalake_root.rglob(f"{book_id}.body.txt"))
        if body_path and body_path.exists():
            return body_path.read_text(encoding="utf-8", errors="ignore")
        return ""

    def _pick_latest(self, paths_iter) -> Optional[Path]:
        candidates = sorted(paths_iter, key=self._sort_key, reverse=True)
        return candidates[0] if candidates else None

    def _sort_key(self, p: Path) -> Tuple[str, str, str]:
        parts = p.parts
        date  = parts[-3] if len(parts) >= 3 else ""
        shard = parts[-2] if len(parts) >= 2 else ""
        return (date, shard, p.name)

    def _load_stopwords(self, path: Optional[str]) -> set:
        try:
            if path:
                return {line.strip().lower() for line in open(path, "r", encoding="utf-8").read().splitlines() if
                        line.strip()}
            else:
                import nltk
                from nltk.corpus import stopwords
                nltk.download("stopwords", quiet=True)
                return set(stopwords.words("english"))
        except Exception as e:
            print(f"[WARN] No se pudieron cargar las stopwords ({e}), usando conjunto vacío.")
            return set()

    def _normalize(self, s: str) -> str:
        s = s.lower()
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = re.sub(r"[^\w\s]", " ", s)
        s = re.sub(r"\d+", " ", s)
        s = re.sub(r"_", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _tokenize(self, s: str) -> List[str]:
        return [t for t in s.split(" ") if t.isalpha()]

    def _remove_stop(self, tokens: Iterable[str], min_len: int = 3) -> List[str]:
        if not self.stopwords:
            return [t for t in tokens if len(t) >= min_len]
        return [t for t in tokens if len(t) >= min_len and t not in self.stopwords]

    def _stem(self, tokens: Iterable[str]) -> List[str]:
        if not self.stemmer:
            return list(tokens)
        return [self.stemmer.stem(t) for t in tokens]

    def _dedup(self, tokens: Iterable[str]) -> List[str]:
        seen, out = set(), []
        for t in tokens:
            if t not in seen:
                seen.add(t)
                out.append(t)
        return out

    def _pipeline_tokens(self, raw: str) -> List[str]:
        norm = self._normalize(raw)
        toks = self._tokenize(norm)
        toks = self._remove_stop(toks)
        toks = self._stem(toks)
        toks = self._remove_stop(toks)
        return self._dedup(toks)

    def _pipeline_single_token(self, term: str) -> Optional[str]:
        norm = self._normalize(term)
        if not norm:
            return None
        toks = self._tokenize(norm)
        if not toks:
            return None
        t = toks[0]
        if t in self.stopwords or len(t) < 3:
            return None
        t = self.stemmer.stem(t) if self.stemmer else t
        if t in self.stopwords or len(t) < 3:
            return None
        return t
