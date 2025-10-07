from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

@dataclass
class Book:
    book_id: Optional[int]
    title: Optional[str]
    author: Optional[str]
    language: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
