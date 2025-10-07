from abc import ABC, abstractmethod
from domain.book import Book
from typing import List, Dict


class InvertedIndexRepository(ABC):

    @abstractmethod
    def index_book(self, book: Book) -> bool:
        pass

    @abstractmethod
    def search(self, term: str) -> List[int]:
        pass

    @abstractmethod
    def get_index_stats(self) -> Dict[str, int]:
        pass