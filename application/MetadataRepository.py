from abc import ABC, abstractmethod
from domain.book import Book


class MetadataRepository(ABC):
    @abstractmethod
    def save_metadata(self, book: Book) -> bool:
        pass