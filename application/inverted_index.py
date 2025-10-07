from abc import ABC, abstractmethod
from typing import Set, List, Dict
import re


class Tokenizer:

    @staticmethod
    def tokenize(text: str) -> List[str]:

        text = text.lower()
        tokens = re.findall(r"\b\w+\b", text)
        # Filtra palabras muy cortas (menos de 3 caracteres)
        tokens = [t for t in tokens if len(t) > 2]
        return tokens


class InvertedIndex(ABC):

    @abstractmethod
    def add_document(self, doc_id: int, tokens: List[str]) -> None:

        pass

    @abstractmethod
    def search(self, term: str) -> Set[int]:

        pass

    @abstractmethod
    def multi_term_search(self, terms: List[str]) -> Set[int]:

        pass

    @abstractmethod
    def get_stats(self) -> Dict:

        pass

    @abstractmethod
    def close(self) -> None:

        pass


class QueryEngine:

    def __init__(self, indexer: InvertedIndex):

        self.indexer = indexer
        self.tokenizer = Tokenizer()

    def search(self, query: str) -> Set[int]:

        tokens = self.tokenizer.tokenize(query)
        if not tokens:
            return set()

        return self.indexer.multi_term_search(tokens)

    def search_single_term(self, term: str) -> Set[int]:

        return self.indexer.search(term.lower())
