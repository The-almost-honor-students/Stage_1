import sqlite3
from pathlib import Path
from typing import Set, List, Dict

from application.inverted_index import InvertedIndex, Tokenizer, QueryEngine


class SQLiteIndexer(InvertedIndex):

    def __init__(self, db_path: str = "inverted_index.db"):

        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()
        self.doc_count = 0

    def _create_tables(self) -> None:
        """Crea las tablas necesarias para el índice invertido"""
        cursor = self.conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS terms (
                term_id INTEGER PRIMARY KEY AUTOINCREMENT,
                term TEXT UNIQUE NOT NULL
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS postings (
                term_id INTEGER,
                doc_id INTEGER,
                PRIMARY KEY (term_id, doc_id)
            )
        """
        )

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_term ON terms(term)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_postings_term ON postings(term_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_postings_doc ON postings(doc_id)"
        )

        self.conn.commit()

    def add_document(self, doc_id: int, tokens: List[str]) -> None:

        cursor = self.conn.cursor()
        unique_tokens = set(tokens)

        for token in unique_tokens:
            cursor.execute(
                "INSERT OR IGNORE INTO terms (term) VALUES (?)", (token,)
            )
            cursor.execute("SELECT term_id FROM terms WHERE term = ?", (token,))
            term_id = cursor.fetchone()[0]

            cursor.execute(
                "INSERT OR IGNORE INTO postings (term_id, doc_id) VALUES (?, ?)",
                (term_id, doc_id),
            )

        self.conn.commit()
        self.doc_count += 1

    def search(self, term: str) -> Set[int]:

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT p.doc_id 
            FROM postings p
            JOIN terms t ON p.term_id = t.term_id
            WHERE t.term = ?
        """,
            (term.lower(),),
        )
        return set(row[0] for row in cursor.fetchall())

    def multi_term_search(self, terms: List[str]) -> Set[int]:

        if not terms:
            return set()

        result = self.search(terms[0])
        for term in terms[1:]:
            result = result.intersection(self.search(term))
        return result

    def get_disk_size(self) -> int:

        return (
            Path(self.db_path).stat().st_size
            if Path(self.db_path).exists()
            else 0
        )

    def get_stats(self) -> Dict:

        cursor = self.conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM terms")
        unique_terms = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM postings")
        total_postings = cursor.fetchone()[0]

        return {
            "doc_count": self.doc_count,
            "unique_terms": unique_terms,
            "total_postings": total_postings,
            "disk_mb": self.get_disk_size() / (1024 * 1024),
            "storage_type": "disk",
        }

    def close(self) -> None:
        self.conn.close()


if __name__ == "__main__":
    """Esto es lo que creo que deberia ser el otro modulo en cuestion."""

    from application.inverted_index import Tokenizer, QueryEngine

    print("=" * 70)
    print(" SQLite Indexer - Example Usage")
    print("=" * 70)

    indexer = SQLiteIndexer("example_index.db")
    tokenizer = Tokenizer()

    tokenizer = Tokenizer()
    datalake_path = Path(Path(__file__).resolve().parent.parent / "datalake")
    documents = {}

    for i, file_path in enumerate(datalake_path.rglob("*.txt"), start=1):
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        tokens = tokenizer.tokenize(content)
        documents[i] = content
        if i >= 10:
            break

    print("\n Indexing documents...")
    for doc_id, content in documents.items():
        tokens = tokenizer.tokenize(content)
        indexer.add_document(doc_id, tokens)
        print(f"   Document {doc_id} indexed with {len(tokens)} unique tokens")

    stats = indexer.get_stats()
    print(f"\n Index Statistics:")
    print(f"   Documents:      {stats['doc_count']}")
    print(f"   Unique terms:   {stats['unique_terms']}")
    print(f"   Total postings: {stats['total_postings']}")
    print(f"   Disk size:      {stats['disk_mb']:.3f} MB")

    qe = QueryEngine(indexer)

    print(f"\n Search Examples:")

    queries = ["Moby", "Independence", "Holmes", "Quijote"]

    for query in queries:
        results = qe.search(query)
        print(f"   '{query}' -> {len(results)} results: {sorted(results)}")

    indexer.close()
    print(f"\n Successfully closed the indexer. Operation complete.")
