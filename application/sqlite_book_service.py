from pathlib import Path
import sys

sys.path.append("../")
import sqlite3
import requests
from datetime import datetime
from contextlib import contextmanager


class SQLiteBookService:
    """SQLite-based book storage for benchmarking"""

    def __init__(self, db_path: str = "../benchmark/books.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Initialize database schema"""
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS books (
                    book_id INTEGER PRIMARY KEY,
                    title TEXT,
                    author TEXT,
                    header TEXT,
                    body TEXT,
                    footer TEXT,
                    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    indexed_at TIMESTAMP
                )
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_downloaded 
                ON books(downloaded_at) 
                WHERE downloaded_at IS NOT NULL
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_indexed 
                ON books(indexed_at) 
                WHERE indexed_at IS NOT NULL
            """
            )

            conn.commit()

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def download_book(self, book_id: int) -> bool:
        """Download and store book in SQLite"""
        START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
        END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK"

        url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            text = response.text

            if START_MARKER not in text or END_MARKER not in text:
                return False

            header, body_and_footer = text.split(START_MARKER, 1)
            body, footer = body_and_footer.split(END_MARKER, 1)

            with self._get_connection() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO books 
                    (book_id, header, body, footer, downloaded_at)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        book_id,
                        header.strip(),
                        body.strip(),
                        footer.strip(),
                        datetime.now(),
                    ),
                )
                conn.commit()

            return True
        except Exception as e:
            print(f"Error downloading book {book_id}: {e}")
            return False

    def get_downloaded_ids(self) -> set[int]:
        """Get all downloaded book IDs"""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT book_id FROM books 
                WHERE downloaded_at IS NOT NULL
            """
            )
            return {row[0] for row in cursor.fetchall()}

    def get_indexed_ids(self) -> set[int]:
        """Get all indexed book IDs"""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT book_id FROM books 
                WHERE indexed_at IS NOT NULL
            """
            )
            return {row[0] for row in cursor.fetchall()}

    def mark_indexed(self, book_id: int) -> bool:
        """Mark a book as indexed"""
        try:
            with self._get_connection() as conn:
                conn.execute(
                    """
                    UPDATE books 
                    SET indexed_at = ? 
                    WHERE book_id = ?
                """,
                    (datetime.now(), book_id),
                )
                conn.commit()
            return True
        except Exception as e:
            print(f"Error marking book {book_id} as indexed: {e}")
            return False

    def is_downloaded(self, book_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM books WHERE book_id = ? AND downloaded_at IS NOT NULL",
                (book_id,),
            )
            return cursor.fetchone() is not None
