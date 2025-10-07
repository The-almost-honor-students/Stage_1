from typing import Union

from application.bookService import download_book, create_datalake
from pathlib import Path
import random
import time

from apscheduler.schedulers.background import BackgroundScheduler
from application.inverted_index import SQLiteIndexer, Tokenizer

BASE_DIR = Path(__file__).resolve().parent.parent

CONTROL_PATH = BASE_DIR / "control"
DOWNLOADS = CONTROL_PATH / "downloaded_books.txt"
INDEXINGS = CONTROL_PATH / "indexed_books.txt"
STAGING_DIR = BASE_DIR / "staging/downloads"
TOTAL_BOOKS = 70000
MAX_RETRIES_NEW_BOOK = 10
SLEEP_SECONDS_BETWEEN_RUNS = 0
indexer = SQLiteIndexer("example_index.db")
tokenizer = Tokenizer()


def _read_ids(path: Path) -> set[str]:
    if path.exists():
        return set(path.read_text(encoding="utf-8").splitlines())
    return set()


def _append_id(path: Path, book_id: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{book_id}\n")


def _safe_int(s: Union[str, int]) -> int:
    return int(s) if not isinstance(s, int) else s


def control_pipeline_step() -> dict[str, str] | None:
    CONTROL_PATH.mkdir(parents=True, exist_ok=True)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    downloaded = _read_ids(DOWNLOADS)
    indexed = _read_ids(INDEXINGS)
    ready_to_index = downloaded - indexed

    if ready_to_index:
        """No estoy convencido de que esto sea correcto.
        En mi opinion esto deberia ser un modulo que se ejecute aparte
        y no dentro del control pipeline step. Sin embargo, lo dejo así para que podais darme feedback.
        Si estais de acuerdo conmigo, lo cambiamos en la siguiente iteracion.
        Para más info, ver comentarios en application/sqlite_indexer.py"""
        book_id_str = ready_to_index.pop()
        book_id = _safe_int(book_id_str)
        print(f"[CONTROL] Scheduling book {book_id} for indexing...")
        try:
            _append_id(INDEXINGS, book_id)
            print(f"[CONTROL] Book {book_id} successfully indexed.")
            return {"action": "indexed", "book_id": str(book_id)}
        except Exception as e:
            print(f"[CONTROL][ERROR] Failed to index {book_id}: {e}")
            return {"action": "error", "book_id": str(book_id), "error": str(e)}

    for _ in range(MAX_RETRIES_NEW_BOOK):
        candidate_id = random.randint(1, TOTAL_BOOKS)
        if str(candidate_id) in downloaded:
            continue
        print(f"[CONTROL] Downloading new book with ID {candidate_id}...")
        try:
            download_book(candidate_id, str(STAGING_DIR))
            ok = create_datalake(candidate_id, str(STAGING_DIR))
            if ok:
                _append_id(DOWNLOADS, candidate_id)
                print(
                    f"[CONTROL] Book {candidate_id} downloaded and registered."
                )
                return {"action": "downloaded", "book_id": str(candidate_id)}
            else:
                print(f"[CONTROL][WARN] Book {candidate_id} is invalid.")
        except Exception as e:
            print(f"[CONTROL][ERROR] Download failed for {candidate_id}: {e}")
            return {
                "action": "error",
                "book_id": str(candidate_id),
                "error": str(e),
            }

    print("[CONTROL] No new book to download in this cycle.")
    return None


if __name__ == "__main__":

    scheduler = BackgroundScheduler()

    scheduler.add_job(
        control_pipeline_step, "interval", seconds=1, max_instances=3
    )

    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
