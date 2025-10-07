from typing import Union
from application.bookService import download_book, create_datalake
from pathlib import Path
import random
import time
from apscheduler.schedulers.background import BackgroundScheduler

from infrastructure.InvertedIndexMongoDBRepository import InvertedIndexMongoDBRepository

CONTROL_PATH = Path("../control")
DOWNLOADS = CONTROL_PATH / "downloaded_books.txt"
INDEXINGS = CONTROL_PATH / "indexed_books.txt"
STAGING_DIR = Path("../staging/downloads")
TOTAL_BOOKS = 70000
MAX_RETRIES_NEW_BOOK = 10
SLEEP_SECONDS_BETWEEN_RUNS = 0

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

def control_pipeline_step() -> None:
    CONTROL_PATH.mkdir(parents=True, exist_ok=True)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = _read_ids(DOWNLOADS)
    indexed = _read_ids(INDEXINGS)
    ready_to_index = downloaded - indexed
    if ready_to_index:
        book_id_str = ready_to_index.pop()
        book_id = _safe_int(book_id_str)
        print(f"[CONTROL] Scheduling book {book_id} for indexing...")
        try:
            inverted_index = InvertedIndexMongoDBRepository()
            inverted_index.index_book(book_id)
            _append_id(INDEXINGS, book_id)
            print(f"[CONTROL] Book {book_id} successfully indexed.")
        except Exception as e:
            print(f"[CONTROL][ERROR] Falló el indexado de {book_id}: {e}")
        return
    for _ in range(MAX_RETRIES_NEW_BOOK):
        candidate_id = random.randint(1, TOTAL_BOOKS)
        if str(candidate_id) in downloaded:
            continue
        print(f"[CONTROL] Downloading new book with ID {candidate_id}...")
        try:
            download_book(candidate_id, str(STAGING_DIR))
            ok = create_datalake(candidate_id,str(STAGING_DIR))
            if ok:
                _append_id(DOWNLOADS, candidate_id)
                print(f"[CONTROL] Book {candidate_id} downloaded and registered.")
                return
            else:
                print(f"[CONTROL][WARN] Libro {candidate_id} no válido.")
        except Exception as e:
            print(f"[CONTROL][ERROR] Descarga {candidate_id} falló: {e}")
    print("[CONTROL] No se encontró un libro nuevo para descargar en este ciclo.")

if __name__ == "__main__":

    scheduler = BackgroundScheduler()
    scheduler.add_job(control_pipeline_step, 'interval', seconds=1)
    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
