import requests
from datetime import datetime
from pathlib import Path
import shutil
from application.MetadataRepository import MetadataRepository
from utils.GutenbergHeaderSerializer import GutenbergHeaderSerializer


def create_datalake(book_id: int, download_path: str):
    date = datetime.now().strftime("%Y%m%d")
    hour = datetime.now().strftime("%H")

    datalake_dir = Path(f"../datalake/{date}/{hour}")
    datalake_dir.mkdir(parents=True, exist_ok=True)

    downloads_dir = Path(download_path)
    body_src = downloads_dir / f"{book_id}_body.txt"
    header_src = downloads_dir / f"{book_id}_header.txt"

    if not body_src.exists() or not header_src.exists():
        print(f"Archivos no encontrados en {downloads_dir}")
        return False

    body_dst = datalake_dir / f"{book_id}.body.txt"
    header_dst = datalake_dir / f"{book_id}.header.txt"

    shutil.move(str(body_src), str(body_dst))
    shutil.move(str(header_src), str(header_dst))

    print(f"Archivos movidos a {datalake_dir.resolve()}")
    return True


def download_book(book_id: int, output_path: str):
    START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
    END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK"

    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
    response = requests.get(url)
    response.raise_for_status()
    text = response.text
    if START_MARKER not in text or END_MARKER not in text:
        return False
    header, body_and_footer = text.split(START_MARKER, 1)
    body, footer = body_and_footer.split(END_MARKER, 1)

    body_path = output_path / f"{book_id}_body.txt"
    header_path = output_path / f"{book_id}_header.txt"
    with open(body_path, "w", encoding="utf-8") as f:
        f.write(body.strip())
    with open(header_path, "w", encoding="utf-8") as f:
        f.write(header.strip())
    return True


class BookService:
    def __init__(self,metadata_repository:MetadataRepository):
        self.metadata_repository = metadata_repository

    def create_metadata(self,book_id: int, download_path: str):
        header = GutenbergHeaderSerializer.from_file(download_path)
        self.metadata_repository.save_metadata(header)
