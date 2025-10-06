import time
from typing import List, Dict, Any
from pymongo import MongoClient, ASCENDING
from domain.book import Book
from infrastructure.MetadataMongoDBRepository import MetadataMongoDBRepository

MONGO_URI = "mongodb://localhost:27017"
db_name = "books"
collection = "metadata_test"

def gen_books(start_id: int, n: int) -> List[Book]:
    return [
        Book(
            book_id= start_id + i,
            title=f"Sample Book {start_id + i}",
            author=f"Author X{start_id + i}",
            language="English",
        )
        for i in range(n)
    ]
def ensure_collection(client: MongoClient, db_name: str, coll_name: str):

    col = client[db_name][coll_name]
    col.delete_many({})
    return col



def summarize(elapsed_s: float, n_docs: int):
    total_ms = elapsed_s * 1000.0
    ops_sec = n_docs / elapsed_s if elapsed_s > 0 else float('inf')
    avg_ms = total_ms / n_docs
    print(f"Total: {total_ms:.2f} ms| Ops/s: {ops_sec:,.0f} | Media: {avg_ms:.3f} ms/doc")


def bench_insert_metadata():
    documents = 10000
    batch_size = 1
    print(f"[INFO] Conectando a MongoDB: {MONGO_URI}")
    print(f"[INFO] DB: {db_name} | Collection: {collection}")
    print(f"[INFO] Documentos: {documents} | Batch size: {batch_size}")

    mongo_client = MongoClient(MONGO_URI)
    ensure_collection(mongo_client, db_name, collection)
    metadata_repository = MetadataMongoDBRepository(mongo_client, db_name, collection)
    mocked_books = gen_books(1, documents)
    t0 = time.perf_counter()
    for book in mocked_books:
        metadata_repository.save_metadata(book)
    t1 = time.perf_counter()
    duration = t1 - t0
    summarize(duration, documents)


if __name__ == "__main__":
    bench_insert_metadata()