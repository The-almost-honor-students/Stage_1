import time
import random
from typing import List
from pymongo import MongoClient
from domain.book import Book
from infrastructure.MetadataMongoDBRepository import MetadataMongoDBRepository
import matplotlib.pyplot as plt
from pathlib import Path

MONGO_URI = "mongodb://localhost:27017"
db_name = "books"
collection = "metadata_test"

def gen_books(start_id: int, n: int) -> List[Book]:
    return [
        Book(
            book_id=start_id + i,
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

def summarize(elapsed_s: float, n_ops: int):
    total_ms = elapsed_s * 1000.0
    ops_sec = n_ops / elapsed_s if elapsed_s > 0 else float("inf")
    avg_ms = total_ms / n_ops
    return total_ms, ops_sec, avg_ms

def bench_insert_metadata(n_docs: int):
    mongo_client = MongoClient(MONGO_URI)
    ensure_collection(mongo_client, db_name, collection)
    metadata_repository = MetadataMongoDBRepository(mongo_client, db_name, collection)
    mocked_books = gen_books(1, n_docs)
    t0 = time.perf_counter()
    for book in mocked_books:
        metadata_repository.save_metadata(book)
    t1 = time.perf_counter()
    duration = t1 - t0
    total_ms, ops_sec, avg_ms = summarize(duration, n_docs)
    return mocked_books, total_ms, ops_sec, avg_ms

def bench_get_metadata(mocked_books: List[Book]):
    n_queries = len(mocked_books)
    mongo_client = MongoClient(MONGO_URI)
    metadata_repository = MetadataMongoDBRepository(mongo_client, db_name, collection)
    book_ids = [b.book_id for b in mocked_books]
    t0 = time.perf_counter()
    for _ in range(n_queries):
        random_id = random.choice(book_ids)
        metadata_repository.collection.find_one({"book_id": random_id})
    t1 = time.perf_counter()
    duration = t1 - t0
    total_ms, ops_sec, avg_ms = summarize(duration, n_queries)
    return total_ms, ops_sec, avg_ms

if __name__ == "__main__":
    plots_dir = Path("mongo_plots")
    plots_dir.mkdir(parents=True, exist_ok=True)

    dataset_sizes = [50, 500, 1000, 5000, 8000, 10000, 15000, 20000, 30000, 40000, 50000, 70000]

    insert_times, get_times = [], []
    insert_ops, get_ops = [], []
    insert_avg, get_avg = [], []

    print("=" * 102)
    print(f"{'N_BOOKS':>10} | {'INS TOTAL (ms)':>15} | {'INS OPS/s':>12} | {'INS AVG (ms)':>12} | "
          f"{'GET TOTAL (ms)':>15} | {'GET OPS/s':>12} | {'GET AVG (ms)':>12}")
    print("=" * 102)

    for n_docs in dataset_sizes:
        books, ins_total, ins_ops, ins_avg = bench_insert_metadata(n_docs)
        get_total, get_ops_val, get_avg_val = bench_get_metadata(books)

        insert_times.append(ins_total)
        get_times.append(get_total)
        insert_ops.append(ins_ops)
        get_ops.append(get_ops_val)
        insert_avg.append(ins_avg)
        get_avg.append(get_avg_val)

        print(f"{n_docs:>10} | {ins_total:>15.2f} | {ins_ops:>12.0f} | {ins_avg:>12.3f} | "
              f"{get_total:>15.2f} | {get_ops_val:>12.0f} | {get_avg_val:>12.3f}")

    print("=" * 102)

    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, insert_times, marker="o", label="Insert Total Time")
    plt.plot(dataset_sizes, get_times, marker="o", label="GET Total Time")
    for x, y in zip(dataset_sizes, insert_times):
        plt.text(x, y, f"{y:.0f} ms", ha="center", va="bottom", fontsize=8, rotation=0)
    for x, y in zip(dataset_sizes, get_times):
        plt.text(x, y, f"{y:.0f} ms", ha="center", va="bottom", fontsize=8, rotation=0)
    plt.xlabel("Number of Books")
    plt.ylabel("Total Time (ms)")
    plt.title("Total Execution Time by Dataset Size")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(plots_dir / "line_total_time_by_dataset.png", dpi=140)
    plt.close()

    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, insert_ops, marker="o", label="Insert Ops/s")
    plt.plot(dataset_sizes, get_ops, marker="o", label="GET Ops/s")
    for x, y in zip(dataset_sizes, insert_ops):
        plt.text(x, y, f"{y:,.0f}", ha="center", va="bottom", fontsize=8)
    for x, y in zip(dataset_sizes, get_ops):
        plt.text(x, y, f"{y:,.0f}", ha="center", va="bottom", fontsize=8)
    plt.xlabel("Number of Books")
    plt.ylabel("Operations per Second")
    plt.title("Throughput (Ops/s) by Dataset Size")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(plots_dir / "line_ops_per_sec_by_dataset.png", dpi=140)
    plt.close()

    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, insert_avg, marker="o", label="Insert Avg Latency")
    plt.plot(dataset_sizes, get_avg, marker="o", label="GET Avg Latency")
    for x, y in zip(dataset_sizes, insert_avg):
        plt.text(x, y, f"{y:.3f} ms", ha="center", va="bottom", fontsize=8)
    for x, y in zip(dataset_sizes, get_avg):
        plt.text(x, y, f"{y:.3f} ms", ha="center", va="bottom", fontsize=8)
    plt.xlabel("Number of Books")
    plt.ylabel("Average Time (ms per operation)")
    plt.title("Average Latency by Dataset Size")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(plots_dir / "line_avg_latency_by_dataset.png", dpi=140)
    plt.close()

    print(f"Line graphs saved in: {plots_dir.resolve()}")
