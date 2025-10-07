from __future__ import annotations
import time
import random
from typing import List, Tuple
from pathlib import Path
import matplotlib.pyplot as plt
from pymongo import MongoClient
from domain.book import Book
from infrastructure.InvertedIndexMongoDBRepository import InvertedIndexMongoDBRepository

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "bench_inverted"
INDEX_COLLECTION = "inverted_index"
DATALAKE_ROOT = "/Users/giselabelmontecruz/PycharmProjects/Stage_1/datalake"
USE_STEMMING = True
DATASET_SIZES = [20, 40, 60, 80, 100, 120, 150, 200, 250, 300]
PLOTS_DIR = Path("inverted_bench_plots")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

def ensure_nltk_stopwords_ready():
    import nltk
    try:
        from nltk.corpus import stopwords
    except LookupError:
        nltk.download("stopwords", quiet=True)

def ensure_clean_collection(client: MongoClient, db_name: str, coll_name: str):
    client[db_name][coll_name].delete_many({})

def summarize(elapsed_s: float, n_ops: int) -> Tuple[float, float, float]:
    total_ms = elapsed_s * 1000.0
    ops_sec = (n_ops / elapsed_s) if elapsed_s > 0 else float("inf")
    avg_ms = total_ms / n_ops if n_ops > 0 else 0.0
    return total_ms, ops_sec, avg_ms

def list_book_ids_from_datalake(datalake_root: str) -> List[int]:
    root = Path(datalake_root)
    ids = set()
    for p in root.rglob("*.body.txt"):
        try:
            bid = int(p.name.split(".")[0])
            ids.add(bid)
        except ValueError:
            continue
    return sorted(ids)

def sample_terms_from_index(client: MongoClient, db_name: str, coll_name: str, limit: int) -> List[str]:
    col = client[db_name][coll_name]
    docs = list(col.aggregate([
        {"$project": {"term": 1, "df": {"$size": {"$ifNull": ["$postings", []]}}}},
        {"$match": {"df": {"$gt": 0}}},
        {"$sort": {"df": -1}},
        {"$limit": 5000}
    ]))
    if not docs:
        return []
    docs.sort(key=lambda d: d["df"], reverse=True)
    top = [d["term"] for d in docs[:max(1, len(docs)//10)]]
    mid = [d["term"] for d in docs[len(docs)//3: 2*len(docs)//3]]
    rare = [d["term"] for d in docs[-max(1, len(docs)//10):]]
    out = []
    each = max(1, limit // 3)
    for bucket in (top, mid, rare):
        for _ in range(each):
            out.append(random.choice(bucket))
    while len(out) < limit:
        out.append(random.choice(mid if mid else top))
    random.shuffle(out)
    return out

def bench_build_inverted_index(book_ids: List[int]) -> Tuple[float, float, float]:
    client = MongoClient(MONGO_URI)
    ensure_clean_collection(client, DB_NAME, INDEX_COLLECTION)
    repo = InvertedIndexMongoDBRepository(
        uri=MONGO_URI,
        db_name=DB_NAME,
        datalake_root=DATALAKE_ROOT,
        index_collection=INDEX_COLLECTION,
        stopwords_path=None,
        use_stemming=USE_STEMMING,
    )
    t0 = time.perf_counter()
    for bid in book_ids:
        repo.index_book(Book(book_id=bid, title=None, author=None, language=None))
    t1 = time.perf_counter()
    total_ms, ops_sec, avg_ms = summarize(t1 - t0, len(book_ids))
    return total_ms, ops_sec, avg_ms

def bench_query_performance(n_queries: int) -> Tuple[float, float, float]:
    repo = InvertedIndexMongoDBRepository(
        uri=MONGO_URI,
        db_name=DB_NAME,
        datalake_root=DATALAKE_ROOT,
        index_collection=INDEX_COLLECTION,
        stopwords_path=None,
        use_stemming=USE_STEMMING,
    )
    client = MongoClient(MONGO_URI)
    terms = sample_terms_from_index(client, DB_NAME, INDEX_COLLECTION, n_queries)
    if not terms:
        return 0.0, 0.0, 0.0
    for q in terms[:10]:
        repo.search(q)
    t0 = time.perf_counter()
    for q in terms:
        repo.search(q)
    t1 = time.perf_counter()
    total_ms, ops_sec, avg_ms = summarize(t1 - t0, len(terms))
    return total_ms, ops_sec, avg_ms

if __name__ == "__main__":
    ensure_nltk_stopwords_ready()
    all_ids = list_book_ids_from_datalake(DATALAKE_ROOT)
    if not all_ids:
        raise RuntimeError("No <book_id>.body.txt files found in datalake.")
    dataset_sizes = [n for n in DATASET_SIZES if n <= len(all_ids)]
    print("=" * 108)
    print(f"{'N_BOOKS':>10} | {'IDX TOTAL (ms)':>15} | {'IDX OPS/s':>12} | {'IDX AVG (ms)':>12} | "
          f"{'QRY TOTAL (ms)':>15} | {'QRY OPS/s':>12} | {'QRY AVG (ms)':>12}")
    print("=" * 108)
    idx_total_list, idx_ops_list, idx_avg_list = [], [], []
    qry_total_list, qry_ops_list, qry_avg_list = [], [], []
    for n in dataset_sizes:
        subset = all_ids[:n]
        idx_total, idx_ops, idx_avg = bench_build_inverted_index(subset)
        n_queries = max(50, n // 2)
        qry_total, qry_ops, qry_avg = bench_query_performance(n_queries)
        idx_total_list.append(idx_total)
        idx_ops_list.append(idx_ops)
        idx_avg_list.append(idx_avg)
        qry_total_list.append(qry_total)
        qry_ops_list.append(qry_ops)
        qry_avg_list.append(qry_avg)
        print(f"{n:>10} | {idx_total:>15.2f} | {idx_ops:>12.0f} | {idx_avg:>12.3f} | "
              f"{qry_total:>15.2f} | {qry_ops:>12.0f} | {qry_avg:>12.3f}")
    print("=" * 108)

    # Indexing-only plots
    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, idx_total_list, marker="o", label="Index Total Time (ms)")
    plt.xlabel("Number of Books")
    plt.ylabel("Total Time (ms)")
    plt.title("Indexing: Total Time by Dataset Size")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "index_total_time.png", dpi=140)
    plt.close()

    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, idx_ops_list, marker="o", label="Index Throughput (books/s)")
    plt.xlabel("Number of Books")
    plt.ylabel("Throughput (books/s)")
    plt.title("Indexing: Throughput by Dataset Size")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "index_throughput.png", dpi=140)
    plt.close()

    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, idx_avg_list, marker="o", label="Index Avg Latency (ms/book)")
    plt.xlabel("Number of Books")
    plt.ylabel("Avg Latency (ms/book)")
    plt.title("Indexing: Average Latency by Dataset Size")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "index_avg_latency.png", dpi=140)
    plt.close()

    # Query-only plots
    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, qry_total_list, marker="o", label="Query Total Time (ms)")
    plt.xlabel("Number of Books")
    plt.ylabel("Total Time (ms)")
    plt.title("Query: Total Time by Dataset Size")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "query_total_time.png", dpi=140)
    plt.close()

    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, qry_ops_list, marker="o", label="Query Throughput (searches/s)")
    plt.xlabel("Number of Books")
    plt.ylabel("Throughput (searches/s)")
    plt.title("Query: Throughput by Dataset Size")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "query_throughput.png", dpi=140)
    plt.close()

    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, qry_avg_list, marker="o", label="Query Avg Latency (ms/search)")
    plt.xlabel("Number of Books")
    plt.ylabel("Avg Latency (ms/search)")
    plt.title("Query: Average Latency by Dataset Size")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "query_avg_latency.png", dpi=140)
    plt.close()

    print(f"Line graphs saved in: {PLOTS_DIR.resolve()}")
