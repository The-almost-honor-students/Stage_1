import os
import time
import random
from pathlib import Path
import matplotlib.pyplot as plt

from infrastructure.implementation_monolithic import build_datalake_index

DATALAKE_ROOT = r"C:\Users\salsa\PycharmProjects\Stage_1\datalake"
OUTPUT_INDEX = "inverted_index.json"
USE_STEMMING = False  # Optional: implement stemming in clean_text_simple if needed
DATASET_SIZES = [50, 200, 500, 1000]
PLOTS_DIR = Path("inverted_bench_plots")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def list_book_ids_from_datalake(datalake_root: str):
    ids = set()
    for p in Path(datalake_root).rglob("*.body.txt"):
        try:
            bid = int(p.name.split(".")[0])
            ids.add(bid)
        except ValueError:
            continue
    return sorted(ids)


def sample_terms_from_index(index: dict, limit: int):
    if not index:
        return []

    # Sort terms by frequency
    term_freqs = [(term, len(postings)) for term, postings in index.items()]
    term_freqs.sort(key=lambda x: x[1], reverse=True)

    top = [t for t, _ in term_freqs[:max(1, len(term_freqs)//10)]]
    mid = [t for t, _ in term_freqs[len(term_freqs)//3:2*len(term_freqs)//3]]
    rare = [t for t, _ in term_freqs[-max(1, len(term_freqs)//10):]]

    out = []
    each = max(1, limit // 3)
    for bucket in (top, mid, rare):
        for _ in range(each):
            out.append(random.choice(bucket))
    while len(out) < limit:
        out.append(random.choice(mid if mid else top))
    random.shuffle(out)
    return out


def bench_build_index(book_ids, datalake_root):
    start = time.perf_counter()
    index = build_datalake_index(datalake_root, OUTPUT_INDEX)
    end = time.perf_counter()
    elapsed = end - start
    total_ms = elapsed * 1000.0
    ops_sec = len(book_ids) / elapsed if elapsed > 0 else float("inf")
    avg_ms = total_ms / len(book_ids) if book_ids else 0
    return total_ms, ops_sec, avg_ms, index


def query_index(index, words):
    results = [set(index.get(word, [])) for word in words]
    return set.intersection(*results) if results else set()


def bench_query_performance(index, n_queries: int):
    terms = sample_terms_from_index(index, n_queries)
    if not terms:
        return 0.0, 0.0, 0.0
    # Warm-up
    for q in terms[:10]:
        query_index(index, [q])
    t0 = time.perf_counter()
    for q in terms:
        query_index(index, [q])
    t1 = time.perf_counter()
    total_ms = (t1 - t0) * 1000.0
    ops_sec = len(terms) / (t1 - t0) if (t1 - t0) > 0 else float("inf")
    avg_ms = total_ms / len(terms) if terms else 0
    return total_ms, ops_sec, avg_ms


if __name__ == "__main__":
    all_ids = list_book_ids_from_datalake(DATALAKE_ROOT)
    dataset_sizes = [n for n in DATASET_SIZES if n <= len(all_ids)]

    idx_total_list, idx_ops_list, idx_avg_list = [], [], []
    qry_total_list, qry_ops_list, qry_avg_list = [], [], []

    print("="*90)
    print(f"{'N_BOOKS':>10} | {'IDX TOTAL (ms)':>15} | {'IDX OPS/s':>12} | {'IDX AVG (ms)':>12} | "
          f"{'QRY TOTAL (ms)':>15} | {'QRY OPS/s':>12} | {'QRY AVG (ms)':>12}")
    print("="*90)

    for n in dataset_sizes:
        subset = all_ids[:n]
        idx_total, idx_ops, idx_avg, index = bench_build_index(subset, DATALAKE_ROOT)
        n_queries = max(50, n // 2)
        qry_total, qry_ops, qry_avg = bench_query_performance(index, n_queries)

        idx_total_list.append(idx_total)
        idx_ops_list.append(idx_ops)
        idx_avg_list.append(idx_avg)
        qry_total_list.append(qry_total)
        qry_ops_list.append(qry_ops)
        qry_avg_list.append(qry_avg)

        print(f"{n:>10} | {idx_total:>15.2f} | {idx_ops:>12.0f} | {idx_avg:>12.3f} | "
              f"{qry_total:>15.2f} | {qry_ops:>12.0f} | {qry_avg:>12.3f}")

    print("="*90)

    # Plotting results
    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, idx_total_list, marker="o", label="Index Total Time")
    plt.plot(dataset_sizes, qry_total_list, marker="o", label="Query Total Time")
    plt.xlabel("Number of Books")
    plt.ylabel("Total Time (ms)")
    plt.title("Monolithic Inverted Index: Total Time by Dataset Size")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "inv_total_time_by_dataset.png", dpi=140)
    plt.close()

    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, idx_ops_list, marker="o", label="Index Ops/s (books/s)")
    plt.plot(dataset_sizes, qry_ops_list, marker="o", label="Query Ops/s (searches/s)")
    plt.xlabel("Number of Books")
    plt.ylabel("Operations per Second")
    plt.title("Monolithic Inverted Index: Throughput by Dataset Size")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "inv_ops_per_sec_by_dataset.png", dpi=140)
    plt.close()

    plt.figure(figsize=(9, 5))
    plt.plot(dataset_sizes, idx_avg_list, marker="o", label="Index Avg Latency (per book)")
    plt.plot(dataset_sizes, qry_avg_list, marker="o", label="Query Avg Latency (per term)")
    plt.xlabel("Number of Books")
    plt.ylabel("Average Time (ms per operation)")
    plt.title("Monolithic Inverted Index: Average Latency by Dataset Size")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "inv_avg_latency_by_dataset.png", dpi=140)
    plt.close()

    print(f"Line graphs saved in: {PLOTS_DIR.resolve()}")
