import os
import time
import psutil
import json
from pathlib import Path

from infrastructure.implementation_monolithic import build_datalake_index


def benchmark_index_build(build_func, datalake_path, output_file="inverted_index.json"):
    """Benchmark the efficiency of the inverted index build process."""
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / (1024 * 1024)

    start = time.perf_counter()
    index = build_func(datalake_path, output_file)
    end = time.perf_counter()

    mem_after = process.memory_info().rss / (1024 * 1024)

    build_time = end - start

    # Handle potential division by zero if datalake is empty
    datalake_size_bytes = sum(f.stat().st_size for f in Path(datalake_path).glob("*.txt"))
    datalake_size = datalake_size_bytes / (1024 * 1024) if datalake_size_bytes > 0 else 1e-6

    # Safely check if file exists before stat
    index_size = Path(output_file).stat().st_size / (1024 * 1024) if Path(output_file).exists() else 0

    total_words = len(index) if isinstance(index, dict) else 0
    words_per_sec = total_words / build_time if build_time > 0 else 0

    print("Indexing Efficiency Benchmark")
    print("-----------------------------------")
    print(f"Total words indexed: {total_words}")
    print(f"Build time:           {build_time:.2f} seconds")
    print(f"Memory used:          {mem_after - mem_before:.2f} MB")
    print(f"Index file size:      {index_size:.2f} MB")
    print(f"Text data size:       {datalake_size:.2f} MB")
    print(f"Words per second:     {words_per_sec:.2f}")
    print(f"Index/Text ratio:     {(index_size / datalake_size):.2f}")

    return {
        "build_time": build_time,
        "memory_used": mem_after - mem_before,
        "index_size": index_size,
        "total_words": total_words,
        "words_per_sec": words_per_sec,
    }


def load_index(index_file):
    """Load the JSON inverted index file."""
    if not Path(index_file).exists():
        raise FileNotFoundError(f"Index file not found: {index_file}")

    with open(index_file, "r", encoding="utf-8") as f:
        return json.load(f)


def query_index(index, words):
    """Perform a query (intersection of all given words)."""
    if not words:
        return set()

    results = [set(index.get(word, [])) for word in words]
    return set.intersection(*results) if results else set()


def benchmark_query_speed(index_file, queries):
    """Benchmark query latency for multiple queries."""
    index = load_index(index_file)
    query_output = {}

    print("\n Query Speed Benchmark")
    print("-----------------------------------")

    for q in queries:
        words = q.lower().split()
        start = time.perf_counter()
        results = query_index(index, words)
        elapsed = (time.perf_counter() - start) * 1000  # ms
        print(f"Query: '{q}' -> {len(results)} results in {elapsed:.3f} ms")

        query_output[q] = {
            "num_results": len(results),
            "latency_ms": round(elapsed, 3)
        }

    return query_output

def save_benchmark_results(index_results, query_results, output_file="benchmark_results.json"):
    full_results = {
        "indexing": index_results,
        "queries": query_results,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(full_results, f, ensure_ascii=False, indent=2)

    print(f"\n Results saved to {output_file}")


if __name__ == "__main__":
    DATA_PATH = r"C:\Users\salsa\PycharmProjects\Stage_1\datalake"
    OUTPUT_FILE = "inverted_index.json"
    RESULTS_FILE = "benchmark_results.json"

    index_results = benchmark_index_build(build_datalake_index, DATA_PATH, OUTPUT_FILE)

    queries = ["love", "war peace", "science fiction", "philosophy", "revolution"]
    query_results = benchmark_query_speed(OUTPUT_FILE, queries)

    save_benchmark_results(index_results, query_results, RESULTS_FILE)