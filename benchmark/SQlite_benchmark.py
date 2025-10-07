import time
import statistics
import json
import random
from pathlib import Path
from typing import List, Tuple

from application.inverted_index import InvertedIndex, Tokenizer
from application.sqlite_indexer import SQLiteIndexer


class IndexerBenchmark:

    def __init__(
        self,
        indexer: InvertedIndex,
        datalake_path: str = f"{Path(__file__).resolve().parent.parent}/datalake",
    ):

        self.indexer = indexer
        self.datalake_path = Path(datalake_path)
        self.tokenizer = Tokenizer()
        self.results = {}

    def load_documents(
        self, max_docs: int = None
    ) -> List[Tuple[int, List[str]]]:

        documents = []

        if not self.datalake_path.exists():
            print(f"[WARN] Datalake no encontrado en {self.datalake_path}")
            return documents

        files = list(self.datalake_path.rglob("*.txt"))
        if max_docs:
            files = files[:max_docs]

        print(f"Loading {len(files)} documents...")
        for i, file_path in enumerate(files, 1):
            try:
                doc_id = int(file_path.stem.split(".")[0])
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                tokens = self.tokenizer.tokenize(content)
                documents.append((doc_id, tokens))

                if i % 100 == 0:
                    print(f"  Loaded {i}/{len(files)} documents...")
            except Exception as e:
                print(f"[ERROR] No se pudo cargar {file_path}: {e}")

        return documents

    def benchmark_indexing(self, documents: List[Tuple[int, List[str]]]):

        print("\n" + "=" * 90)
        print(" BENCHMARK: INDEXING SPEED")
        print("=" * 90)

        print(f"\nIndexing {len(documents)} documents...")
        start = time.perf_counter()

        for i, (doc_id, tokens) in enumerate(documents, 1):
            self.indexer.add_document(doc_id, tokens)

            if i % 100 == 0:
                elapsed = time.perf_counter() - start
                rate = i / elapsed if elapsed > 0 else 0
                print(
                    f"  {i}/{len(documents)} docs indexed ({rate:.1f} docs/s)"
                )

        total_time = time.perf_counter() - start

        stats = self.indexer.get_stats()

        self.results["indexing"] = {
            "num_documents": len(documents),
            "total_time_s": total_time,
            "docs_per_sec": (
                len(documents) / total_time if total_time > 0 else 0
            ),
            "stats": stats,
        }

        print(f"\n{'Metric':<25} | {'Value':<20}")
        print("-" * 50)
        print(f"{'Total time':<25} | {total_time:<20.3f} s")
        print(
            f"{'Documents/second':<25} | {self.results['indexing']['docs_per_sec']:<20.1f}"
        )
        print(f"{'Unique terms':<25} | {stats['unique_terms']:<20}")
        print(f"{'Total postings':<25} | {stats['total_postings']:<20}")

        if "disk_mb" in stats:
            print(f"{'Disk size':<25} | {stats['disk_mb']:<20.3f} MB")
        if "memory_mb" in stats:
            print(f"{'Memory size':<25} | {stats['memory_mb']:<20.3f} MB")

    def benchmark_search(self, num_queries: int = 100):

        print("\n" + "=" * 90)
        print(f" BENCHMARK: SEARCH SPEED ({num_queries} queries)")
        print("=" * 90)

        stats = self.indexer.get_stats()
        print(f"\nGenerating {num_queries} random search terms...")

        search_terms = []
        sample_terms = [
            "the",
            "and",
            "book",
            "test",
            "data",
            "python",
            "algorithm",
            "structure",
            "search",
            "index",
            "document",
            "example",
        ]

        for _ in range(num_queries):
            search_terms.append(random.choice(sample_terms))

        print("Executing searches...")
        search_times = []

        for i, term in enumerate(search_terms, 1):
            start = time.perf_counter()
            results = self.indexer.search(term)
            search_times.append(time.perf_counter() - start)

            if i % 20 == 0:
                print(f"  {i}/{num_queries} queries completed...")

        self.results["search"] = {
            "num_queries": num_queries,
            "mean_ms": statistics.mean(search_times) * 1000,
            "median_ms": statistics.median(search_times) * 1000,
            "min_ms": min(search_times) * 1000,
            "max_ms": max(search_times) * 1000,
            "total_s": sum(search_times),
        }

        print(f"\n{'Metric':<25} | {'Value':<20}")
        print("-" * 50)
        print(
            f"{'Mean time':<25} | {self.results['search']['mean_ms']:<20.4f} ms"
        )
        print(
            f"{'Median time':<25} | {self.results['search']['median_ms']:<20.4f} ms"
        )
        print(
            f"{'Min time':<25} | {self.results['search']['min_ms']:<20.4f} ms"
        )
        print(
            f"{'Max time':<25} | {self.results['search']['max_ms']:<20.4f} ms"
        )
        print(
            f"{'Total time':<25} | {self.results['search']['total_s']:<20.4f} s"
        )

    def benchmark_scalability(self, test_sizes: List[int] = None):

        if test_sizes is None:
            test_sizes = [10, 50, 100, 500, 1000, 5000]

        print("\n" + "=" * 90)
        print(" BENCHMARK: SCALABILITY")
        print("=" * 90)

        sample_terms = [
            "the",
            "and",
            "book",
            "test",
            "data",
            "python",
            "algorithm",
            "structure",
            "search",
            "index",
            "document",
            "example",
        ]

        scalability = []

        print(
            f"\n{'QUERIES':<12} | {'TOTAL TIME (ms)':<18} | {'AVG TIME (ms)':<18} | {'QUERIES/SEC':<15}"
        )
        print("-" * 75)

        for size in test_sizes:
            search_terms = [random.choice(sample_terms) for _ in range(size)]

            start = time.perf_counter()
            for term in search_terms:
                _ = self.indexer.search(term)
            total_time = (time.perf_counter() - start) * 1000

            avg_time = total_time / size
            queries_per_sec = (
                (size / (total_time / 1000)) if total_time > 0 else 0
            )

            print(
                f"{size:<12} | {total_time:<18.3f} | {avg_time:<18.4f} | {queries_per_sec:<15.1f}"
            )

            scalability.append(
                {
                    "queries": size,
                    "total_ms": total_time,
                    "avg_ms": avg_time,
                    "queries_per_sec": queries_per_sec,
                }
            )

        self.results["scalability"] = scalability

    def print_summary(self):
        """Imprime resumen final del benchmark"""
        print("\n" + "=" * 90)
        print(" BENCHMARK SUMMARY")
        print("=" * 90)

        if "indexing" in self.results:
            idx = self.results["indexing"]
            print(f"\n INDEXING:")
            print(f"   Documents indexed:  {idx['num_documents']}")
            print(f"   Total time:         {idx['total_time_s']:.2f}s")
            print(
                f"   Speed:              {idx['docs_per_sec']:.1f} docs/second"
            )
            print(f"   Unique terms:       {idx['stats']['unique_terms']}")
            print(f"   Total postings:     {idx['stats']['total_postings']}")

            if "disk_mb" in idx["stats"]:
                print(
                    f"   Storage (disk):     {idx['stats']['disk_mb']:.3f} MB"
                )
            if "memory_mb" in idx["stats"]:
                print(
                    f"   Storage (memory):   {idx['stats']['memory_mb']:.3f} MB"
                )

        if "search" in self.results:
            srch = self.results["search"]
            print(f"\n SEARCH ({srch['num_queries']} queries):")
            print(f"   Mean time:          {srch['mean_ms']:.4f}ms")
            print(f"   Median time:        {srch['median_ms']:.4f}ms")
            print(f"   Min time:           {srch['min_ms']:.4f}ms")
            print(f"   Max time:           {srch['max_ms']:.4f}ms")

        if "scalability" in self.results:
            print(f"\n SCALABILITY:")
            last = self.results["scalability"][-1]
            print(f"   At {last['queries']} queries:")
            print(f"   - Total time:       {last['total_ms']:.2f}ms")
            print(f"   - Avg per query:    {last['avg_ms']:.4f}ms")
            print(
                f"   - Throughput:       {last['queries_per_sec']:.1f} queries/sec"
            )

    def save_results(self, filename: str = "benchmark_results.json"):
        """
        Guarda resultados en JSON

        Args:
            filename: Nombre del archivo de salida
        """
        with open(filename, "w") as f:
            json.dump(self.results, f, indent=2)
        print(f"\n Results saved to {filename}")


if __name__ == "__main__":

    print("=" * 90)
    print(" INVERTED INDEX BENCHMARK - SQLite Implementation")
    print("=" * 90)

    indexer = SQLiteIndexer("benchmark_index.db")

    benchmark = IndexerBenchmark(
        indexer,
        datalake_path=f"{Path(__file__).resolve().parent.parent}/datalake",
    )

    print("\n Loading documents...")
    documents = benchmark.load_documents(max_docs=1000)

    if not documents:
        print("[ERROR] No documents found. Check your datalake path.")
        print(f"Looking in: {benchmark.datalake_path}")
        exit(1)

    print(f"✓ Loaded {len(documents)} documents")

    benchmark.benchmark_indexing(documents)
    benchmark.benchmark_search(num_queries=100)
    benchmark.benchmark_scalability(test_sizes=[10, 50, 100, 500, 1000, 5000])

    benchmark.print_summary()
    benchmark.save_results("sqlite_benchmark_results.json")

    indexer.close()

    print("\n" + "=" * 90)
    print("  BENCHMARK COMPLETE")
    print("=" * 90)
