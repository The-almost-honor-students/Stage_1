import time
import random
import statistics
from pathlib import Path
import json

from application.sqlite_book_service import SQLiteBookService


class Benchmark:
    def __init__(self):
        self.results = {}
        self.sqlite_service = SQLiteBookService("../benchmark/books.db")

    def benchmark_speed(self, num_runs=100, max_id: int = 100000):
        """Benchmark: Velocidad de consulta (buscar si un libro existe)"""

        downloads_path = (
            Path(__file__).parent.parent / "control" / "downloaded_books.txt"
        )
        downloaded = (
            set(downloads_path.read_text().splitlines())
            if downloads_path.exists()
            else set()
        )
        downloaded: set[str] = set(downloaded)
        num_books = len(downloaded)

        print(
            f"\n=== VELOCIDAD: {num_runs} consultas con {num_books} libros almacenados ==="
        )

        file_times = []
        sqlite_times = []

        for _ in range(num_runs):
            book_id = random.randint(1, max_id)

            start = time.perf_counter()
            exists = str(book_id) in downloaded
            file_times.append(time.perf_counter() - start)

            start = time.perf_counter()
            exists = self.sqlite_service.is_downloaded(book_id)
            sqlite_times.append(time.perf_counter() - start)

        speedup = statistics.mean(file_times) / statistics.mean(sqlite_times)

        self.results["speed"] = {
            "num_books": num_books,
            "num_queries": num_runs,
            "file_system": {
                "mean_ms": statistics.mean(file_times) * 1000,
                "median_ms": statistics.median(file_times) * 1000,
                "total_s": sum(file_times),
            },
            "sqlite": {
                "mean_ms": statistics.mean(sqlite_times) * 1000,
                "median_ms": statistics.median(sqlite_times) * 1000,
                "total_s": sum(sqlite_times),
            },
            "speedup": speedup,
        }

        print(
            f"File System: {self.results['speed']['file_system']['mean_ms']:.3f}ms por consulta"
        )
        print(
            f"SQLite:      {self.results['speed']['sqlite']['mean_ms']:.3f}ms por consulta"
        )
        print(f"SQLite es {speedup:.1f}x más rápido")

    def benchmark_size(self):
        """Benchmark: Espacio en disco"""
        file_size = 0
        num_books_files = 0

        datalake = Path("../datalake")
        if datalake.exists():
            files = [f for f in datalake.rglob("*") if f.is_file()]
            file_size = sum(f.stat().st_size for f in files)
            num_books_files = len(files)

        control = Path("../control")
        if control.exists():
            file_size += sum(
                f.stat().st_size for f in control.rglob("*") if f.is_file()
            )

        db_path = Path("../benchmark/books.db")
        sqlite_size = db_path.stat().st_size if db_path.exists() else 0

        num_books_sqlite = len(self.sqlite_service.get_downloaded_ids())

        self.results["size"] = {
            "num_books_file_system": num_books_files,
            "num_books_sqlite": num_books_sqlite,
            "file_system_mb": file_size / (1024 * 1024),
            "sqlite_mb": sqlite_size / (1024 * 1024),
            "reduction_percent": (
                ((file_size - sqlite_size) / file_size * 100)
                if file_size > 0
                else 0
            ),
        }

        print(f"\n=== TAMAÑO EN DISCO ===")
        print(
            f"File System: {self.results['size']['file_system_mb']:.2f} MB ({num_books_files} libros)"
        )
        print(
            f"SQLite:      {self.results['size']['sqlite_mb']:.2f} MB ({num_books_sqlite} libros)"
        )
        print(f"Reducción:   {self.results['size']['reduction_percent']:.1f}%")

    def benchmark_scalability(self, test_sizes=None, max_id: int = 100000):
        """Benchmark: Escalabilidad (cómo crece el tiempo con más datos)"""
        if test_sizes is None:
            test_sizes = [50, 100, 500, 1000, 5000, 10000, 50000]

        downloads_path = (
            Path(__file__).parent.parent / "control" / "downloaded_books.txt"
        )
        downloaded = (
            set(downloads_path.read_text().splitlines())
            if downloads_path.exists()
            else set()
        )

        scalability = []

        print("\n=== ESCALABILIDAD: Tiempo vs cantidad de consultas ===")

        for size in test_sizes:
            file_times = []
            sqlite_times = []

            for _ in range(size):
                book_id = random.randint(1, max_id)

                start = time.perf_counter()
                exists = str(book_id) in downloaded
                file_times.append(time.perf_counter() - start)

                start = time.perf_counter()
                exists = self.sqlite_service.is_downloaded(book_id)
                sqlite_times.append(time.perf_counter() - start)

            ratio = sum(file_times) / sum(sqlite_times)
            scalability.append(
                {
                    "queries": size,
                    "file_system_total_ms": sum(file_times) * 1000,
                    "sqlite_total_ms": sum(sqlite_times) * 1000,
                    "ratio": ratio,
                }
            )

            print(
                f"{size} consultas: File={sum(file_times)*1000:.2f}ms, SQLite={sum(sqlite_times)*1000:.2f}ms (ratio: {ratio:.1f}x)"
            )

        self.results["scalability"] = scalability

    def print_summary(self):
        """Resumen final"""
        print("\n" + "=" * 70)
        print(" RESUMEN DEL BENCHMARK")
        print("=" * 70)

        if "speed" in self.results:
            s = self.results["speed"]
            print(
                f"\n📊 VELOCIDAD ({s['num_queries']} consultas con {s['num_books']} libros):"
            )
            print(
                f"   SQLite es {s['speedup']:.1f}x más rápido en consultas individuales"
            )

        if "size" in self.results:
            sz = self.results["size"]
            print(f"\n💾 TAMAÑO EN DISCO:")
            print(
                f"   SQLite usa {sz['reduction_percent']:.1f}% menos espacio en disco"
            )

        if "scalability" in self.results:
            print(f"\n📈 ESCALABILIDAD:")
            last = self.results["scalability"][-1]
            print(
                f"   Con {last['queries']} consultas, SQLite es {last['ratio']:.1f}x más rápido"
            )

    def save_results(self, filename: str = "benchmark_results.json"):
        """Guardar resultados"""
        with open(filename, "w") as f:
            json.dump(self.results, f, indent=2)
        print(f"\n✅ Resultados guardados en {filename}")


if __name__ == "__main__":
    benchmark = Benchmark()

    benchmark.benchmark_speed(num_runs=50)
    benchmark.benchmark_size()
    benchmark.benchmark_scalability(test_sizes=[50])

    benchmark.print_summary()
    benchmark.save_results()
