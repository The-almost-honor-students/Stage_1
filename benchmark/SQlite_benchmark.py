import time
import random
import statistics
from pathlib import Path
import json
import sqlite3
import matplotlib.pyplot as plt


class MetadataBenchmark:
    def __init__(
        self,
        datalake_path=f"{Path(__file__).resolve().parent.parent}/datalake",
        db_path=f"{Path(__file__).resolve().parent.parent}/benchmark/books.db",
    ):
        self.datalake_path = Path(datalake_path)
        self.db_path = Path(db_path)
        self.results = {}
        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()
        self._create_table()

    # -------------------------------
    # Crear tabla SQLite
    # -------------------------------
    def _create_table(self):
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS books (
                id INTEGER PRIMARY KEY,
                title TEXT,
                author TEXT,
                year INTEGER,
                categories TEXT
            )
        """
        )
        self.conn.commit()

    # -------------------------------
    # Cargar metadatos del datalake a SQLite
    # -------------------------------
    def load_metadata_to_sqlite(self, max_files=None):
        files = list(self.datalake_path.rglob("*.header.txt"))
        if max_files:
            files = files[:max_files]

        for i, file_path in enumerate(files, 1):
            try:
                book_id = int(file_path.stem.split(".")[0])
                metadata = self._read_metadata(file_path)
                categories_str = ",".join(metadata.get("categories", []))
                self.cur.execute(
                    """
                    INSERT OR REPLACE INTO books (id, title, author, year, categories)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        book_id,
                        metadata.get("title"),
                        metadata.get("author"),
                        metadata.get("year"),
                        categories_str,
                    ),
                )
            except Exception as e:
                print(f"[ERROR] No se pudo procesar {file_path}: {e}")

        self.conn.commit()
        print(f"✓ {len(files)} archivos cargados en SQLite")

    # -------------------------------
    # Leer metadatos de un archivo
    # -------------------------------
    def _read_metadata(self, file_path):
        metadata = {}
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                if ":" in line:
                    key, value = line.strip().split(":", 1)
                    metadata[key.strip()] = value.strip()
        if "categories" in metadata:
            metadata["categories"] = [
                c.strip() for c in metadata["categories"].split(",")
            ]
        return metadata

    # -------------------------------
    # Benchmark velocidad
    # -------------------------------
    def benchmark_speed(self, num_queries=1000):
        files = list(self.datalake_path.rglob("*.header.txt"))
        if not files:
            print("[WARN] No hay archivos en el datalake")
            return

        file_times = []
        sqlite_times = []

        for _ in range(num_queries):
            file_path = random.choice(files)
            book_id = int(file_path.stem.split(".")[0])

            # File System
            start = time.perf_counter()
            _ = self._read_metadata(file_path)
            file_times.append(time.perf_counter() - start)

            # SQLite
            start = time.perf_counter()
            self.cur.execute(
                "SELECT title, author, year, categories FROM books WHERE id=?",
                (book_id,),
            )
            _ = self.cur.fetchone()
            sqlite_times.append(time.perf_counter() - start)

        speedup = statistics.mean(file_times) / statistics.mean(sqlite_times)

        self.results["speed"] = {
            "num_files": len(files),
            "num_queries": num_queries,
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

        print(f"\nBenchmark Velocidad ({num_queries} consultas):")
        print(
            f"File System: {self.results['speed']['file_system']['mean_ms']:.4f} ms/query"
        )
        print(
            f"SQLite: {self.results['speed']['sqlite']['mean_ms']:.4f} ms/query"
        )
        print(f"Speedup SQLite: {speedup:.2f}x")

    # -------------------------------
    # Benchmark tamaño en disco
    # -------------------------------
    def benchmark_size(self):
        file_size = sum(
            f.stat().st_size for f in self.datalake_path.rglob("*.header.txt")
        )
        sqlite_size = (
            self.db_path.stat().st_size if self.db_path.exists() else 0
        )

        self.results["size"] = {
            "file_system_mb": file_size / (1024 * 1024),
            "sqlite_mb": sqlite_size / (1024 * 1024),
            "reduction_percent": (
                ((file_size - sqlite_size) / file_size * 100)
                if file_size > 0
                else 0
            ),
        }

        print(f"\nTamaño en disco:")
        print(f"File System: {self.results['size']['file_system_mb']:.2f} MB")
        print(f"SQLite: {self.results['size']['sqlite_mb']:.2f} MB")
        print(f"Reducción: {self.results['size']['reduction_percent']:.1f}%")

    # -------------------------------
    # Benchmark escalabilidad
    # -------------------------------
    def benchmark_scalability(self, test_sizes=None):
        if test_sizes is None:
            test_sizes = [10, 50, 100, 500, 1000]

        files = list(self.datalake_path.rglob("*.header.txt"))
        scalability = []

        for size in test_sizes:
            file_times = []
            sqlite_times = []

            for _ in range(size):
                file_path = random.choice(files)
                book_id = int(file_path.stem.split(".")[0])

                start = time.perf_counter()
                _ = self._read_metadata(file_path)
                file_times.append(time.perf_counter() - start)

                start = time.perf_counter()
                self.cur.execute(
                    "SELECT title, author, year, categories FROM books WHERE id=?",
                    (book_id,),
                )
                _ = self.cur.fetchone()
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
                f"{size} consultas: File={sum(file_times)*1000:.2f} ms, SQLite={sum(sqlite_times)*1000:.2f} ms, ratio={ratio:.2f}x"
            )

        self.results["scalability"] = scalability

    # -------------------------------
    # Guardar resultados
    # -------------------------------
    def save_results(self, filename="benchmark_results.json"):
        with open(filename, "w") as f:
            json.dump(self.results, f, indent=2)
        print(f"\nResultados guardados en {filename}")

    # -------------------------------
    # Visualización
    # -------------------------------
    def plot_results(self):
        import matplotlib.pyplot as plt

        # Velocidad
        if "speed" in self.results:
            speed = self.results["speed"]
            labels = ["File System", "SQLite"]
            means = [
                speed["file_system"]["mean_ms"],
                speed["sqlite"]["mean_ms"],
            ]
            plt.figure(figsize=(6, 4))
            plt.bar(labels, means, color=["skyblue", "orange"])
            plt.ylabel("Tiempo promedio por consulta (ms)")
            plt.title(
                f"Benchmark de Velocidad ({speed['num_queries']} consultas)"
            )
            plt.show()

        # Tamaño
        if "size" in self.results:
            size = self.results["size"]
            labels = ["File System", "SQLite"]
            sizes_mb = [size["file_system_mb"], size["sqlite_mb"]]
            plt.figure(figsize=(6, 4))
            plt.bar(labels, sizes_mb, color=["lightgreen", "salmon"])
            plt.ylabel("Espacio en disco (MB)")
            plt.title("Comparación de tamaño en disco")
            plt.show()

        # Escalabilidad
        if "scalability" in self.results:
            queries = [s["queries"] for s in self.results["scalability"]]
            file_times = [
                s["file_system_total_ms"] for s in self.results["scalability"]
            ]
            sqlite_times = [
                s["sqlite_total_ms"] for s in self.results["scalability"]
            ]
            ratios = [s["ratio"] for s in self.results["scalability"]]

            plt.figure(figsize=(10, 4))
            plt.plot(
                queries,
                file_times,
                marker="o",
                label="File System",
                color="skyblue",
            )
            plt.plot(
                queries,
                sqlite_times,
                marker="x",
                label="SQLite",
                color="orange",
            )
            plt.xlabel("Número de consultas")
            plt.ylabel("Tiempo total (ms)")
            plt.title("Escalabilidad: Tiempo vs Número de consultas")
            plt.legend()
            plt.grid(True)
            plt.show()

            plt.figure(figsize=(10, 4))
            plt.plot(queries, ratios, marker="o", color="purple")
            plt.xlabel("Número de consultas")
            plt.ylabel("Ratio File/SQLite")
            plt.title("Ratio de velocidad File System / SQLite")
            plt.grid(True)
            plt.show()

    # -------------------------------
    # Cerrar conexión SQLite
    # -------------------------------
    def close(self):
        self.conn.close()


# -------------------------------
# EJECUCIÓN PRINCIPAL
# -------------------------------
if __name__ == "__main__":
    benchmark = MetadataBenchmark()

    # Cargar metadatos en SQLite
    benchmark.load_metadata_to_sqlite()

    # Ejecutar benchmarks
    benchmark.benchmark_speed(num_queries=5000)
    benchmark.benchmark_size()
    benchmark.benchmark_scalability(test_sizes=[500, 1000, 5000, 10000])

    # Resultados
    benchmark.save_results()
    benchmark.plot_results()

    benchmark.close()
