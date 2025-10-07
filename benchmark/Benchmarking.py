import random
import time
import PostgreSQL_functions
from benchmark.PostgreSQL_functions import DB_NAME

dataset_sizes = [100, 1000, 10000, 40000]

def measure_insertion_speed(conn, books):
    cur = conn.cursor()
    start = time.perf_counter()
    for book in books:
        cur.execute("""
            INSERT INTO books (book_id, title, author, release_date, last_updated_date, language, credits)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (book_id) DO NOTHING;
        """, (book["book_id"], book["title"], book["author"], book["release_date"],
              book["last_updated_date"], book["language"], book["credits"]))
    conn.commit()
    end = time.perf_counter()
    cur.close()
    return end - start

def measure_query_by_author(conn, author, repetitions=5):
    times = []
    cur = conn.cursor()
    for _ in range(repetitions):
        start = time.perf_counter()
        cur.execute("SELECT * FROM books WHERE author = %s;", (author,))
        _ = cur.fetchall()
        end = time.perf_counter()
        times.append(end - start)
    cur.close()
    return sum(times) / len(times)

def measure_query_path(conn, book_id, repetitions=5):
    times = []
    cur = conn.cursor()
    for _ in range(repetitions):
        start = time.perf_counter()
        cur.execute("SELECT path FROM books WHERE book_id = %s;", (book_id,))
        _ = cur.fetchone()
        end = time.perf_counter()
        times.append(end - start)
    cur.close()
    return sum(times) / len(times)

def benchmark():
    conn = PostgreSQL_functions.connect_to_postgres(DB_NAME)
    if not conn:
        raise Exception("Could not connect to PostgreSQL database.")

    results = {}

    for size in dataset_sizes:
        print(f"\n📊 Benchmarking dataset size: {size}")

        books = generate_books(size)

        # --- Measure insertion ---
        insertion_time = measure_insertion_speed(conn, books)
        print(f"Insertion of {size} books took {insertion_time:.3f} seconds")

        # --- Measure query by author ---
        random_author = random.choice(books)["author"]
        author_query_time = measure_query_by_author(conn, random_author)
        print(f"Querying author '{random_author}' took {author_query_time:.6f} seconds (avg)")

        # --- Measure query path ---
        random_book_id = random.choice(books)["book_id"]
        path_query_time = measure_query_path(conn, random_book_id)
        print(f"Querying path for book ID {random_book_id} took {path_query_time:.6f} seconds (avg)")

        # --- Store results ---
        results[size] = {
            "insertion_time": insertion_time,
            "author_query_time": author_query_time,
            "path_query_time": path_query_time
        }

    conn.close()
    return results

# ---------- Run benchmark ----------
if __name__ == "__main__":
    benchmark_results = benchmark()
    print("\n✅ Benchmark completed:")
    for size, metrics in benchmark_results.items():
        print(f"{size} books -> Insert: {metrics['insertion_time']:.3f}s, "
              f"Author query: {metrics['author_query_time']:.6f}s, "
              f"Path query: {metrics['path_query_time']:.6f}s")