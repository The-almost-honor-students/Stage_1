import configparser
from datetime import datetime
from pathlib import Path
import random
import time
import psycopg2
from psycopg2 import sql
import matplotlib.pyplot as plt
import re

# ---------- Configuration ----------
DATALAKE_PATH = r"C:\Users\valko\PycharmProjects\Stage_1\datalake"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "books_metadata"

# ---------- Credentials ----------
config = configparser.ConfigParser()
config.read('config.cfg')
DB_USER = config.get('auth', 'db_username')
DB_PASSWORD = config.get('auth', 'db_password')

PATTERNS = {
    "Book ID": r"#(\d{1,})",
    "Title": r"Title:\s*(.+)",
    "Author": r"(Author|Editor):\s*([\w\sÀ-ÿ]+)",
    "Release date": r"Release date:\s*([A-Za-z]+\s\d{1,2},\s\d{4})",
    "Last updated date": r"Most recently updated:\s*([A-Za-z]+\s\d{1,2},\s\d{4})",
    "Language": r"Language:\s*([A-Za-z0-9]+)",
    "Credits": r"Credits:\s*(.+)",
}
book_headers = []
book_header_data = {key: [] for key in PATTERNS.keys()}
dataset_sizes = [100, 1000, 10000]  # For benchmarking

# ---------- PostgreSQL Connection ----------
def connect_to_postgres(dbname=DB_NAME):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"Connection failed: {e}")
        return None

# ---------- DB Setup ----------
def create_database():
    conn = connect_to_postgres("postgres")
    if not conn:
        return
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL(f"CREATE DATABASE {DB_NAME}"))
        print(f"✅ Database '{DB_NAME}' created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"⚠️ Database '{DB_NAME}' already exists.")
    finally:
        cur.close()
        conn.close()

    conn = connect_to_postgres(DB_NAME)
    if not conn:
        return
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS books (
                book_id INT PRIMARY KEY,
                title TEXT,
                author TEXT,
                release_date DATE,
                last_updated_date DATE,
                language TEXT,
                credits TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_author ON books(author);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_title ON books(title);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_release_date ON books(release_date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_language ON books(language);")
        conn.commit()
        print(f"✅ Table 'books' and indexes created successfully.")
    except Exception as e:
        print(f"❌ Error creating table or indexes: {e}")
    finally:
        cur.close()
        conn.close()

# ---------- Helper Functions ----------
def format_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%B %d, %Y")
        return dt.strftime("%d/%m/%Y")
    except ValueError:
        return "INVALID DATE"

def find_pattern(data_name: str, pattern: str, book_header: str, essential: bool = False):
    match = re.search(pattern, book_header)

    if match:
        # Use the last captured group if multiple exist
        matched_data = match.group(match.lastindex) if match.lastindex else match.group(0)

        if data_name.endswith("date"):
            matched_data = format_date(matched_data)
            if matched_data == 'INVALID DATE':
                matched_data = None  # Store None instead of "invalid date"

        book_header_data[data_name].append(matched_data)
    else:
        book_header_data[data_name].append(None)


def read_header_files():
    datalake_root = Path(DATALAKE_PATH)
    if not datalake_root.exists():
        raise Exception(f"Datalake path does not exist: {DATALAKE_PATH}")
    for header_file in datalake_root.rglob("*.header.txt"):
        try:
            content = header_file.read_text(encoding="utf-8")
            book_headers.append(content)
        except Exception as e:
            print(f"Failed to read {header_file}: {e}")

# ---------- Benchmark Helper ----------
def summarize(elapsed_s, n_ops):
    total_ms = elapsed_s * 1000.0
    ops_sec = n_ops / elapsed_s if elapsed_s > 0 else float("inf")
    avg_ms = total_ms / n_ops
    return total_ms, ops_sec, avg_ms

# ---------- Benchmarking Functions ----------
def benchmark_insert_books(books):
    conn = connect_to_postgres(DB_NAME)
    cur = conn.cursor()
    t0 = time.perf_counter()
    for book in books:
        cur.execute("""
            INSERT INTO books (book_id, title, author, release_date, last_updated_date, language, credits)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (book_id) DO NOTHING;
        """, (book["book_id"], book["title"], book["author"], book["release_date"],
              book["last_updated_date"], book["language"], book["credits"]))
    conn.commit()
    t1 = time.perf_counter()
    cur.close()
    conn.close()
    return summarize(t1 - t0, len(books))

def benchmark_query_by_author(books):
    conn = connect_to_postgres(DB_NAME)
    cur = conn.cursor()
    n_queries = len(books)
    authors = [b["author"] for b in books]
    t0 = time.perf_counter()
    for _ in range(n_queries):
        random_author = random.choice(authors)
        cur.execute("SELECT * FROM books WHERE author = %s;", (random_author,))
        _ = cur.fetchall()
    t1 = time.perf_counter()
    cur.close()
    conn.close()
    return summarize(t1 - t0, n_queries)

def benchmark_query_by_id(books):
    conn = connect_to_postgres(DB_NAME)
    cur = conn.cursor()
    n_queries = len(books)
    ids = [b["book_id"] for b in books]
    t0 = time.perf_counter()
    for _ in range(n_queries):
        random_id = random.choice(ids)
        cur.execute("SELECT * FROM books WHERE book_id = %s;", (random_id,))
        _ = cur.fetchone()
    t1 = time.perf_counter()
    cur.close()
    conn.close()
    return summarize(t1 - t0, n_queries)

# ---------- Generate Books for Benchmark ----------
def generate_books_from_headers():
    books = []
    for i in range(len(book_header_data["Book ID"])):
        books.append({
            "book_id": book_header_data["Book ID"][i],
            "title": book_header_data["Title"][i],
            "author": book_header_data["Author"][i],
            "release_date": book_header_data["Release date"][i],
            "last_updated_date": book_header_data["Last updated date"][i],
            "language": book_header_data["Language"][i],
            "credits": book_header_data["Credits"][i]
        })
    return books

def generate_benchmark_plots(dataset_sizes, insert_metrics, author_metrics, id_metrics, output_dir="postgres_plots"):
    """
    Generate benchmark plots for PostgreSQL.

    Args:
        dataset_sizes (list[int]): Sizes of each dataset.
        insert_metrics (tuple): (totals, ops/sec, avg_ms) for insert operations.
        author_metrics (tuple): (totals, ops/sec, avg_ms) for author queries.
        id_metrics (tuple): (totals, ops/sec, avg_ms) for ID queries.
        output_dir (str): Directory to save plots.
    """
    plots_dir = Path(output_dir)
    plots_dir.mkdir(exist_ok=True)

    insert_totals, insert_ops, insert_avg = insert_metrics
    author_totals, author_ops, author_avg = author_metrics
    id_totals, id_ops, id_avg = id_metrics

    # 1️⃣ Total Execution Time
    plt.figure(figsize=(10, 5))
    plt.plot(dataset_sizes, insert_totals, marker='o', label="Insert Total Time")
    plt.plot(dataset_sizes, author_totals, marker='o', label="Author Query Total Time")
    plt.plot(dataset_sizes, id_totals, marker='o', label="ID Query Total Time")
    plt.xlabel("Number of Books")
    plt.ylabel("Total Time (ms)")
    plt.title("Total Execution Time by Dataset Size")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plots_dir / "total_time.png", dpi=140)
    plt.close()

    # 2️⃣ Throughput (Ops/sec)
    plt.figure(figsize=(10, 5))
    plt.plot(dataset_sizes, insert_ops, marker='o', label="Insert Ops/s")
    plt.plot(dataset_sizes, author_ops, marker='o', label="Author Query Ops/s")
    plt.plot(dataset_sizes, id_ops, marker='o', label="ID Query Ops/s")
    plt.xlabel("Number of Books")
    plt.ylabel("Operations per Second")
    plt.title("Throughput by Dataset Size")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plots_dir / "throughput_ops.png", dpi=140)
    plt.close()

    # 3️⃣ Average Latency
    plt.figure(figsize=(10, 5))
    plt.plot(dataset_sizes, insert_avg, marker='o', label="Insert Avg Latency")
    plt.plot(dataset_sizes, author_avg, marker='o', label="Author Query Avg Latency")
    plt.plot(dataset_sizes, id_avg, marker='o', label="ID Query Avg Latency")
    plt.xlabel("Number of Books")
    plt.ylabel("Average Time per Operation (ms)")
    plt.title("Average Latency by Dataset Size")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plots_dir / "avg_latency.png", dpi=140)
    plt.close()

    print(f"\n✅ Plots saved in: {plots_dir.resolve()}")

# ---------- Main Benchmark ----------
if __name__ == "__main__":
    create_database()
    read_header_files()

    for i, header in enumerate(book_headers, start=1):
        for key in PATTERNS.keys():
            if key == "Last updated date":
                find_pattern(key, PATTERNS[key][0], header, PATTERNS[key][1])
            else:
                find_pattern(key, PATTERNS[key], header)

    books_for_benchmark = generate_books_from_headers()

    insert_totals, insert_ops, insert_avg = [], [], []
    query_author_totals, query_author_ops, query_author_avg = [], [], []
    query_id_totals, query_id_ops, query_id_avg = [], [], []

    print("=" * 110)
    print(f"{'N_BOOKS':>10} | {'INS TOTAL (ms)':>15} | {'INS OPS/s':>12} | {'INS AVG (ms)':>12} | "
          f"{'AUTH TOTAL (ms)':>15} | {'AUTH OPS/s':>12} | {'AUTH AVG (ms)':>12} | "
          f"{'ID TOTAL (ms)':>15} | {'ID OPS/s':>12} | {'ID AVG (ms)':>12}")
    print("=" * 110)

    for n in dataset_sizes:
        if n > len(books_for_benchmark):
            print(f"Skipping {n}, not enough books")
            continue
        sample_books = random.sample(books_for_benchmark, n)

        ins_total, ins_ops_val, ins_avg_val = benchmark_insert_books(sample_books)
        auth_total, auth_ops_val, auth_avg_val = benchmark_query_by_author(sample_books)
        id_total, id_ops_val, id_avg_val = benchmark_query_by_id(sample_books)

        insert_totals.append(ins_total)
        insert_ops.append(ins_ops_val)
        insert_avg.append(ins_avg_val)
        query_author_totals.append(auth_total)
        query_author_ops.append(auth_ops_val)
        query_author_avg.append(auth_avg_val)
        query_id_totals.append(id_total)
        query_id_ops.append(id_ops_val)
        query_id_avg.append(id_avg_val)

        print(f"{n:>10} | {ins_total:>15.2f} | {ins_ops_val:>12.0f} | {ins_avg_val:>12.3f} | "
              f"{auth_total:>15.2f} | {auth_ops_val:>12.0f} | {auth_avg_val:>12.3f} | "
              f"{id_total:>15.2f} | {id_ops_val:>12.0f} | {id_avg_val:>12.3f}")
    print("=" * 110)

    # --- Generate plots ---
    generate_benchmark_plots(
        dataset_sizes,
        (insert_totals, insert_ops, insert_avg),
        (query_author_totals, query_author_ops, query_author_avg),
        (query_id_totals, query_id_ops, query_id_avg)
    )
