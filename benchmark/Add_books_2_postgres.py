import configparser
from datetime import datetime
from pathlib import Path
import re
import random
import time
import psycopg2
from psycopg2 import sql

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
    "Book ID": r"#(\d{5,})",
    "Title": r"Title:\s*(.+)",
    "Author": r"Author:\s*(.+)",
    "Release date": r"Release date:\s*([A-Za-z]+\s\d{1,2},\s\d{4})",
    "Last updated date": [r"Most recently updated:\s*([A-Za-z]+\s\d{1,2},\s\d{4})", False],
    "Language": r"Language:\s*([A-Za-z0-9]+)",
    "Credits": r"Credits:\s*(.+)",
}
book_headers = []
book_header_data = {key: [] for key in PATTERNS.keys()}
dataset_sizes = [100, 1000, 10000, 40000]  # For benchmarking

# ---------- PostgreSQL Connection & DB Setup ----------
def connect_to_postgres(dbname=DB_NAME):
    """Connect to PostgreSQL database."""
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

def create_database():
    """Create database and table with indexes if not exist."""
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
                title TEXT NOT NULL,
                author TEXT NOT NULL,
                release_date DATE NOT NULL,
                last_updated_date DATE,
                language TEXT NOT NULL,
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

def insert_book_metadata(book_id, title, author, release_date, last_updated_date, language, credits):
    """Insert a single book record into the books table."""
    conn = connect_to_postgres(DB_NAME)
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO books (book_id, title, author, release_date, last_updated_date, language, credits)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (book_id) DO NOTHING;
        """, (book_id, title, author, release_date, last_updated_date, language, credits))
        conn.commit()
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
    finally:
        cur.close()
        conn.close()

# ---------- Helper Functions for Headers ----------
def format_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%B %d, %Y")
        return dt.strftime("%d/%m/%Y")
    except ValueError:
        return "INVALID DATE"

def find_pattern(data_name: str, pattern: str, book_header: str, essential: bool = True):
    if data_name == "Credits":
        match = re.search(pattern, book_header, re.DOTALL)
    else:
        match = re.search(pattern, book_header)
    if match:
        matched_data = match.group(1)
        if data_name.endswith("date"):
            matched_data = format_date(matched_data)
        book_header_data[data_name].append(matched_data)
    elif data_name == "Last updated date":
        book_header_data["Last updated date"].append(None)
    elif essential:
        raise Exception(f"{data_name} not found")

def read_header_files():
    datalake_root = Path(DATALAKE_PATH)
    if not datalake_root.exists():
        raise Exception(f"Datalake path does not exist: {DATALAKE_PATH}")
    for header_file in datalake_root.rglob("*.header.txt"):
        try:
            content = header_file.read_text()
            book_headers.append(content)
        except Exception as e:
            print(f"Failed to read {header_file}: {e}")

# ---------- Benchmarking Functions ----------
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
        cur.execute("SELECT * FROM books WHERE book_id = %s;", (book_id,))
        _ = cur.fetchone()
        end = time.perf_counter()
        times.append(end - start)
    cur.close()
    return sum(times) / len(times)

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

def benchmark():
    conn = connect_to_postgres(DB_NAME)
    if not conn:
        raise Exception("Could not connect to PostgreSQL database.")

    results = {}
    books = generate_books_from_headers()

    for size in dataset_sizes:
        if size > len(books):
            print(f"Skipping size {size}, not enough book headers available")
            continue

        sample_books = random.sample(books, size)
        print(f"\n📊 Benchmarking dataset size: {size}")

        insertion_time = measure_insertion_speed(conn, sample_books)
        random_author = random.choice(sample_books)["author"]
        author_query_time = measure_query_by_author(conn, random_author)
        random_book_id = random.choice(sample_books)["book_id"]
        path_query_time = measure_query_path(conn, random_book_id)

        results[size] = {
            "insertion_time": insertion_time,
            "author_query_time": author_query_time,
            "path_query_time": path_query_time
        }

    conn.close()
    return results

# ---------- Main Script ----------
if __name__ == "__main__":
    create_database()
    read_header_files()

    for i, header in enumerate(book_headers, start=1):
        for key in PATTERNS.keys():
            if key == "Last updated date":
                find_pattern(key, PATTERNS[key][0], header, PATTERNS[key][1])
            else:
                find_pattern(key, PATTERNS[key], header)

    books_for_insertion = generate_books_from_headers()
    for book in books_for_insertion:
        insert_book_metadata(
            book["book_id"],
            book["title"],
            book["author"],
            book["release_date"],
            book["last_updated_date"],
            book["language"],
            book["credits"]
        )

    benchmark_results = benchmark()
    for size, metrics in benchmark_results.items():
        print(f"{size} books -> Insert: {metrics['insertion_time']:.3f}s, "
              f"Author query: {metrics['author_query_time']:.6f}s, "
              f"Path query: {metrics['path_query_time']:.6f}s")
