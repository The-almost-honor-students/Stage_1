import configparser
from datetime import datetime
import psycopg2
from psycopg2 import sql



# ---------- Configuration ----------
DB_HOST = "localhost"        # or your server's hostname
DB_PORT = "5432"             # default PostgresSQL port
DB_NAME = "books_metadata"   # database name


# ---------- Credentials ----------
config = configparser.ConfigParser()
config.read('config.cfg')
DB_USER = config.get('auth', 'db_username')         # PostgresSQL username
DB_PASSWORD = config.get('auth', 'db_password')  # PostgresSQL password


# ---------- Create a connection ----------
def connect_to_postgres(dbname="postgres"):
    """Connect to the default PostgresSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        #print(f"Connected to database: {dbname}")
        return conn
    except Exception as e:
        print(f"Connection failed: {e}")
        return None

# ---------- Create the database ----------
def create_database():
    """Create the target database if it doesn't already exist."""
    conn = connect_to_postgres("postgres")
    print(f"✅ Connected to database 'postgres' successfully.")
    if not conn:
        return
    try:
        cur = conn.cursor()
        # Try to create the new database
        cur.execute(sql.SQL(f"CREATE DATABASE {DB_NAME}"))
        print(f"✅ Database '{DB_NAME}' created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"⚠️ Database '{DB_NAME}' already exists.")
    finally:
        cur.close()
        conn.close()

    # ---- NEW SECTION: connect to the newly created database ----
    conn = connect_to_postgres(DB_NAME)
    print(f"✅ Connected to database '{DB_NAME}' successfully.")
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
        print(f"✅ Table 'books' created successfully.")

        # Create indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_author ON books(author);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_title ON books(title);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_release_date ON books(release_date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_language ON books(language);")
        conn.commit()
        print(f"✅ Indexes created successfully.")

    except Exception as e:
        print(f"❌ Error creating table or indexes: {e}")
    finally:
        cur.close()
        conn.close()


# ---------- Insert book metadata ----------
def insert_book_metadata(book_id: int, title: str, author: str, release_date: datetime, last_updated_date: datetime, language: str, book_credits: str):
    """Insert a single book record into the books table."""
    conn = connect_to_postgres(DB_NAME)
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO books (book_id, title, author, release_date, last_updated_date, language, credits)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (book_id, title, author, release_date, last_updated_date, language, book_credits))
        conn.commit()
        print(f"📚 Added book: ID #{book_id}-'{title}' by {author}")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
    finally:
        cur.close()
        conn.close()

