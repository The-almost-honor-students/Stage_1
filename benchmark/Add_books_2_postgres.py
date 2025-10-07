from datetime import datetime
from pathlib import Path
import re
import  PostgreSQL_functions


DATALAKE_PATH = r"C:\Users\valko\PycharmProjects\Stage_1\datalake"

PATTERNS = {
    "Book ID": r"#(\d{5,})",
    "Title": r"Title:\s*(.+)",
    "Author": r"Author:\s*(.+)",
    "Release date": r"Release date:\s*([A-Za-z]+\s\d{1,2},\s\d{4})",  # letters and digits, no spaces/specials
    "Last updated date": [r"Most recently updated:\s*([A-Za-z]+\s\d{1,2},\s\d{4})", False],  # unchanged
    "Language": r"Language:\s*([A-Za-z0-9]+)",
    "Credits": r"Credits:\s*(.+)",
}

book_headers = []
book_header_data = {
    "Book ID": [],
    "Title": [],
    "Author": [],
    "Release date": [],
    "Last updated date": [],
    "Language": [],
    "Credits": [],
}

def format_date(date_str: str) -> str:
    """Convert 'Month DD, YYYY' to 'DD/MM/YYYY'."""
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
    """
    Reads all _header.txt files in the datalake and returns a list of their contents.

    datalake_path: str, path to the root datalake folder (e.g., "datalake/")
    """
    #header_text = None
    datalake_root = Path(DATALAKE_PATH)
    if not datalake_root.exists():
        raise Exception(f"Datalake path does not exist: {DATALAKE_PATH}")
    else:
        print(f"\ndatalake_path found")

    # Recursively search for all *_header.txt files
    for header_file in datalake_root.rglob("*.header.txt"):
        try:
            content = header_file.read_text(encoding="utf-8")
            book_headers.append(content)
        except Exception as e:
            print(f"Failed to read {header_file}: {e}")

    print(f"✅ Book headers read successfully.")




# ---------- Run all functions for each book ----------
PostgreSQL_functions.create_database()

read_header_files()

for i, header in enumerate(book_headers, start=1):
    for key in PATTERNS.keys():
        if key == "Last updated date":
            find_pattern(key, PATTERNS[key][0], header, PATTERNS[key][1])
        else:
            find_pattern(key, PATTERNS[key], header)
    print(f"✅ Data from header '{i}' extracted successfully.")


for i in range(len(book_header_data["Book ID"])):
    PostgreSQL_functions.insert_book_metadata(
        book_header_data["Book ID"][i],
        book_header_data["Title"][i],
        book_header_data["Author"][i],
        book_header_data["Release date"][i],
        book_header_data["Last updated date"][i],
        book_header_data["Language"][i],
        book_header_data["Credits"][i]
    )
print(f"✅ Data added to postgreSQL successfully.")
