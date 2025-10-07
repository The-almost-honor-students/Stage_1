import os
import re
import json
import unicodedata
from collections import defaultdict

STOPWORDS = {
    "el", "la", "los", "las", "de", "del", "y", "a", "en",
    "the", "and", "of", "to", "in",
    "le", "la", "les", "et", "de", "à", "en"
}

MIN_LENGTH = 3  # Ignore very short words (1-2 letters)


def clean_text_simple(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = text.lower()
    text = re.sub(r"[^a-z\u00C0-\u017F\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def build_datalake_index(folder: str, output_file: str = "inverted_index.json"):
    # If an index already exists, load it for incremental update
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8") as f:
            index = defaultdict(list, json.load(f))
    else:
        index = defaultdict(list)

    # Iterate through all files in the folder
    for file in os.listdir(folder):
        if file.endswith(".txt") and "body" in file.lower():
            book_code_match = re.match(r"(\d+)", file)
            if not book_code_match:
                continue
            book_code = book_code_match.group(1)

            # Avoid re-indexing the same book if it's already in the index
            if any(book_code in books for books in index.values()):
                continue

            body_path = os.path.join(folder, file)
            with open(body_path, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()

            clean_text = clean_text_simple(text)
            words = set(clean_text.split())

            for word in words:
                if word in STOPWORDS or len(word) < MIN_LENGTH:
                    continue
                index[word].append(book_code)

    # Save to JSON
    index_json = dict(index)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(index_json, f, ensure_ascii=False, indent=2)

    return index_json


if __name__ == "__main__":
    datalake_path = r"C:\Users\salsa\PycharmProjects\Stage_1\datalake\20251006\22"
    index = build_datalake_index(datalake_path, "inverted_index.json")
    print(f"Inverted index created with {len(index)} words and saved to 'inverted_index.json'")
