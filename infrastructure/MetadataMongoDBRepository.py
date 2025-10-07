import hashlib
from application.MetadataRepository import MetadataRepository
from domain.book import Book
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

class MetadataMongoDBRepository(MetadataRepository):
    def __init__(self, client: MongoClient, db_name: str = "books", collection: str = "metadata"):
        self.collection = client[db_name][collection]
        self.client = client
        self.col: Collection = client[db_name][collection]
        self.col.create_index([("raw_text_hash", ASCENDING)], unique=True)

    def save_metadata(self, book: Book) -> str:
        if not book.book_id:
            raise ValueError("book_id es obligatorio para guardar en MongoDB.")
        doc = book.to_dict()
        doc["raw_text_hash"] = hashlib.sha256(str(book.book_id).encode()).hexdigest()
        self.col.insert_one(doc)

        doc = self.col.find_one({"book_id": book.book_id}, {"_id": 1})
        return str(doc["_id"]) if doc else ""
