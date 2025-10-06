from abc import ABC
from application.MetadataRepository import MetadataRepository
from domain.book import Book
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

class MetadataMongoDBRepository(MetadataRepository):
    def __init__(self, client: MongoClient, db_name: str = "books", collection: str = "metadata"):
        self.client = client
        self.col: Collection = client[db_name][collection]
        # Índice único para idempotencia por hash del texto
        self.col.create_index([("raw_text_hash", ASCENDING)], unique=True)

    def save_metadata(self, book: Book) -> str:
        if not book.book_id:
            raise ValueError("book_id es obligatorio para guardar en MongoDB.")

        self.col.update_one(
            {"book_id": book.book_id},
            {"$set": book.to_dict()},
            upsert=True
        )

        doc = self.col.find_one({"book_id": book.book_id}, {"_id": 1})
        return str(doc["_id"]) if doc else ""
