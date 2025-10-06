from pymongo import MongoClient

from application.bookService import BookService
from infrastructure.MetadataMongoDBRepository import MetadataMongoDBRepository

mongo_client = MongoClient("mongodb://localhost:27017")
book_service = BookService(MetadataMongoDBRepository(mongo_client, "books","metadata"))
book_service.create_metadata(8388,"/Users/giselabelmontecruz/PycharmProjects/Stage_1/datalake/20251005/00/8388.header.txt")