from domain.book import Book
from infrastructure.InvertedIndexMongoDBRepository import InvertedIndexMongoDBRepository


def main():
    repo = InvertedIndexMongoDBRepository(
        uri="mongodb://localhost:27017",
        db_name="bench_inverted",
        datalake_root="/Users/giselabelmontecruz/PycharmProjects/Stage_1/datalake",
        stopwords_path="stopwords.txt",
        use_stemming=True,
    )

    book_ids = [8388, 32972, 67041, 1342]

    print(f"\n📚 Indexando libros: {book_ids}\n")
    for bid in book_ids:
        success = repo.index_book(Book(book_id=bid, title=None, author=None, language=None))
        print(f"  → Libro {bid} {'indexado' if success else 'omitido'}")

    term = "Angels"
    results = repo.search(term)
    print(f"\n🔍 Libros donde aparece el término '{term.lower()}': {results}")

if __name__ == "__main__":
    main()