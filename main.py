from application.bookService import download_book, create_datalake

success = download_book(1342, "staging/downloads")
print(success)

print(create_datalake(1342))

