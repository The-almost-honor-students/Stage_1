from application.bookService import download_book

success = download_book(1342, "staging/downloads")
print(success)
