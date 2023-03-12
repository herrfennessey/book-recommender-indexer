from cachetools import TTLCache, LRUCache

"""
This is a workaround for the fact that FastAPI doesn't retain classes beyond the request scope. This means that
things like caches need to be persisted in global variables, and then injected in constantly. 
"""

user_read_book_cache = TTLCache(maxsize=2000, ttl=60 * 10)
book_exists_cache = LRUCache(maxsize=4000)


def get_user_read_book_cache():
    return user_read_book_cache


def get_book_exists_cache():
    return book_exists_cache
