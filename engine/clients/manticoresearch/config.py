MANTICORESEARCH_PORT = 9308
MANTICORESEARCH_TABLE = "bench"


def get_table_name() -> str:
    return MANTICORESEARCH_TABLE


def set_table_name(name: str) -> None:
    global MANTICORESEARCH_TABLE
    MANTICORESEARCH_TABLE = name
