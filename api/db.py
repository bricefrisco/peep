from contextlib import contextmanager

import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

_pool = None


def init_pool(database_url, minconn=1, maxconn=10):
    global _pool
    _pool = ThreadedConnectionPool(minconn, maxconn, database_url)


def close_pool():
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


@contextmanager
def _cursor():
    conn = _pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def get_db():
    """FastAPI dependency -- yields a cursor borrowed from the pool for the
    duration of one request."""
    with _cursor() as cur:
        yield cur
