from contextlib import contextmanager
import psycopg2
from src.settings import Settings


def get_connection():
    return psycopg2.connect(
        host=Settings.PG_HOST,
        port=Settings.PG_PORT,
        dbname=Settings.PG_DB,
        user=Settings.PG_USER,
        password=Settings.PG_PASSWORD,
        sslmode="disable",
    )


@contextmanager
def db_cursor():
    conn = get_connection()
    cur = conn.cursor()
    try:
        yield conn, cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
