import psycopg2
from src.settings import Settings

def get_connection():
    return psycopg2.connect(
        host=Settings.PG_HOST,
        port=Settings.PG_PORT,
        dbname=Settings.PG_DB,
        user=Settings.PG_USER,
        password=Settings.PG_PASSWORD,
        sslmode="require"
    )
