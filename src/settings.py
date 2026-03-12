import os

class Settings:
    PG_HOST = os.getenv("PG_HOST","")
    PG_PORT = int(os.getenv("PG_PORT","6432"))
    PG_DB = os.getenv("PG_DB","traffic_analytics")
    PG_USER = os.getenv("PG_USER","")
    PG_PASSWORD = os.getenv("PG_PASSWORD","")

    DIRECT_TOKEN = os.getenv("DIRECT_TOKEN","")
    DIRECT_CLIENT_LOGIN = os.getenv("DIRECT_CLIENT_LOGIN","")

    METRICA_TOKEN = os.getenv("METRICA_TOKEN","")
    METRICA_COUNTER_ID = os.getenv("METRICA_COUNTER_ID","")

    WEBMASTER_TOKEN = os.getenv("WEBMASTER_TOKEN","")
    WEBMASTER_HOST_ID = os.getenv("WEBMASTER_HOST_ID","")
