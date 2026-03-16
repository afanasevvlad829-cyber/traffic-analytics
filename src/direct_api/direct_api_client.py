import os
import requests

DIRECT_API_URL = "https://api.direct.yandex.com/json/v5"

TOKEN = os.getenv("YANDEX_DIRECT_TOKEN")
CLIENT_LOGIN = os.getenv("YANDEX_DIRECT_LOGIN")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept-Language": "ru",
    "Client-Login": CLIENT_LOGIN,
    "Content-Type": "application/json"
}

def request(method, params):

    body = {
        "method": method,
        "params": params
    }

    r = requests.post(
        DIRECT_API_URL,
        json=body,
        headers=HEADERS
    )

    if r.status_code != 200:
        raise Exception(f"Direct API HTTP error {r.status_code}: {r.text}")

    data = r.json()

    if "error" in data:
        raise Exception(data["error"])

    return data["result"]
