import requests
from datetime import date, timedelta
from src.db import db_cursor
from src.settings import Settings

BASE_URL = "https://api.webmaster.yandex.net/v4"


def fetch_queries(date_from, date_to, limit=500, offset=0):
    url = f"{BASE_URL}/user/{Settings.WEBMASTER_USER_ID}/hosts/{Settings.WEBMASTER_HOST_ID}/search-queries/popular"
    headers = {
        "Authorization": f"OAuth {Settings.WEBMASTER_TOKEN}"
    }
    params = [
        ("date_from", date_from),
        ("date_to", date_to),
        ("order_by", "TOTAL_SHOWS"),
        ("query_indicator", "TOTAL_SHOWS"),
        ("query_indicator", "TOTAL_CLICKS"),
        ("query_indicator", "AVG_SHOW_POSITION"),
        ("query_indicator", "AVG_CLICK_POSITION"),
        ("limit", str(limit)),
        ("offset", str(offset)),
    ]

    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def save_queries(data, report_date):
    queries = data.get("queries", [])
    inserted = 0

    with db_cursor() as (_, cur):
        for q in queries:
            indicators = q.get("indicators", {}) or {}

            impressions = indicators.get("TOTAL_SHOWS", 0) or 0
            clicks = indicators.get("TOTAL_CLICKS", 0) or 0
            avg_position = indicators.get("AVG_SHOW_POSITION", 0) or 0
            ctr = round(clicks / impressions, 4) if impressions > 0 else 0

            cur.execute(
                """
                insert into stg_webmaster_query_daily
                (
                    date,
                    site_id,
                    query_id,
                    query_text,
                    page_url,
                    impressions,
                    clicks,
                    ctr,
                    avg_position,
                    loaded_at
                )
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                on conflict (date, site_id, query_text, page_url)
                do update set
                    query_id = excluded.query_id,
                    impressions = excluded.impressions,
                    clicks = excluded.clicks,
                    ctr = excluded.ctr,
                    avg_position = excluded.avg_position,
                    loaded_at = now()
                """,
                (
                    report_date,
                    Settings.WEBMASTER_HOST_ID,
                    q.get("query_id", ""),
                    q.get("query_text", ""),
                    "",
                    impressions,
                    clicks,
                    ctr,
                    avg_position,
                )
            )
            inserted += 1

    return inserted


def run(date_from=None, date_to=None):
    if date_to is None:
        date_to = (date.today() - timedelta(days=1)).isoformat()
    if date_from is None:
        date_from = (date.today() - timedelta(days=14)).isoformat()

    data = fetch_queries(date_from, date_to)
    rows = save_queries(data, date_to)
    return {
        "status": "ok",
        "rows_loaded": rows,
        "date_from": date_from,
        "date_to": date_to,
        "total_found": data.get("count", 0)
    }


if __name__ == "__main__":
    print(run())
