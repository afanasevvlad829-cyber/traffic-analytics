import os
ENV_PATH = "/home/kv145/traffic-analytics/.env"
import json
import requests
import psycopg2
from datetime import date
from urllib.parse import urlparse
from lxml import etree

KEYWORDS_FILE = "/home/kv145/traffic-analytics/config/competitor_keywords.txt"

def load_env(path: str):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

load_env(ENV_PATH)

def read_keywords():
    with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
        return [x.strip() for x in f if x.strip()]

def main():
    api_key = os.getenv("YANDEX_SEARCH_API_KEY")
    folder_id = os.getenv("YANDEX_SEARCH_FOLDER_ID")
    region = os.getenv("YANDEX_SEARCH_REGION", "213")

    print("YANDEX_SEARCH_API_KEY present:", bool(api_key))
    print("YANDEX_SEARCH_FOLDER_ID:", folder_id)
    print("YANDEX_SEARCH_REGION:", region)

    if not api_key:
        raise RuntimeError("YANDEX_SEARCH_API_KEY is empty")
    if not folder_id:
        raise RuntimeError("YANDEX_SEARCH_FOLDER_ID is empty")

    keywords = read_keywords()
    report_date = date.today().isoformat()

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("PGDATABASE", "traffic_analytics"),
        user=os.getenv("PGUSER", "traffic_admin"),
        password=os.getenv("PGPASSWORD"),
    )
    cur = conn.cursor()

    cur.execute("delete from stg_competitor_serp_daily where report_date = %s", (report_date,))
    conn.commit()

    inserted = 0

    for kw in keywords:
        body = {
            "query": {
                "searchType": "SEARCH_TYPE_RU",
                "queryText": kw,
                "page": "0"
            },
            "region": region,
            "l10n": "LOCALIZATION_RU",
            "folderId": folder_id
        }

        r = requests.post(
            "https://searchapi.api.cloud.yandex.net/v2/web/search",
            headers={
                "Authorization": f"Api-Key {api_key}",
                "Content-Type": "application/json"
            },
            json=body,
            timeout=60,
        )

        print("keyword:", kw, "status:", r.status_code)
        print("response preview:", r.text[:500])

        r.raise_for_status()
        data = r.json()
        raw_xml = data.get("rawData", "")

        if not raw_xml:
            print("No rawData for keyword:", kw)
            continue

        root = etree.fromstring(raw_xml.encode("utf-8") if isinstance(raw_xml, str) else raw_xml)

        groups = root.xpath("//group")
        print("groups found:", len(groups), "for keyword:", kw)

        for pos, g in enumerate(groups, start=1):
            url = "".join(g.xpath(".//url/text()")) or "".join(g.xpath(".//doc/url/text()"))
            domain = "".join(g.xpath(".//domain/text()")) or urlparse(url).netloc.replace("www.", "")
            title = "".join(g.xpath(".//title/text()")) or "".join(g.xpath(".//doc/title/text()"))
            headline = "".join(g.xpath(".//headline/text()"))
            passage = " ".join(g.xpath(".//passage/text()"))

            if not url:
                continue

            cur.execute("""
                insert into stg_competitor_serp_daily (
                    report_date, keyword, result_type, position,
                    domain, url, title, headline, passage, raw_xml
                )
                values (%s,%s,'organic',%s,%s,%s,%s,%s,%s,%s)
            """, (
                report_date, kw, pos, domain, url, title, headline, passage, raw_xml[:20000]
            ))
            inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"yandex serp rows loaded: {inserted}")

if __name__ == "__main__":
    main()
