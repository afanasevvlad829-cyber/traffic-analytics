import csv
import io
import json
import time
import requests
from datetime import date, timedelta
from src.db import db_cursor
from src.settings import Settings

REPORTS_URL = "https://api.direct.yandex.com/json/v5/reports"
ADS_URL = "https://api.direct.yandex.com/json/v5/ads"


def _direct_headers_reports():
    return {
        "Authorization": f"Bearer {Settings.DIRECT_TOKEN}",
        "Client-Login": Settings.DIRECT_CLIENT_LOGIN,
        "Accept-Language": "ru",
        "processingMode": "auto",
        "returnMoneyInMicros": "false",
        "skipReportHeader": "true",
        "skipReportSummary": "true",
        "skipColumnHeader": "false",
        "Content-Type": "application/json; charset=utf-8",
    }


def _direct_headers_json():
    return {
        "Authorization": f"Bearer {Settings.DIRECT_TOKEN}",
        "Client-Login": Settings.DIRECT_CLIENT_LOGIN,
        "Accept-Language": "ru",
        "Content-Type": "application/json; charset=utf-8",
    }


def fetch_search_query_report(report_date: str) -> str:
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": report_date,
                "DateTo": report_date
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "AdGroupId",
                "Query",
                "Impressions",
                "Clicks",
                "Cost"
            ],
            "ReportName": f"search_query_report_{report_date}",
            "ReportType": "SEARCH_QUERY_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "YES"
        }
    }

    for _ in range(10):
        r = requests.post(REPORTS_URL, headers=_direct_headers_reports(), json=body, timeout=180)

        if r.status_code == 200:
            return r.text

        if r.status_code in (201, 202):
            retry_in = int(r.headers.get("retryIn", "5"))
            time.sleep(retry_in)
            continue

        raise RuntimeError(f"Direct report error {r.status_code}: {r.text}")

    raise RuntimeError("Direct report was not ready after multiple retries")


def save_search_query_report(report_text: str, report_date: str) -> int:
    lines = [x for x in report_text.strip().split("\n") if x.strip()]
    if len(lines) < 2:
        return 0

    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter="\t")
    inserted = 0

    with db_cursor() as (_, cur):
        cur.execute("delete from stg_direct_search_queries where date = %s", (report_date,))

        for row in reader:
            cur.execute("""
                insert into stg_direct_search_queries
                (date, campaign_id, campaign_name, ad_group_id, search_query, impressions, clicks, cost, loaded_at)
                values (%s,%s,%s,%s,%s,%s,%s,%s,now())
            """, (
                row["Date"],
                int(row["CampaignId"]),
                row["CampaignName"],
                int(row["AdGroupId"]),
                row["Query"],
                int(row["Impressions"]),
                int(row["Clicks"]),
                float(row["Cost"]),
            ))
            inserted += 1

    return inserted


def fetch_ads_meta():
    body = {
        "method": "get",
        "params": {
            "SelectionCriteria": {},
            "FieldNames": ["Id", "CampaignId", "AdGroupId"],
            "TextAdFieldNames": ["Title", "Title2", "Text", "Href"]
        }
    }

    r = requests.post(ADS_URL, headers=_direct_headers_json(), data=json.dumps(body), timeout=180)
    r.raise_for_status()
    data = r.json()

    if "error" in data:
        return []

    return data.get("result", {}).get("Ads", [])


def save_ads_meta(ads) -> int:
    inserted = 0

    with db_cursor() as (_, cur):
        cur.execute("truncate table stg_direct_ads_meta")

        for ad in ads:
            text_ad = ad.get("TextAd", {}) or {}
            cur.execute("""
                insert into stg_direct_ads_meta
                (ad_id, ad_group_id, campaign_id, title, title2, body_text, href, loaded_at)
                values (%s,%s,%s,%s,%s,%s,%s,now())
            """, (
                ad.get("Id"),
                ad.get("AdGroupId"),
                ad.get("CampaignId"),
                text_ad.get("Title"),
                text_ad.get("Title2"),
                text_ad.get("Text"),
                text_ad.get("Href"),
            ))
            inserted += 1

    return inserted


def run(report_date=None):
    if report_date is None:
        report_date = (date.today() - timedelta(days=1)).isoformat()

    report_text = fetch_search_query_report(report_date)
    search_rows = save_search_query_report(report_text, report_date)

    ads_rows = 0
    try:
        ads = fetch_ads_meta()
        ads_rows = save_ads_meta(ads)
    except Exception:
        ads_rows = 0

    return {
        "status": "ok",
        "date": report_date,
        "search_rows_loaded": search_rows,
        "ads_rows_loaded": ads_rows
    }


if __name__ == "__main__":
    print(run())
