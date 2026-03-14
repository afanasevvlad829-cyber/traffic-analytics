import requests
from datetime import date, timedelta
from src.db import db_cursor
from src.settings import Settings

DIRECT_URL = "https://api.direct.yandex.com/json/v5/reports"


def fetch_direct_campaign_daily(report_date: str):
    headers = {
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
                "Clicks",
                "Impressions",
                "Cost"
            ],
            "ReportName": f"campaign_report_{report_date}",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "YES"
        }
    }

    r = requests.post(DIRECT_URL, headers=headers, json=body, timeout=120)
    r.raise_for_status()
    return r.text


def save_direct_report(report_text: str):
    lines = [x for x in report_text.strip().split("\n") if x.strip()]
    if len(lines) < 2:
        return 0

    headers = lines[0].split("\t")
    inserted = 0

    with db_cursor() as (_, cur):
        for line in lines[1:]:
            values = line.split("\t")
            row = dict(zip(headers, values))

            cur.execute(
                """
                insert into stg_direct_campaign_daily
                (
                    date,
                    campaign_id,
                    campaign_name,
                    impressions,
                    clicks,
                    cost,
                    loaded_at
                )
                values (%s,%s,%s,%s,%s,%s,now())
                on conflict (date, campaign_id)
                do update set
                    campaign_name = excluded.campaign_name,
                    impressions = excluded.impressions,
                    clicks = excluded.clicks,
                    cost = excluded.cost,
                    loaded_at = now()
                """,
                (
                    row["Date"],
                    int(row["CampaignId"]),
                    row["CampaignName"],
                    int(row["Impressions"]),
                    int(row["Clicks"]),
                    float(row["Cost"]),
                )
            )
            inserted += 1

    return inserted


def run(report_date=None):
    if report_date is None:
        report_date = (date.today() - timedelta(days=1)).isoformat()

    report = fetch_direct_campaign_daily(report_date)
    rows = save_direct_report(report)
    return {"status": "ok", "rows_loaded": rows, "date": report_date}


if __name__ == "__main__":
    print(run())
