import requests
from src.db import db_cursor
from src.settings import Settings


METRICA_URL = "https://api-metrika.yandex.net/stat/v1/data"


def fetch_metrica_source_daily(report_date: str):
    params = {
        "ids": Settings.METRICA_COUNTER_ID,
        "date1": report_date,
        "date2": report_date,
        "metrics": "ym:s:visits,ym:s:users",
        "dimensions": "ym:s:lastTrafficSource,ym:s:lastAdvEngine,ym:s:lastUTMCampaign",
        "accuracy": "full",
        "limit": 100000,
    }

    headers = {
        "Authorization": f"OAuth {Settings.METRICA_TOKEN}",
    }

    response = requests.get(METRICA_URL, params=params, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def safe_dim(dims, idx):
    if len(dims) > idx and dims[idx] and dims[idx].get("name") is not None:
        return dims[idx]["name"]
    return ""


def save_metrica_source(data: dict, report_date: str):
    rows = data.get("data", [])
    inserted = 0

    with db_cursor() as (_, cur):
        for item in rows:
            dims = item.get("dimensions", [])
            mets = item.get("metrics", [0, 0])

            traffic_source = safe_dim(dims, 0)
            source_engine = safe_dim(dims, 1)
            campaign_name = safe_dim(dims, 2)

            sessions = int(mets[0]) if len(mets) > 0 else 0
            users = int(mets[1]) if len(mets) > 1 else 0

            cur.execute(
                """
                insert into stg_metrica_source_daily
                (
                    date,
                    counter_id,
                    traffic_source,
                    source_engine,
                    source_medium,
                    campaign_name,
                    sessions,
                    users,
                    loaded_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict (
                    date, counter_id, traffic_source, source_engine, source_medium, campaign_name
                )
                do update set
                    sessions = excluded.sessions,
                    users = excluded.users,
                    loaded_at = now()
                """,
                (
                    report_date,
                    int(Settings.METRICA_COUNTER_ID),
                    traffic_source,
                    source_engine,
                    "",
                    campaign_name,
                    sessions,
                    users,
                ),
            )
            inserted += 1

    return inserted


def run(report_date: str):
    data = fetch_metrica_source_daily(report_date)
    inserted = save_metrica_source(data, report_date)
    return {"status": "ok", "rows": inserted}


if __name__ == "__main__":
    from datetime import date, timedelta
    report_date = (date.today() - timedelta(days=1)).isoformat()
    print(run(report_date))
