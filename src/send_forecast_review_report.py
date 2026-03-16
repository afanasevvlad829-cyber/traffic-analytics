import os
import psycopg2
import requests

ENV_PATH = "/home/kv145/traffic-analytics/.env"

def load_env():
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

load_env()

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    dbname=os.getenv("PGDATABASE", "traffic_analytics"),
    user=os.getenv("PGUSER", "traffic_admin"),
    password=os.getenv("PGPASSWORD"),
)
cur = conn.cursor()

cur.execute("""
select
    campaign_name,
    ad_group_id,
    ad_id,
    predicted_ctr_pct,
    predicted_cpc,
    predicted_relevance,
    actual_ctr_pct,
    actual_cpc,
    actual_relevance,
    forecast_status,
    comment
from mart_ai_creative_forecast_review
where review_date = current_date
order by created_at desc
limit 10
""")
rows = cur.fetchall()
cur.close()
conn.close()

text = "📈 FORECAST REVIEW\n\n"

if not rows:
    text += "Сегодня сравнений прогноза с фактом нет."
else:
    for row in rows:
        campaign, ad_group_id, ad_id, pctr, pcpc, prel, actr, acpc, arel, status, comment = row
        text += (
            f"{campaign}\n"
            f"Ad ID: {ad_id}\n"
            f"Ad Group ID: {ad_group_id}\n"
            f"Pred CTR: {pctr}\n"
            f"Pred CPC: {pcpc}\n"
            f"Pred Relevance: {prel}\n"
            f"Actual CTR: {actr}\n"
            f"Actual CPC: {acpc}\n"
            f"Actual Relevance: {arel}\n"
            f"Status: {status}\n"
            f"{comment}\n\n---\n\n"
        )

url = f"https://api.telegram.org/bot{os.getenv('TG_TOKEN')}/sendMessage"
resp = requests.post(
    url,
    json={"chat_id": os.getenv("TG_CHAT"), "text": text[:4000]},
    timeout=30
)
resp.raise_for_status()
print("forecast review report sent")
