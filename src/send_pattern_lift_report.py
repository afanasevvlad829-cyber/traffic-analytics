import os
import psycopg2
import requests

ENV_PATH = "/home/kv145/traffic-analytics/.env"

def load_env(path: str):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

load_env(ENV_PATH)

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
    pattern,
    pattern_type,
    avg_ctr_pct,
    account_avg_ctr_pct,
    ctr_lift_pct
from vw_headline_pattern_lift_report
where verdict = 'POSITIVE_LIFT'
order by ctr_lift_pct desc, total_impressions desc
limit 12
""")
positive = cur.fetchall()

cur.execute("""
select
    campaign_name,
    pattern,
    pattern_type,
    avg_ctr_pct,
    account_avg_ctr_pct,
    ctr_lift_pct
from vw_headline_pattern_lift_report
where verdict = 'NEGATIVE_LIFT'
order by ctr_lift_pct asc, total_impressions desc
limit 12
""")
negative = cur.fetchall()

cur.close()
conn.close()

text = "🧩 HEADLINE PATTERN LIFT\n\n"

if positive:
    text += "📈 ФРАЗЫ, КОТОРЫЕ ПОДНИМАЮТ CTR\n\n"
    for campaign, pattern, pattern_type, avg_ctr_pct, account_avg_ctr_pct, ctr_lift_pct in positive:
        text += (
            f"{campaign}\n"
            f"{pattern_type}: {pattern}\n"
            f"CTR: {avg_ctr_pct}% vs аккаунт {account_avg_ctr_pct}%\n"
            f"Lift: +{ctr_lift_pct}%\n\n---\n\n"
        )

if negative:
    text += "📉 ФРАЗЫ, КОТОРЫЕ ТЯНУТ CTR ВНИЗ\n\n"
    for campaign, pattern, pattern_type, avg_ctr_pct, account_avg_ctr_pct, ctr_lift_pct in negative:
        text += (
            f"{campaign}\n"
            f"{pattern_type}: {pattern}\n"
            f"CTR: {avg_ctr_pct}% vs аккаунт {account_avg_ctr_pct}%\n"
            f"Lift: {ctr_lift_pct}%\n\n---\n\n"
        )

if text.strip() == "🧩 HEADLINE PATTERN LIFT":
    text += "Пока недостаточно данных для устойчивых выводов по фразам."

url = f"https://api.telegram.org/bot{os.getenv('TG_TOKEN')}/sendMessage"
resp = requests.post(
    url,
    json={"chat_id": os.getenv("TG_CHAT"), "text": text[:4000]},
    timeout=30
)
resp.raise_for_status()
print("Pattern lift report sent")
