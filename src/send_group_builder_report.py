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
    recommendation
from mart_group_builder
order by created_at desc
limit 5
""")
rows = cur.fetchall()
cur.close()
conn.close()

text = "🧠 AI GROUP BUILDER\n\n"

if not rows:
    text += "Сегодня рекомендаций по перестроению групп нет."
else:
    for campaign, group_id, recommendation in rows:
        text += (
            f"{campaign}\n"
            f"Группа: {group_id}\n\n"
            f"{recommendation}\n\n"
            f"---\n\n"
        )

url = f"https://api.telegram.org/bot{os.getenv('TG_TOKEN')}/sendMessage"
resp = requests.post(
    url,
    json={"chat_id": os.getenv("TG_CHAT"), "text": text[:4000]},
    timeout=30
)
resp.raise_for_status()
print("group builder report sent")
