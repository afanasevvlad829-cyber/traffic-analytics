import os
ENV_PATH = "/home/kv145/traffic-analytics/.env"
import psycopg2
import requests

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

print("TG_TOKEN present:", bool(os.getenv("TG_TOKEN")))
print("TG_CHAT present:", bool(os.getenv("TG_CHAT")))

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    dbname=os.getenv("PGDATABASE", "traffic_analytics"),
    user=os.getenv("PGUSER", "traffic_admin"),
    password=os.getenv("PGPASSWORD"),
)
cur = conn.cursor()

cur.execute("""
select
    keyword,
    position,
    domain,
    title,
    relevance_score,
    verdict,
    action_note
from mart_competitor_serp_alerts
where report_date = current_date
  and verdict in ('STRONG_ORGANIC_RESULT','MID_ORGANIC_RESULT')
order by relevance_score desc, position asc
limit 10
""")
rows = cur.fetchall()
cur.close()
conn.close()

text = "🕵️ YANDEX COMPETITOR WATCH\n\n"

if not rows:
    text += "Сегодня сильных конкурентных сигналов нет."
else:
    for kw, pos, domain, title, score, verdict, note in rows:
        text += (
            f"Запрос: {kw}\n"
            f"Позиция: {pos}\n"
            f"Домен: {domain}\n"
            f"Релевантность: {round(score,2)}\n"
            f"Тайтл: {title}\n"
            f"Действие: {note}\n\n---\n\n"
        )

token = os.getenv("TG_TOKEN")
chat = os.getenv("TG_CHAT")

if not token or not chat:
    raise RuntimeError("TG_TOKEN or TG_CHAT is empty")

url = f"https://api.telegram.org/bot{token}/sendMessage"
resp = requests.post(url, json={"chat_id": chat, "text": text[:4000]}, timeout=30)
print("telegram status:", resp.status_code)
print("telegram response:", resp.text[:500])
resp.raise_for_status()
print("yandex competitor report sent")
