import os
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

load_env("/home/kv145/traffic-analytics/.env_telegram")
load_env("/home/kv145/traffic-analytics/.env_ai")

TELEGRAM_TOKEN = os.getenv("TG_TOKEN")
TELEGRAM_CHAT = os.getenv("TG_CHAT")

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    dbname=os.getenv("PGDATABASE", "traffic_analytics"),
    user=os.getenv("PGUSER", "traffic_admin"),
    password=os.getenv("PGPASSWORD"),
)

cur = conn.cursor()
cur.execute("""
select
    task_id,
    campaign_name,
    search_query,
    decision,
    impressions,
    clicks,
    cost,
    ctr_pct,
    avg_cpc,
    account_avg_ctr_pct,
    forecast_target_ctr_pct,
    forecast_target_cpc,
    forecast_cost_effect,
    exact_action
from vw_direct_task_queue
where status = 'OPEN'
order by
    case
      when decision = 'EXCLUDE' then 1
      when decision = 'REWRITE_AD' then 2
      when decision = 'LOWER_BID_OR_SPLIT' then 3
      when decision = 'LANDING_CHECK' then 4
      when decision = 'SCALE' then 5
      else 9
    end,
    cost desc
limit 12
""")
rows = cur.fetchall()

groups = {
    "EXCLUDE": [],
    "REWRITE_AD": [],
    "LOWER_BID_OR_SPLIT": [],
    "LANDING_CHECK": [],
    "SCALE": [],
}

for r in rows:
    task_id, campaign, query, decision, impr, clicks, cost, ctr_pct, cpc, acc_ctr, target_ctr, target_cpc, effect, action = r

    forecast_bits = []
    if target_ctr is not None:
        forecast_bits.append(f"Прогноз CTR: {target_ctr}%")
    if target_cpc is not None:
        forecast_bits.append(f"Прогноз CPC: {target_cpc} ₽")
    if effect is not None:
        forecast_bits.append(f"Эффект: {effect} ₽")

    forecast_text = " | ".join(forecast_bits) if forecast_bits else "Прогноз: убрать лишний трафик"

    line = (
        f"#{task_id}\n"
        f"{campaign}\n"
        f"{query}\n"
        f"Показы: {impr} | Клики: {clicks} | Cost: {round(cost or 0,2)} ₽\n"
        f"CTR: {round(ctr_pct or 0,2)}% | Ср. CTR аккаунта: {round(acc_ctr or 0,2)}%\n"
        f"CPC: {round(cpc or 0,2)} ₽\n"
        f"{forecast_text}\n"
        f"Действие: {action}\n"
        f"Закрыть: /done {task_id}"
    )
    groups.setdefault(decision, []).append(line)

parts = ["📊 DIRECTOLOGIST TASKS\n"]

labels = {
    "EXCLUDE": "🚫 УБРАТЬ / МИНУС-СЛОВА",
    "REWRITE_AD": "✍️ ПЕРЕПИСАТЬ ОБЪЯВЛЕНИЕ",
    "LOWER_BID_OR_SPLIT": "💸 СНИЗИТЬ СТАВКУ / РАЗДЕЛИТЬ",
    "LANDING_CHECK": "🧪 ПРОВЕРИТЬ ПОСАДОЧНУЮ",
    "SCALE": "🚀 МАСШТАБИРОВАТЬ",
}

for key in ["EXCLUDE","REWRITE_AD","LOWER_BID_OR_SPLIT","LANDING_CHECK","SCALE"]:
    items = groups.get(key) or []
    if not items:
        continue
    parts.append(labels[key])
    parts.append("\n\n---\n\n".join(items[:3]))
    parts.append("")

text = "\n".join(parts)
if len(text.strip()) <= len("📊 DIRECTOLOGIST TASKS"):
    text += "\nСегодня открытых задач нет."

url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
resp = requests.post(
    url,
    json={"chat_id": TELEGRAM_CHAT, "text": text[:4000]},
    timeout=30,
)
resp.raise_for_status()
print("Task report sent")
