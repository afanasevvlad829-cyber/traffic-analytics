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
    ad_group_id,
    ad_title,
    creative_score,
    creative_grade,
    recommended_action,
    ctr_pct,
    account_avg_ctr_pct,
    avg_cpc,
    account_avg_cpc,
    relevance_score,
    traffic_quality_score,
    cost,
    ad_type,
    thumbnail_url
from vw_creative_score_report
order by creative_score asc, cost desc
limit 8
""")
low_rows = cur.fetchall()

cur.execute("""
select
    campaign_name,
    ad_group_id,
    ad_title,
    creative_score,
    creative_grade,
    recommended_action,
    ctr_pct,
    account_avg_ctr_pct,
    avg_cpc,
    account_avg_cpc,
    relevance_score,
    traffic_quality_score,
    cost,
    ad_type,
    thumbnail_url
from vw_creative_score_report
where creative_grade in ('STRONG','NORMAL')
order by creative_score desc, cost desc
limit 5
""")
top_rows = cur.fetchall()

cur.close()
conn.close()

text = "🎯 CREATIVE SCORE REPORT\n\n"

if low_rows:
    text += "🔻 СЛАБЫЕ КРЕАТИВЫ\n\n"
    for row in low_rows:
        campaign, ad_group_id, ad_title, score, grade, action, ctr_pct, acc_ctr, avg_cpc, acc_cpc, rel_score, tq_score, cost, ad_type, thumbnail_url = row
        text += (
            f"{campaign}\n"
            f"ID: {ad_group_id}\n"
            f"Заголовок: {ad_title}\n"
            f"Тип: {ad_type}\n"
            f"Score: {score} | Grade: {grade} | Action: {action}\n"
            f"CTR: {ctr_pct}% vs account {acc_ctr}%\n"
            f"CPC: {avg_cpc} ₽ vs account {acc_cpc} ₽\n"
            f"Relevance: {rel_score} | Traffic Quality: {tq_score}\n"
            f"Cost: {round(cost or 0,2)} ₽\n"
        )
        if thumbnail_url:
            text += f"Preview: {thumbnail_url}\n"
        text += "\n---\n\n"

if top_rows:
    text += "🟢 СИЛЬНЫЕ КРЕАТИВЫ\n\n"
    for row in top_rows:
        campaign, ad_group_id, ad_title, score, grade, action, ctr_pct, acc_ctr, avg_cpc, acc_cpc, rel_score, tq_score, cost, ad_type, thumbnail_url = row
        text += (
            f"{campaign}\n"
            f"ID: {ad_group_id}\n"
            f"Заголовок: {ad_title}\n"
            f"Тип: {ad_type}\n"
            f"Score: {score} | Grade: {grade} | Action: {action}\n"
            f"CTR: {ctr_pct}% vs account {acc_ctr}%\n"
            f"CPC: {avg_cpc} ₽ vs account {acc_cpc} ₽\n"
        )
        if thumbnail_url:
            text += f"Preview: {thumbnail_url}\n"
        text += "\n---\n\n"

url = f"https://api.telegram.org/bot{os.getenv('TG_TOKEN')}/sendMessage"
resp = requests.post(
    url,
    json={"chat_id": os.getenv("TG_CHAT"), "text": text[:4000]},
    timeout=30
)
resp.raise_for_status()
print("Creative score report sent")
