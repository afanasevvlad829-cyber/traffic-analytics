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
select campaign_name, safe_negative_keywords_copy_paste, keywords_count
from vw_campaign_negative_keywords_ai_safe_copy_paste
order by campaign_name
""")
safe_rows = cur.fetchall()

cur.execute("""
select campaign_name, blocked_negative_keywords_copy_paste, keywords_count
from vw_campaign_negative_keywords_ai_blocked_copy_paste
order by campaign_name
""")
blocked_rows = cur.fetchall()

cur.execute("""
select
    campaign_name,
    word,
    avg_ctr_pct,
    account_avg_ctr_pct,
    ctr_lift_pct,
    verdict
from vw_headline_word_lift_report
where verdict = 'POSITIVE_LIFT'
order by ctr_lift_pct desc
limit 12
""")
positive_words = cur.fetchall()

cur.execute("""
select
    campaign_name,
    word,
    avg_ctr_pct,
    account_avg_ctr_pct,
    ctr_lift_pct,
    verdict
from vw_headline_word_lift_report
where verdict = 'NEGATIVE_LIFT'
order by ctr_lift_pct asc
limit 12
""")
negative_words = cur.fetchall()

cur.close()
conn.close()

text = "🧠 NEGATIVE + HEADLINE LEARNING REPORT\n\n"

if safe_rows:
    text += "✅ SAFE_NEGATIVES (AI VERIFIED)\n\n"
    for campaign, words, cnt in safe_rows:
        text += f"{campaign}\nКол-во: {cnt}\n{words}\n\n---\n\n"

if blocked_rows:
    text += "🛑 BLOCKED_NEGATIVES (НЕ МИНУСОВАТЬ)\n\n"
    for campaign, words, cnt in blocked_rows:
        text += f"{campaign}\nКол-во: {cnt}\n{words}\n\n---\n\n"

if positive_words:
    text += "📈 СЛОВА, КОТОРЫЕ ПОДНИМАЮТ CTR\n\n"
    for campaign, word, avg_ctr_pct, account_avg_ctr_pct, ctr_lift_pct, verdict in positive_words:
        text += (
            f"{campaign}\n"
            f"Слово: {word}\n"
            f"CTR: {avg_ctr_pct}% vs аккаунт {account_avg_ctr_pct}%\n"
            f"Lift: +{ctr_lift_pct}%\n\n---\n\n"
        )

if negative_words:
    text += "📉 СЛОВА, КОТОРЫЕ ТЯНУТ CTR ВНИЗ\n\n"
    for campaign, word, avg_ctr_pct, account_avg_ctr_pct, ctr_lift_pct, verdict in negative_words:
        text += (
            f"{campaign}\n"
            f"Слово: {word}\n"
            f"CTR: {avg_ctr_pct}% vs аккаунт {account_avg_ctr_pct}%\n"
            f"Lift: {ctr_lift_pct}%\n\n---\n\n"
        )

if text.strip() == "🧠 NEGATIVE + HEADLINE LEARNING REPORT":
    text += "Сегодня данных недостаточно."

url = f"https://api.telegram.org/bot{os.getenv('TG_TOKEN')}/sendMessage"
resp = requests.post(
    url,
    json={"chat_id": os.getenv("TG_CHAT"), "text": text[:4000]},
    timeout=30
)
resp.raise_for_status()
print("Negative + word lift report sent")
