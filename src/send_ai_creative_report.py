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
    password=os.getenv("PGPASSWORD")
)

cur = conn.cursor()
cur.execute("""
select
    campaign_name,
    ad_id,
    ad_group_id,
    original_title,
    original_title_2,
    original_body_text,
    sample_queries,
    score,
    round(coalesce(ctr,0) * 100, 2) as ctr_pct,
    cpc,
    ai_title_1,
    ai_title_2,
    ai_body_1,
    ai_title_1_b,
    ai_title_2_b,
    ai_body_2,
    ai_title_1_c,
    ai_title_2_c,
    ai_body_3,
    predicted_ctr_pct,
    predicted_cpc,
    predicted_relevance,
    prediction_confidence,
    prediction_reason
from mart_ai_creative_candidates
where decision = 'PENDING'
order by created_at desc
limit 5
""")
rows = cur.fetchall()
cur.close()
conn.close()

text = "🤖 AI CREATIVE ENGINE\n\n"

if not rows:
    text += "Сегодня новых кандидатов на переписывание объявлений нет."
else:
    for row in rows:
        (
            campaign, ad_id, ad_group_id,
            title, title2, body_text, sample_queries,
            score, ctr_pct, cpc,
            a1, a2, b1,
            a1b, a2b, b2,
            a1c, a2c, b3,
            pred_ctr, pred_cpc, pred_rel, pred_conf, pred_reason
        ) = row

        text += (
            f"{campaign}\n"
            f"Ad ID: {ad_id}\n"
            f"Ad Group ID: {ad_group_id}\n"
            f"Score: {score}\n"
            f"CTR сейчас: {ctr_pct}%\n"
            f"CPC сейчас: {cpc} ₽\n\n"
            f"Текущее объявление:\n"
            f"TITLE1: {title}\n"
            f"TITLE2: {title2}\n"
            f"BODY: {body_text}\n\n"
            f"Ключевые слова / условия показа:\n{sample_queries}\n\n"
            f"📈 Прогноз по новому объявлению\n"
            f"CTR: {pred_ctr}%\n"
            f"CPC: {pred_cpc} ₽\n"
            f"Relevance: {pred_rel}\n"
            f"Confidence: {pred_conf}\n"
            f"{pred_reason}\n\n"
            f"Вариант A:\n"
            f"TITLE1: {a1}\n"
            f"TITLE2: {a2}\n"
            f"BODY: {b1}\n\n"
            f"Вариант B:\n"
            f"TITLE1: {a1b}\n"
            f"TITLE2: {a2b}\n"
            f"BODY: {b2}\n\n"
            f"Вариант C:\n"
            f"TITLE1: {a1c}\n"
            f"TITLE2: {a2c}\n"
            f"BODY: {b3}\n\n"
            f"Чтобы поставить в очередь A/B тест:\n"
            f"python ~/traffic-analytics/src/queue_ai_ab_test.py {ad_id} A\n"
            f"python ~/traffic-analytics/src/queue_ai_ab_test.py {ad_id} B\n"
            f"python ~/traffic-analytics/src/queue_ai_ab_test.py {ad_id} C\n\n"
            f"---\n\n"
        )

url = f"https://api.telegram.org/bot{os.getenv('TG_TOKEN')}/sendMessage"
resp = requests.post(
    url,
    json={"chat_id": os.getenv("TG_CHAT"), "text": text[:4000]},
    timeout=30
)
resp.raise_for_status()
print("AI creative report sent")
