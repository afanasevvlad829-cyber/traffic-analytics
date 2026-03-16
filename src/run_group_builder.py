import os
import psycopg2
from openai import OpenAI

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

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url=os.getenv("OPENAI_BASE_URL"),
)

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    dbname=os.getenv("PGDATABASE", "traffic_analytics"),
    user=os.getenv("PGUSER", "traffic_admin"),
    password=os.getenv("PGPASSWORD"),
)
cur = conn.cursor()

cur.execute("truncate table mart_group_builder")

cur.execute("""
select
    q.campaign_name,
    q.ad_group_id,
    string_agg(distinct q.search_query, ' | ' order by q.search_query) as queries
from mart_direct_action_queue_v3 q
group by q.campaign_name, q.ad_group_id
having count(*) >= 3
order by q.campaign_name, q.ad_group_id
limit 20
""")
rows = cur.fetchall()

inserted = 0

for campaign, group_id, queries in rows:
    prompt = f"""
Ты senior директолог по Яндекс Директ.

Нужно провести аудит структуры рекламной группы и предложить более правильную разбивку.

Кампания: {campaign}
Ad Group ID: {group_id}

Поисковые запросы:
{queries}

Сделай ответ строго в формате:

SUMMARY:
кратко, что не так с группой

GROUP_1:
название группы
WHY_1:
почему именно такие запросы надо объединить
QUERIES_1:
список запросов

GROUP_2:
название группы
WHY_2:
почему именно такие запросы надо объединить
QUERIES_2:
список запросов

EXPECTED_EFFECT:
какой эффект даст перестроение группы

RISK:
какой риск есть при перестроении

SPLIT_PRIORITY:
HIGH / MEDIUM / LOW
"""

    resp = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL"),
        messages=[
            {"role": "system", "content": "Ты сильный performance-маркетолог и директолог."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )

    result = resp.choices[0].message.content or ""

    cur.execute("""
        insert into mart_group_builder(
            campaign_name,
            ad_group_id,
            queries,
            recommendation
        )
        values (%s,%s,%s,%s)
    """, (campaign, group_id, queries, result))
    inserted += 1

conn.commit()
cur.close()
conn.close()

print(f"group builder done: {inserted}")
