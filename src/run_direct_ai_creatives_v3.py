import os
ENV_PATH = "/home/kv145/traffic-analytics/.env"
import re
import psycopg2
from openai import OpenAI

ENV_FILES = [
    "/home/kv145/traffic-analytics/.env_ai",
]

def load_env():
    for path in ENV_FILES:
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip().strip('"').strip("'")

def extract(label: str, text: str) -> str:
    m = re.search(rf"{label}:\s*(.*)", text, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""

def main():
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

    cur.execute("""
        select
            task_id,
            date,
            campaign_name,
            search_query,
            decision,
            impressions,
            clicks,
            cost,
            ctr,
            avg_cpc,
            account_avg_ctr,
            account_p75_cpc,
            exact_action
        from mart_direct_task_queue
        where decision in ('REWRITE_AD','EXCLUDE','LOWER_BID_OR_SPLIT','LANDING_CHECK')
          and status = 'OPEN'
        order by priority_score desc, cost desc
        limit 12
    """)
    rows = cur.fetchall()

    generated = 0

    for row in rows:
        task_id, report_date, campaign, query, decision, impressions, clicks, cost, ctr, cpc, acc_ctr, acc_p75_cpc, exact_action = row

        prompt = f"""
Ты сильный performance-маркетолог и директолог.
Ниша: детский лагерь / IT-лагерь.

Данные:
- Кампания: {campaign}
- Запрос: {query}
- Решение: {decision}
- Показы: {impressions}
- Клики: {clicks}
- CTR: {round((ctr or 0) * 100, 2)}%
- Средний CTR аккаунта: {round((acc_ctr or 0) * 100, 2)}%
- CPC: {round(cpc or 0, 2)} ₽
- p75 CPC аккаунта: {round(acc_p75_cpc or 0, 2)} ₽
- Рекомендация системы: {exact_action}

Нужно:
1) Для REWRITE_AD / LOWER_BID_OR_SPLIT / LANDING_CHECK:
   дать 3 варианта объявления Яндекс Директ.
2) Для EXCLUDE:
   дать минус-слова и короткий комментарий, почему это мусорный интент.
3) Пиши по-русски, коротко, конкретно, коммерчески.

Формат строго такой:

TITLE1: ...
TITLE2: ...
TEXT: ...

TITLE1_B: ...
TITLE2_B: ...
TEXT_B: ...

TITLE1_C: ...
TITLE2_C: ...
TEXT_C: ...

MINUS_WORDS: слово1, слово2, слово3

RATIONALE: ...
"""

        resp = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "openai/gpt-4o-mini"),
            messages=[
                {
                    "role": "system",
                    "content": "Ты senior директолог. Даёшь короткие, боевые, коммерческие тексты объявлений и минус-слова."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.7,
        )

        text = resp.choices[0].message.content or ""

        cur.execute("""
            insert into mart_direct_ai_creatives (
                date,
                campaign_name,
                search_query,
                decision,
                impressions,
                clicks,
                cost,
                ctr,
                avg_cpc,
                ai_title_1,
                ai_title_2,
                ai_text,
                ai_title_1_b,
                ai_title_2_b,
                ai_text_b,
                ai_title_1_c,
                ai_title_2_c,
                ai_text_c,
                minus_words,
                raw_llm_response
            )
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            report_date,
            campaign,
            query,
            decision,
            impressions,
            clicks,
            cost,
            ctr,
            cpc,
            extract("TITLE1", text),
            extract("TITLE2", text),
            extract("TEXT", text),
            extract("TITLE1_B", text),
            extract("TITLE2_B", text),
            extract("TEXT_B", text),
            extract("TITLE1_C", text),
            extract("TITLE2_C", text),
            extract("TEXT_C", text),
            extract("MINUS_WORDS", text),
            text
        ))
        generated += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"AI creatives generated: {generated}")

if __name__ == "__main__":
    main()
