import os
import re
import psycopg2
from openai import OpenAI

def load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ[key.strip()] = value.strip()

def parse_llm_response(text: str) -> dict:
    def extract(label: str) -> str:
        pattern = rf"{label}:\s*(.*)"
        m = re.search(pattern, text, flags=re.IGNORECASE)
        return m.group(1).strip() if m else ""

    return {
        "ai_title_1": extract("TITLE1"),
        "ai_title_2": extract("TITLE2"),
        "ai_text": extract("TEXT"),
        "ai_title_1_b": extract("TITLE1_B"),
        "ai_title_2_b": extract("TITLE2_B"),
        "ai_text_b": extract("TEXT_B"),
        "ai_title_1_c": extract("TITLE1_C"),
        "ai_title_2_c": extract("TITLE2_C"),
        "ai_text_c": extract("TEXT_C"),
        "minus_words": extract("MINUS_WORDS"),
    }

def main() -> None:
    load_env_file("/home/kv145/traffic-analytics/.env_ai")

    api_key = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("OPENAI_BASE_URL")
    model = os.getenv("OPENAI_MODEL", "openai/gpt-4o-mini")

    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is empty")
    if not base_url:
        raise RuntimeError("OPENAI_BASE_URL is empty")

    client = OpenAI(
        api_key=api_key,
        base_url=base_url,
    )

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("PGDATABASE", "traffic_analytics"),
        user=os.getenv("PGUSER", "traffic_admin"),
        password=os.getenv("PGPASSWORD"),
    )
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
        select
            date,
            campaign_name,
            search_query,
            decision,
            impressions,
            clicks,
            cost,
            ctr,
            avg_cpc
        from mart_direct_action_queue_v3
        where decision in ('REWRITE_AD','EXCLUDE','LOWER_BID_OR_SPLIT','LANDING_CHECK')
        order by priority_score desc, cost desc
        limit 10
    """)
    rows = cur.fetchall()

    generated = 0

    for row in rows:
        report_date, campaign, query, decision, impressions, clicks, cost, ctr, cpc = row

        prompt = f"""
Ты сильный специалист по Яндекс Директ для детского лагеря / IT-лагеря.

Контекст:
- Кампания: {campaign}
- Поисковый запрос: {query}
- Решение системы: {decision}
- Показы: {impressions}
- Клики: {clicks}
- CTR: {round((ctr or 0) * 100, 2)}%
- CPC: {round(cpc or 0, 2)} ₽
- Cost: {round(cost or 0, 2)} ₽

Задача:
1. Если запрос релевантный, предложи 3 варианта объявления Яндекс Директ.
2. Если запрос нерелевантный или похож на чужой бренд/мусор, предложи минус-слова.
3. Пиши по-русски.
4. Заголовки короткие и коммерческие.
5. Текст объявления конкретный, без воды.

Формат ответа строго такой:

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
"""

        resp = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "Ты performance-маркетолог и директолог. Даёшь конкретные рекламные тексты и минус-слова.",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            temperature=0.7,
        )

        text = resp.choices[0].message.content or ""
        parsed = parse_llm_response(text)

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
            parsed["ai_title_1"],
            parsed["ai_title_2"],
            parsed["ai_text"],
            parsed["ai_title_1_b"],
            parsed["ai_title_2_b"],
            parsed["ai_text_b"],
            parsed["ai_title_1_c"],
            parsed["ai_title_2_c"],
            parsed["ai_text_c"],
            parsed["minus_words"],
            text,
        ))
        generated += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"AI creatives generated: {generated}")

if __name__ == "__main__":
    main()
