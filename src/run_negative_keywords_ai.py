import os
import re
from datetime import date
import psycopg2
from openai import OpenAI

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

def parse_section(name: str, text: str):
    m = re.search(rf"{name}:\s*(.*?)(?:\n[A-Z_]+:|$)", text, flags=re.S)
    if not m:
        return []
    raw = m.group(1).strip()
    if not raw:
        return []
    parts = re.split(r"[,;\n]", raw)
    return [p.strip().lower() for p in parts if p.strip()]

def parse_rationales(text: str):
    out = {}
    block = re.search(r"RATIONALES:\s*(.*)$", text, flags=re.S)
    if not block:
        return out
    for line in block.group(1).splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        w, r = line.split(":", 1)
        out[w.strip().lower()] = r.strip()
    return out

def main():
    load_env(ENV_PATH)

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

    today = date.today().isoformat()
    cur.execute("delete from mart_campaign_negative_keywords_ai where report_date = %s", (today,))
    conn.commit()

    cur.execute("""
        select
            campaign_name,
            string_agg(negative_keyword, ', ' order by negative_keyword) as candidates
        from vw_campaign_negative_keywords
        group by campaign_name
        order by campaign_name
    """)
    campaigns = cur.fetchall()

    inserted = 0

    for campaign_name, candidates in campaigns:
        if not candidates:
            continue

        prompt = f"""
Ты проверяешь кандидаты в минус-слова для поисковой кампании Яндекс Директа.

Контекст кампании:
- название: {campaign_name}
- продукт: детский лагерь / IT-лагерь / летний лагерь для детей
- нельзя минусовать слова, которые являются ядром продукта: лагерь, детский, летний, путевка, дети, смена, отдых, каникулы, подмосковье, лагерь для детей

Кандидаты:
{candidates}

Задача:
1. Раздели слова на 2 списка:
SAFE_NEGATIVES — точно можно минусовать
BLOCKED_NEGATIVES — нельзя минусовать или нужно оставить
2. Учитывай контекст кампании.
3. Если слово слишком общее и может убить релевантный трафик, отправляй его в BLOCKED_NEGATIVES.

Формат ответа строго такой:

SAFE_NEGATIVES: слово1, слово2
BLOCKED_NEGATIVES: слово3, слово4
RATIONALES:
слово1: причина
слово2: причина
слово3: причина
"""

        resp = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "openai/gpt-4o-mini"),
            messages=[
                {"role": "system", "content": "Ты senior PPC-специалист. Очень осторожно работаешь с минус-словами."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
        )
        text = resp.choices[0].message.content or ""

        safe_words = parse_section("SAFE_NEGATIVES", text)
        blocked_words = parse_section("BLOCKED_NEGATIVES", text)
        rationales = parse_rationales(text)

        for w in safe_words:
            cur.execute("""
                insert into mart_campaign_negative_keywords_ai (
                    report_date, campaign_name, candidate_word, decision, rationale
                ) values (%s,%s,%s,'SAFE_NEGATIVE',%s)
            """, (today, campaign_name, w, rationales.get(w, "Подходит для минусации")))
            inserted += 1

        for w in blocked_words:
            cur.execute("""
                insert into mart_campaign_negative_keywords_ai (
                    report_date, campaign_name, candidate_word, decision, rationale
                ) values (%s,%s,%s,'BLOCKED_NEGATIVE',%s)
            """, (today, campaign_name, w, rationales.get(w, "Нельзя минусовать автоматически")))
            inserted += 1

        conn.commit()

    cur.close()
    conn.close()
    print(f"AI negative keywords processed: {inserted}")

if __name__ == "__main__":
    main()
