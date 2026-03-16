import os
import re
import psycopg2
from openai import OpenAI
from datetime import date

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

def extract_number(label: str, text: str, default: float = 50.0) -> float:
    m = re.search(rf"{label}:\s*([0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
    if not m:
        return default
    try:
        val = float(m.group(1))
        return max(0.0, min(100.0, val))
    except Exception:
        return default

def extract_text(label: str, text: str, default: str = "") -> str:
    m = re.search(rf"{label}:\s*(.*)", text, flags=re.IGNORECASE)
    return m.group(1).strip() if m else default

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
    cur.execute("delete from mart_direct_image_audit where audit_date = %s", (today,))
    conn.commit()

    cur.execute("""
        select
            campaign_name,
            ad_group_id,
            ad_title,
            ad_type,
            thumbnail_url,
            preview_url
        from vw_creative_score_report
        where coalesce(thumbnail_url, '') <> ''
           or coalesce(preview_url, '') <> ''
        order by creative_score asc, cost desc
        limit 12
    """)
    rows = cur.fetchall()

    audited = 0

    for campaign_name, ad_group_id, ad_title, ad_type, thumbnail_url, preview_url in rows:
        image_url = preview_url or thumbnail_url
        if not image_url:
            continue

        prompt = f"""
Ты аудируешь рекламный креатив для Яндекс Директа.
Ниша: детский IT-лагерь / летний лагерь для детей.

Контекст:
- Кампания: {campaign_name}
- ID группы/креатива: {ad_group_id}
- Заголовок объявления: {ad_title}
- Тип объявления: {ad_type}

Оцени картинку по шкале 0-100 и ответь строго в формате:

VISUAL_SCORE: ...
READABILITY_SCORE: ...
FOCUS_SCORE: ...
OFFER_MATCH_SCORE: ...
CHILD_FIT_SCORE: ...
CLUTTER_SCORE: ...
VERDICT: ...
RECOMMENDATION: ...

Критерии:
- VISUAL_SCORE: общий визуальный потенциал
- READABILITY_SCORE: насколько легко считывается главный смысл
- FOCUS_SCORE: есть ли понятный главный объект/центр внимания
- OFFER_MATCH_SCORE: соответствует ли картинка офферу детского IT-лагеря
- CHILD_FIT_SCORE: насколько образ подходит детям/родителям
- CLUTTER_SCORE: чем выше, тем больше визуального шума и перегруза
- VERDICT: STRONG / NORMAL / WEAK / BAD
- RECOMMENDATION: короткая практическая рекомендация на русском
"""

        try:
            resp = client.chat.completions.create(
                model=os.getenv("OPENAI_MODEL", "openai/gpt-4o-mini"),
                messages=[
                    {
                        "role": "system",
                        "content": "Ты senior creative strategist. Анализируешь рекламные изображения коротко и по делу."
                    },
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {"type": "image_url", "image_url": {"url": image_url}}
                        ]
                    }
                ],
                temperature=0.2,
            )
            text = resp.choices[0].message.content or ""
        except Exception as e:
            text = f"VISUAL_SCORE: 50\nREADABILITY_SCORE: 50\nFOCUS_SCORE: 50\nOFFER_MATCH_SCORE: 50\nCHILD_FIT_SCORE: 50\nCLUTTER_SCORE: 50\nVERDICT: REVIEW\nRECOMMENDATION: Не удалось проанализировать изображение автоматически: {e}"

        visual_score = extract_number("VISUAL_SCORE", text, 50)
        readability_score = extract_number("READABILITY_SCORE", text, 50)
        focus_score = extract_number("FOCUS_SCORE", text, 50)
        offer_match_score = extract_number("OFFER_MATCH_SCORE", text, 50)
        child_fit_score = extract_number("CHILD_FIT_SCORE", text, 50)
        clutter_score = extract_number("CLUTTER_SCORE", text, 50)
        verdict = extract_text("VERDICT", text, "REVIEW")
        recommendation = extract_text("RECOMMENDATION", text, "Проверить креатив вручную.")

        cur.execute("""
            insert into mart_direct_image_audit (
                audit_date,
                campaign_name,
                ad_group_id,
                ad_title,
                ad_type,
                thumbnail_url,
                preview_url,
                visual_score,
                readability_score,
                focus_score,
                offer_match_score,
                child_fit_score,
                clutter_score,
                verdict,
                recommendation,
                raw_llm_response
            )
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            today,
            campaign_name,
            ad_group_id,
            ad_title,
            ad_type,
            thumbnail_url,
            preview_url,
            visual_score,
            readability_score,
            focus_score,
            offer_match_score,
            child_fit_score,
            clutter_score,
            verdict,
            recommendation,
            text
        ))
        audited += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Image creatives audited: {audited}")

if __name__ == "__main__":
    main()
