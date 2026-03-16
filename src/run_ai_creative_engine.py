import os
import re
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

def parse_variant_block(text: str):
    def find(label: str):
        m = re.search(rf"{label}:\s*(.*)", text, flags=re.IGNORECASE)
        return m.group(1).strip() if m else ""
    return {
        "title1": find("TITLE1"),
        "title2": find("TITLE2"),
        "body": find("BODY"),
        "title1_b": find("TITLE1_B"),
        "title2_b": find("TITLE2_B"),
        "body_b": find("BODY_B"),
        "title1_c": find("TITLE1_C"),
        "title2_c": find("TITLE2_C"),
        "body_c": find("BODY_C"),
    }

def normalize(text: str) -> str:
    text = (text or "").lower()
    text = text.replace("—", " ").replace("–", " ")
    text = re.sub(r"[^a-zа-я0-9\s-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def lexical_relevance(sample_queries: str, title1: str, title2: str, body: str) -> float:
    query_words = set([w for w in normalize(sample_queries).split() if len(w) >= 3])
    ad_words = set([w for w in normalize(" ".join([title1, title2, body])).split() if len(w) >= 3])
    if not query_words:
        return 50.0
    overlap = len(query_words & ad_words) / max(len(query_words), 1)
    return round(min(90.0, max(45.0, 35.0 + overlap * 65.0)), 2)

def pattern_lift(cur, campaign_name: str, full_title: str):
    full_title = normalize(full_title)
    cur.execute("""
        select pattern, ctr_lift
        from mart_headline_pattern_lift
        where campaign_name = %s
    """, (campaign_name,))
    rows = cur.fetchall()

    matched = []
    for pattern, ctr_lift in rows:
        p = normalize(pattern)
        if p and p in full_title:
            matched.append(float(ctr_lift or 0))

    if not matched:
        return 0.0, 0

    avg_lift = sum(matched) / len(matched)
    avg_lift = max(-0.20, min(0.60, avg_lift))
    return avg_lift, len(matched)

def confidence_label(matches: int, impressions: int):
    if matches >= 3 and impressions >= 100:
        return "HIGH"
    if matches >= 1 and impressions >= 40:
        return "MEDIUM"
    return "LOW"

load_env()

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url=os.getenv("OPENAI_BASE_URL")
)

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    dbname=os.getenv("PGDATABASE", "traffic_analytics"),
    user=os.getenv("PGUSER", "traffic_admin"),
    password=os.getenv("PGPASSWORD")
)
cur = conn.cursor()

cur.execute("truncate table mart_ai_creative_candidates")

cur.execute("""
select
    cs.campaign_name,
    coalesce(am.ad_id, 0) as ad_id,
    cs.ad_group_id,
    cs.ad_title,
    coalesce(cs.ad_title_2, '') as ad_title_2,
    coalesce(cs.body_text, '') as body_text,
    cs.creative_score,
    cs.ctr,
    cs.avg_cpc,
    cs.relevance_score,
    coalesce(cs.sample_queries, '') as sample_queries,
    cs.impressions
from mart_direct_creative_score cs
left join (
    select distinct on (ad_group_id)
        ad_group_id,
        ad_id
    from stg_direct_ads_meta
    where ad_id is not null
    order by ad_group_id, loaded_at desc nulls last, ad_id desc
) am
    on cs.ad_group_id = am.ad_group_id
where cs.creative_score < 75
  and cs.impressions >= 20
order by cs.creative_score asc, cs.cost desc
limit 10
""")
rows = cur.fetchall()

generated = 0

for campaign, ad_id, ad_group_id, title, title2, body_text, score, ctr, cpc, relevance, sample_queries, impressions in rows:
    cur.execute("""
        select pattern, round(ctr_lift * 100, 2) as ctr_lift_pct
        from mart_headline_pattern_lift
        where campaign_name = %s
          and verdict = 'POSITIVE_LIFT'
        order by ctr_lift desc
        limit 5
    """, (campaign,))
    pos_patterns = cur.fetchall()

    cur.execute("""
        select pattern, round(ctr_lift * 100, 2) as ctr_lift_pct
        from mart_headline_pattern_lift
        where campaign_name = %s
          and verdict = 'NEGATIVE_LIFT'
        order by ctr_lift asc
        limit 5
    """, (campaign,))
    neg_patterns = cur.fetchall()

    positive_hint = ", ".join([f"{p} ({v}%)" for p, v in pos_patterns]) or "нет данных"
    negative_hint = ", ".join([f"{p} ({v}%)" for p, v in neg_patterns]) or "нет данных"

    prompt = f"""
Ты сильный директолог по Яндекс Директ.

Нужно сделать 3 новых ПОЛНЫХ варианта поискового объявления для детского IT-лагеря.

Контекст:
Кампания: {campaign}
Ad ID: {ad_id}
Ad Group ID: {ad_group_id}

Текущее объявление:
TITLE1: {title}
TITLE2: {title2}
BODY: {body_text}

Метрики:
Creative score: {score}
CTR: {round((ctr or 0)*100, 2)}%
CPC: {cpc} ₽
Relevance: {relevance}

Условия показа / ключевые запросы:
{sample_queries}

Фразы, которые чаще поднимают CTR:
{positive_hint}

Фразы, которых лучше избегать:
{negative_hint}

Задача:
Сделай 3 новых ПОЛНЫХ варианта объявления:
- TITLE1
- TITLE2
- BODY

Правила:
- релевантно поисковым запросам
- коротко и коммерчески
- не вода
- не копируй исходник буквально
- используй сильные паттерны, если они уместны
- избегай слабых паттернов

Ответ строго в формате:

TITLE1: ...
TITLE2: ...
BODY: ...

TITLE1_B: ...
TITLE2_B: ...
BODY_B: ...

TITLE1_C: ...
TITLE2_C: ...
BODY_C: ...
"""

    resp = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL"),
        messages=[
            {"role": "system", "content": "Ты сильный директолог. Пишешь эффективные поисковые объявления для Яндекс Директ."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.5
    )

    txt = resp.choices[0].message.content or ""
    parsed = parse_variant_block(txt)

    if not parsed["title1"] or not parsed["body"]:
        continue

    p_lift, matches = pattern_lift(cur, campaign, f"{parsed['title1']} {parsed['title2']}")
    pred_relevance = lexical_relevance(sample_queries, parsed["title1"], parsed["title2"], parsed["body"])
    current_ctr_pct = round((ctr or 0) * 100, 2)
    current_cpc = float(cpc or 0)
    current_rel = float(relevance or 50)

    # Базовый прогноз
    predicted_ctr_pct = max(
        current_ctr_pct * (1 + p_lift + max(0.0, (pred_relevance - current_rel) / 200.0)),
        current_ctr_pct * 0.9 if current_ctr_pct > 0 else 1.0
    )
    predicted_ctr_pct = round(min(predicted_ctr_pct, current_ctr_pct * 1.8 if current_ctr_pct > 0 else predicted_ctr_pct), 2)

    rel_gain = max(0.0, pred_relevance - current_rel)
    cpc_factor = 1 - min(0.35, rel_gain / 120.0 + max(0.0, p_lift) / 3.0)
    predicted_cpc = round(current_cpc * cpc_factor, 2) if current_cpc > 0 else 0.0

    confidence = confidence_label(matches, int(impressions or 0))
    reason = (
        f"Прогноз основан на pattern lift={round(p_lift*100,2)}%, "
        f"совпадениях паттернов={matches}, "
        f"ожидаемой релевантности={pred_relevance}."
    )

    cur.execute("""
        insert into mart_ai_creative_candidates(
            campaign_name,
            ad_id,
            ad_group_id,
            original_title,
            original_title_2,
            original_body_text,
            sample_queries,
            score,
            ctr,
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
        )
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        campaign,
        ad_id,
        ad_group_id,
        title,
        title2,
        body_text,
        sample_queries,
        score,
        ctr,
        cpc,
        parsed["title1"],
        parsed["title2"],
        parsed["body"],
        parsed["title1_b"],
        parsed["title2_b"],
        parsed["body_b"],
        parsed["title1_c"],
        parsed["title2_c"],
        parsed["body_c"],
        predicted_ctr_pct,
        predicted_cpc,
        pred_relevance,
        confidence,
        reason
    ))
    generated += 1

conn.commit()
cur.close()
conn.close()

print(f"AI creative candidates generated: {generated}")
