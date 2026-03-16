import os
ENV_PATH = "/home/kv145/traffic-analytics/.env"
import re
from datetime import date
import psycopg2

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

def text_norm(s: str) -> list[str]:
    s = (s or "").lower()
    s = re.sub(r"[^a-zа-я0-9\s-]+", " ", s)
    return [x for x in s.split() if len(x) >= 3]

def relevance_score(keyword: str, title: str, headline: str, passage: str, domain: str) -> float:
    q = set(text_norm(keyword))
    if not q:
        return 0.0

    def overlap(text: str) -> float:
        t = set(text_norm(text))
        return len(q & t) / max(len(q), 1)

    score = 0.0
    score += overlap(title) * 40
    score += overlap(headline) * 35
    score += overlap(passage) * 20
    score += overlap(domain.replace(".", " ")) * 5
    return min(score * 100, 100.0)

def main():
    load_env(ENV_PATH)

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("PGDATABASE", "traffic_analytics"),
        user=os.getenv("PGUSER", "traffic_admin"),
        password=os.getenv("PGPASSWORD"),
    )
    cur = conn.cursor()

    report_date = date.today().isoformat()
    cur.execute("delete from mart_competitor_serp_alerts where report_date = %s", (report_date,))

    cur.execute("""
        select
            report_date,
            keyword,
            result_type,
            position,
            domain,
            url,
            title,
            coalesce(headline, ''),
            coalesce(passage, '')
        from stg_competitor_serp_daily
        where report_date = %s
    """, (report_date,))
    rows = cur.fetchall()

    for row in rows:
        report_date, keyword, result_type, position, domain, url, title, headline, passage = row
        score = relevance_score(keyword, title, headline, passage, domain)

        if score >= 70:
            verdict = "STRONG_ORGANIC_RESULT"
            action_note = "Сильный конкурент в выдаче: посмотреть promise, структуру title/H1 и оффер страницы."
        elif score >= 40:
            verdict = "MID_ORGANIC_RESULT"
            action_note = "Средняя релевантность: разобрать формулировки и что обещают на странице."
        else:
            verdict = "WEAK_ORGANIC_RESULT"
            action_note = "Слабая релевантность: можно перехватывать более точным оффером."

        cur.execute("""
            insert into mart_competitor_serp_alerts (
                report_date, keyword, result_type, position, domain, url, title,
                relevance_score, verdict, action_note
            )
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            report_date, keyword, result_type, position, domain, url, title,
            score, verdict, action_note
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("yandex competitor alerts built")

if __name__ == "__main__":
    main()
