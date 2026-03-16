import os
import re
from collections import Counter
from datetime import datetime
import psycopg2

ENV_PATH = "/home/kv145/traffic-analytics/.env"

STOPWORDS = {
    "для","и","в","на","с","по","из","до","от","или","без","под","над",
    "это","как","что","подмосковье"
}

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

def normalize(text: str):
    text = (text or "").lower()
    text = text.replace("—", " ").replace("–", " ")
    text = re.sub(r"[^a-zа-я0-9\s-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def tokenize(text: str):
    tokens = normalize(text).split()
    out = []
    for t in tokens:
        if len(t) < 2:
            continue
        if t in STOPWORDS:
            continue
        out.append(t)
    return out

def patterns(tokens):
    result = []
    for i in range(len(tokens) - 1):
        bg = f"{tokens[i]} {tokens[i+1]}"
        result.append((bg, "BIGRAM"))
    for i in range(len(tokens) - 2):
        tg = f"{tokens[i]} {tokens[i+1]} {tokens[i+2]}"
        result.append((tg, "TRIGRAM"))
    return result

def main():
    load_env(ENV_PATH)

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("PGDATABASE", "traffic_analytics"),
        user=os.getenv("PGUSER", "traffic_admin"),
        password=os.getenv("PGPASSWORD"),
    )
    cur = conn.cursor()

    cur.execute("truncate table mart_headline_pattern_lift")

    cur.execute("""
        select
            campaign_name,
            ad_group_id,
            coalesce(ad_title, '') as ad_title,
            impressions,
            clicks,
            ctr,
            account_avg_ctr
        from mart_direct_creative_score
        where coalesce(ad_title, '') <> ''
          and impressions > 0
    """)
    rows = cur.fetchall()

    bucket = {}

    for campaign_name, ad_group_id, ad_title, impressions, clicks, ctr, account_avg_ctr in rows:
        toks = tokenize(ad_title)
        pats = set(patterns(toks))
        for pattern, pattern_type in pats:
            key = (campaign_name, pattern, pattern_type)
            if key not in bucket:
                bucket[key] = {
                    "ads_count": 0,
                    "impressions": 0,
                    "clicks": 0,
                    "ctr_sum": 0.0,
                    "account_ctr_sum": 0.0,
                }
            bucket[key]["ads_count"] += 1
            bucket[key]["impressions"] += int(impressions or 0)
            bucket[key]["clicks"] += int(clicks or 0)
            bucket[key]["ctr_sum"] += float(ctr or 0.0)
            bucket[key]["account_ctr_sum"] += float(account_avg_ctr or 0.0)

    inserted = 0
    now = datetime.now()

    for (campaign_name, pattern, pattern_type), stats in bucket.items():
        if stats["impressions"] < 30:
            continue
        if stats["ads_count"] < 1:
            continue

        avg_ctr = stats["ctr_sum"] / max(stats["ads_count"], 1)
        account_avg_ctr = stats["account_ctr_sum"] / max(stats["ads_count"], 1)

        if account_avg_ctr > 0:
            ctr_lift = avg_ctr / account_avg_ctr - 1
        else:
            ctr_lift = 0.0

        if ctr_lift >= 0.20:
            verdict = "POSITIVE_LIFT"
        elif ctr_lift <= -0.15:
            verdict = "NEGATIVE_LIFT"
        else:
            verdict = "NEUTRAL"

        cur.execute("""
            insert into mart_headline_pattern_lift (
                calculated_at,
                campaign_name,
                pattern,
                pattern_type,
                ads_count,
                total_impressions,
                total_clicks,
                avg_ctr,
                account_avg_ctr,
                ctr_lift,
                verdict
            ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            now,
            campaign_name,
            pattern,
            pattern_type,
            stats["ads_count"],
            stats["impressions"],
            stats["clicks"],
            avg_ctr,
            account_avg_ctr,
            ctr_lift,
            verdict
        ))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Headline pattern lift rows built: {inserted}")

if __name__ == "__main__":
    main()
