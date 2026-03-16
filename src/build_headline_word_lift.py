import os
from datetime import datetime
import psycopg2

ENV_PATH = "/home/kv145/traffic-analytics/.env"

STOPWORDS = {
    "для","и","в","на","с","по","из","до","от","или","без","под","над",
    "детей","детский","летний","лагерь","это","как","что","подмосковье"
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

def main():
    load_env(ENV_PATH)

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("PGDATABASE", "traffic_analytics"),
        user=os.getenv("PGUSER", "traffic_admin"),
        password=os.getenv("PGPASSWORD"),
    )
    cur = conn.cursor()

    cur.execute("truncate table mart_headline_word_lift")

    cur.execute("""
        with ads as (
            select
                campaign_name,
                ad_group_id,
                lower(coalesce(ad_title, '')) as ad_title,
                impressions,
                clicks,
                ctr,
                account_avg_ctr
            from mart_direct_creative_score
            where coalesce(ad_title, '') <> ''
              and impressions > 0
        ),
        words as (
            select
                campaign_name,
                regexp_split_to_table(ad_title, '\s+') as word,
                impressions,
                clicks,
                ctr,
                account_avg_ctr
            from ads
        )
        select
            campaign_name,
            word,
            count(*) as ads_count,
            sum(impressions) as total_impressions,
            sum(clicks) as total_clicks,
            avg(ctr) as avg_ctr,
            avg(account_avg_ctr) as account_avg_ctr,
            case
                when avg(account_avg_ctr) > 0 then avg(ctr) / avg(account_avg_ctr) - 1
                else 0
            end as ctr_lift
        from words
        where length(word) >= 3
        group by campaign_name, word
        having sum(impressions) >= 20
        order by campaign_name, ctr_lift desc
    """)
    rows = cur.fetchall()

    inserted = 0
    for campaign_name, word, ads_count, total_impressions, total_clicks, avg_ctr, account_avg_ctr, ctr_lift in rows:
        word_clean = "".join(ch for ch in word if ch.isalnum() or ch in "-–")
        if not word_clean or word_clean in STOPWORDS:
            continue

        if ctr_lift >= 0.20:
            verdict = "POSITIVE_LIFT"
        elif ctr_lift <= -0.15:
            verdict = "NEGATIVE_LIFT"
        else:
            verdict = "NEUTRAL"

        cur.execute("""
            insert into mart_headline_word_lift (
                calculated_at,
                campaign_name,
                word,
                ads_count,
                total_impressions,
                total_clicks,
                avg_ctr,
                account_avg_ctr,
                ctr_lift,
                verdict
            ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            datetime.now(),
            campaign_name,
            word_clean,
            ads_count,
            total_impressions,
            total_clicks,
            avg_ctr,
            account_avg_ctr,
            ctr_lift,
            verdict
        ))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Headline word lift rows built: {inserted}")

if __name__ == "__main__":
    main()
