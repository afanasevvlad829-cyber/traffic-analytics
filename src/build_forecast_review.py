import os
from datetime import date
import psycopg2

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
    password=os.getenv("PGPASSWORD"),
)
cur = conn.cursor()

today = date.today().isoformat()
cur.execute("delete from mart_ai_creative_forecast_review where review_date = %s", (today,))

cur.execute("""
select
    c.campaign_name,
    c.ad_group_id,
    c.ad_id,
    c.predicted_ctr_pct,
    c.predicted_cpc,
    c.predicted_relevance,
    round(cs.ctr * 100, 2) as actual_ctr_pct,
    cs.avg_cpc as actual_cpc,
    cs.relevance_score as actual_relevance
from mart_ai_creative_candidates c
left join mart_direct_creative_score cs
    on cs.ad_group_id = c.ad_group_id
where c.decision in ('APPROVED','EXECUTED')
order by c.created_at desc
limit 50
""")
rows = cur.fetchall()

inserted = 0
for campaign, ad_group_id, ad_id, pred_ctr, pred_cpc, pred_rel, act_ctr, act_cpc, act_rel in rows:
    if act_ctr is None and act_cpc is None and act_rel is None:
        status = "WAITING_FOR_DATA"
        comment = "Нет фактических данных после запуска теста."
    else:
        ctr_ok = act_ctr is not None and pred_ctr is not None and float(act_ctr) >= float(pred_ctr) * 0.9
        cpc_ok = act_cpc is not None and pred_cpc is not None and float(act_cpc) <= float(pred_cpc) * 1.15

        if ctr_ok and cpc_ok:
            status = "BETTER_OR_CLOSE_TO_FORECAST"
            comment = "Фактические метрики близки к прогнозу или лучше."
        else:
            status = "WORSE_THAN_FORECAST"
            comment = "Фактические метрики пока хуже прогноза. Нужна дополнительная проверка."

    cur.execute("""
        insert into mart_ai_creative_forecast_review(
            review_date,
            campaign_name,
            ad_group_id,
            ad_id,
            variant,
            predicted_ctr_pct,
            predicted_cpc,
            predicted_relevance,
            actual_ctr_pct,
            actual_cpc,
            actual_relevance,
            forecast_status,
            comment
        )
        values (%s,%s,%s,%s,'TEST',%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        today,
        campaign,
        ad_group_id,
        ad_id,
        pred_ctr,
        pred_cpc,
        pred_rel,
        act_ctr,
        act_cpc,
        act_rel,
        status,
        comment
    ))
    inserted += 1

conn.commit()
cur.close()
conn.close()

print(f"forecast review rows built: {inserted}")
