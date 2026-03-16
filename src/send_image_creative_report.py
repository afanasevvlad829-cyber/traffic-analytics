import os
import psycopg2
import requests

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

load_env(ENV_PATH)

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    dbname=os.getenv("PGDATABASE", "traffic_analytics"),
    user=os.getenv("PGUSER", "traffic_admin"),
    password=os.getenv("PGPASSWORD"),
)
cur = conn.cursor()

cur.execute("""
    select
        campaign_name,
        ad_group_id,
        ad_title,
        ad_type,
        visual_score,
        readability_score,
        focus_score,
        offer_match_score,
        child_fit_score,
        clutter_score,
        verdict,
        recommendation,
        coalesce(preview_url, thumbnail_url, '') as image_url
    from vw_image_creative_audit_report
    where audit_date = current_date
    order by
        case verdict
            when 'BAD' then 1
            when 'WEAK' then 2
            when 'NORMAL' then 3
            when 'STRONG' then 4
            else 5
        end,
        visual_score asc
    limit 10
""")
rows = cur.fetchall()
cur.close()
conn.close()

text = "🖼 IMAGE CREATIVE AUDIT\n\n"

if not rows:
    text += "Сегодня креативы с картинками не найдены или не были проаудированы."
else:
    for row in rows:
        campaign_name, ad_group_id, ad_title, ad_type, visual_score, readability_score, focus_score, offer_match_score, child_fit_score, clutter_score, verdict, recommendation, image_url = row
        text += (
            f"{campaign_name}\n"
            f"ID: {ad_group_id}\n"
            f"Заголовок: {ad_title}\n"
            f"Тип: {ad_type}\n"
            f"Verdict: {verdict}\n"
            f"Visual: {visual_score} | Readability: {readability_score} | Focus: {focus_score}\n"
            f"Offer Match: {offer_match_score} | Child Fit: {child_fit_score} | Clutter: {clutter_score}\n"
            f"Рекомендация: {recommendation}\n"
        )
        if image_url:
            text += f"Image: {image_url}\n"
        text += "\n---\n\n"

url = f"https://api.telegram.org/bot{os.getenv('TG_TOKEN')}/sendMessage"
resp = requests.post(
    url,
    json={"chat_id": os.getenv("TG_CHAT"), "text": text[:4000]},
    timeout=30
)
resp.raise_for_status()
print("Image creative report sent")
