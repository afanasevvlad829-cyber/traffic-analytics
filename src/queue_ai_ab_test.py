import os
import psycopg2
import sys

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

if len(sys.argv) < 3:
    raise SystemExit("Usage: python queue_ai_ab_test.py <ad_id> <variant:A|B|C>")

ad_id = int(sys.argv[1])
variant = sys.argv[2].upper()

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    dbname=os.getenv("PGDATABASE", "traffic_analytics"),
    user=os.getenv("PGUSER", "traffic_admin"),
    password=os.getenv("PGPASSWORD")
)
cur = conn.cursor()

cur.execute("""
select
    campaign_name,
    ad_id,
    ad_group_id,
    original_title,
    original_title_2,
    original_body_text,
    ai_title_1,
    ai_title_2,
    ai_body_1,
    ai_title_1_b,
    ai_title_2_b,
    ai_body_2,
    ai_title_1_c,
    ai_title_2_c,
    ai_body_3
from mart_ai_creative_candidates
where ad_id = %s
order by created_at desc
limit 1
""", (ad_id,))
row = cur.fetchone()

if not row:
    raise SystemExit(f"No candidate found for ad_id={ad_id}")

(
    campaign_name, ad_id, ad_group_id,
    source_title, source_title_2, source_body_text,
    a1, a2, b1,
    a1b, a2b, b2,
    a1c, a2c, b3
) = row

if variant == "A":
    new_title, new_title_2, new_body = a1, a2, b1
elif variant == "B":
    new_title, new_title_2, new_body = a1b, a2b, b2
elif variant == "C":
    new_title, new_title_2, new_body = a1c, a2c, b3
else:
    raise SystemExit("Variant must be A, B or C")

cur.execute("""
insert into mart_ai_ab_test_actions(
    campaign_name,
    ad_id,
    ad_group_id,
    source_title,
    source_title_2,
    source_body_text,
    new_title,
    new_title_2,
    new_body_text,
    action_type,
    status
)
values (%s,%s,%s,%s,%s,%s,%s,%s,%s,'CREATE_AB_TEST','PENDING')
""", (
    campaign_name,
    ad_id,
    ad_group_id,
    source_title,
    source_title_2,
    source_body_text,
    new_title,
    new_title_2,
    new_body
))

cur.execute("""
update mart_ai_creative_candidates
set decision = 'APPROVED'
where ad_id = %s
""", (ad_id,))

conn.commit()
cur.close()
conn.close()

print(f"A/B test queued for ad_id={ad_id}, variant={variant}")
