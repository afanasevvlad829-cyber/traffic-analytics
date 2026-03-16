import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor

ENV="/home/kv145/traffic-analytics/.env"

def load_env():
    with open(ENV) as f:
        for line in f:
            if "=" not in line:
                continue
            k,v=line.strip().split("=",1)
            os.environ[k]=v.strip('"')

load_env()

def db():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        cursor_factory=RealDictCursor
    )

def upsert(cur,code,type_,entity,campaign,payload):

    cur.execute("""
    insert into ai_context_registry
    (context_code,context_type,entity_id,campaign_name,payload_json)
    values (%s,%s,%s,%s,%s)
    on conflict (context_code)
    do update set
    payload_json=excluded.payload_json,
    updated_at=now()
    """,(code,type_,entity,campaign,json.dumps(payload)))

def build_creatives(cur):

    cur.execute("""
    select *
    from mart_ai_creative_candidates
    order by created_at desc
    limit 500
    """)

    rows=cur.fetchall()

    for r in rows:

        base=f"CR-{r['ad_id']}"

        payload={
            "type":"creative_candidate",
            "campaign":r["campaign_name"],
            "ad_id":r["ad_id"],
            "group_id":r["ad_group_id"],
            "current":{
                "title1":r["original_title"],
                "title2":r["original_title_2"],
                "body":r["original_body_text"]
            },
            "queries":r["sample_queries"],
            "forecast":{
                "ctr":r["predicted_ctr_pct"],
                "cpc":r["predicted_cpc"],
                "relevance":r["predicted_relevance"],
                "confidence":r["prediction_confidence"]
            },
            "variants":{
                "A":{
                    "title1":r["ai_title_1"],
                    "title2":r["ai_title_2"],
                    "body":r["ai_body_1"]
                },
                "B":{
                    "title1":r["ai_title_1_b"],
                    "title2":r["ai_title_2_b"],
                    "body":r["ai_body_2"]
                },
                "C":{
                    "title1":r["ai_title_1_c"],
                    "title2":r["ai_title_2_c"],
                    "body":r["ai_body_3"]
                }
            }
        }

        upsert(cur,base,"creative",str(r["ad_id"]),r["campaign_name"],payload)

def build_structure(cur):

    cur.execute("""
    select *
    from mart_group_builder
    order by created_at desc
    limit 200
    """)

    rows=cur.fetchall()

    for r in rows:

        code=f"ST-{r['ad_group_id']}"

        payload={
            "type":"structure",
            "campaign":r["campaign_name"],
            "group":r["ad_group_id"],
            "queries":r["queries"],
            "recommendation":r["recommendation"]
        }

        upsert(cur,code,"structure",str(r["ad_group_id"]),r["campaign_name"],payload)

def run():

    conn=db()

    try:
        with conn.cursor() as cur:

            build_creatives(cur)
            build_structure(cur)

        conn.commit()

    finally:
        conn.close()

if __name__=="__main__":
    run()
