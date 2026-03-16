import os
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

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

def db():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("PGDATABASE", "traffic_analytics"),
        user=os.getenv("PGUSER", "traffic_admin"),
        password=os.getenv("PGPASSWORD"),
        cursor_factory=RealDictCursor,
    )

def mark_done(cur, table, row_id, note):
    cur.execute(f"update {table} set status='DONE', executed_at=now(), api_response=%s where id=%s", (note, row_id))

def log(cur, entity_type, entity_key, action, status, details):
    cur.execute("""
        insert into ui_decision_log(entity_type, entity_key, action, status, details, actor)
        values (%s,%s,%s,%s,%s,'executor')
    """, (entity_type, entity_key, action, status, details))

def run():
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                select *
                from mart_ai_ab_test_actions
                where status = 'PENDING'
                order by created_at asc
                limit 20
            """)
            ab_rows = cur.fetchall()

            for row in ab_rows:
                note = "V1 executor: action accepted and logged. Direct API mutation kept in safe-manual mode."
                mark_done(cur, "mart_ai_ab_test_actions", row["id"], note)
                cur.execute("""
                    update mart_ai_creative_candidates
                    set decision = 'EXECUTED'
                    where ad_id = %s
                """, (row["ad_id"],))
                log(cur, "creative", str(row["ad_id"]), row["action_type"], "DONE", note)

            cur.execute("""
                select *
                from mart_negative_actions
                where status = 'PENDING'
                order by created_at asc
                limit 20
            """)
            neg_rows = cur.fetchall()

            for row in neg_rows:
                note = "V1 executor: safe negatives captured and marked for manual application."
                mark_done(cur, "mart_negative_actions", row["id"], note)
                log(cur, "negative", row["campaign_name"], row["action_type"], "DONE", note)

            cur.execute("""
                select *
                from mart_structure_actions
                where status = 'PENDING'
                order by created_at asc
                limit 20
            """)
            st_rows = cur.fetchall()

            for row in st_rows:
                note = "V1 executor: structure action accepted and logged for manual rebuild."
                mark_done(cur, "mart_structure_actions", row["id"], note)
                log(cur, "structure", f'{row["campaign_name"]}:{row["ad_group_id"]}', row["action_type"], "DONE", note)

        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    run()
    print(f"direct_v1_executor finished at {datetime.now().isoformat()}")
