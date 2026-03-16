import os
import sys
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

load_env("/home/kv145/traffic-analytics/.env_ai")
load_env("/home/kv145/traffic-analytics/.env_telegram")

if len(sys.argv) < 2:
    raise SystemExit("Usage: python src/direct_task_done.py <task_id> [done_by]")

task_id = int(sys.argv[1])
done_by = sys.argv[2] if len(sys.argv) > 2 else "manual"

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    dbname=os.getenv("PGDATABASE", "traffic_analytics"),
    user=os.getenv("PGUSER", "traffic_admin"),
    password=os.getenv("PGPASSWORD"),
)
cur = conn.cursor()
cur.execute("""
update mart_direct_task_queue
set status = 'DONE',
    completed_at = now(),
    completed_by = %s
where task_id = %s
""", (done_by, task_id))
conn.commit()
print(f"Task {task_id} marked as DONE by {done_by}")
