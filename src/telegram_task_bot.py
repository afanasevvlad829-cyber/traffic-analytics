import os
ENV_PATH = "/home/kv145/traffic-analytics/.env"
import time
import requests
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
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

load_env(ENV_PATH)

BOT_TOKEN = os.getenv("TG_TOKEN")
ALLOWED_CHAT = str(os.getenv("TG_CHAT"))

def db():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("PGDATABASE", "traffic_analytics"),
        user=os.getenv("PGUSER", "traffic_admin"),
        password=os.getenv("PGPASSWORD"),
    )

def send(chat_id: str, text: str):
    requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": text[:4000]},
        timeout=30,
    )

def mark_done(task_id: int, who: str):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        update mart_direct_task_queue
        set status = 'DONE',
            completed_at = now(),
            completed_by = %s
        where task_id = %s
    """, (who, task_id))
    conn.commit()
    cur.close()
    conn.close()

def snooze(task_id: int, who: str):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        update mart_direct_task_queue
        set status = 'SNOOZED',
            completed_at = now(),
            completed_by = %s
        where task_id = %s
    """, (who, task_id))
    conn.commit()
    cur.close()
    conn.close()

def main():
    offset = 0
    while True:
        r = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
            params={"timeout": 20, "offset": offset},
            timeout=40,
        )
        data = r.json()
        for upd in data.get("result", []):
            offset = upd["update_id"] + 1

            msg = upd.get("message") or {}
            chat = msg.get("chat", {})
            chat_id = str(chat.get("id"))
            text = (msg.get("text") or "").strip()

            if not text or chat_id != ALLOWED_CHAT:
                continue

            if text.startswith("/done "):
                try:
                    task_id = int(text.split()[1])
                    mark_done(task_id, "telegram")
                    send(chat_id, f"✅ Задача #{task_id} закрыта.")
                except Exception as e:
                    send(chat_id, f"Ошибка /done: {e}")

            elif text.startswith("/snooze "):
                try:
                    task_id = int(text.split()[1])
                    snooze(task_id, "telegram")
                    send(chat_id, f"😴 Задача #{task_id} отложена.")
                except Exception as e:
                    send(chat_id, f"Ошибка /snooze: {e}")

            elif text.startswith("/open"):
                conn = db()
                cur = conn.cursor()
                cur.execute("""
                    select task_id, decision, search_query
                    from mart_direct_task_queue
                    where status = 'OPEN'
                    order by priority_score desc, cost desc
                    limit 10
                """)
                rows = cur.fetchall()
                cur.close()
                conn.close()

                if not rows:
                    send(chat_id, "Открытых задач нет.")
                else:
                    answer = "📌 OPEN TASKS\n\n" + "\n".join(
                        f"#{tid} | {decision} | {query}" for tid, decision, query in rows
                    )
                    send(chat_id, answer)
        time.sleep(2)

if __name__ == "__main__":
    main()
