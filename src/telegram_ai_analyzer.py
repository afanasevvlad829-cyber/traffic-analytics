import os
import re
import json
import psycopg2
import requests

ENV="/home/kv145/traffic-analytics/.env"

def load_env():
    with open(ENV) as f:
        for l in f:
            if "=" not in l:
                continue
            k,v=l.strip().split("=",1)
            os.environ[k]=v

load_env()

BOT=os.getenv("TG_TOKEN")

def db():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD")
    )

def get_context(code):

    conn=db()

    try:
        cur=conn.cursor()

        cur.execute("""
        select payload_json
        from ai_context_registry
        where context_code=%s
        """,(code,))

        row=cur.fetchone()

        if not row:
            return None

        return row[0]

    finally:
        conn.close()

def analyze(code):

    ctx=get_context(code)

    if not ctx:
        return "Контекст не найден"

    text=json.dumps(ctx,indent=2)

    prompt=f"""
Ты AI директолог.

Проанализируй объект рекламы.

CODE: {code}

DATA:
{text}

Ответь:

1 сильные стороны
2 слабые стороны
3 запускать ли A/B тест
4 что улучшить
"""

    r=requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization":f"Bearer {os.getenv('OPENAI_KEY')}"
        },
        json={
            "model":"gpt-4o-mini",
            "messages":[{"role":"user","content":prompt}]
        }
    )

    return r.json()["choices"][0]["message"]["content"]

def handle(text):

    m=re.search(r"(CR|ST)-[0-9]+",text)

    if not m:
        return None

    code=m.group(0)

    return analyze(code)
