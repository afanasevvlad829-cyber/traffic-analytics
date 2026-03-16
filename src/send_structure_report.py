import os
import psycopg2
import requests

ENV_PATH="/home/kv145/traffic-analytics/.env"

def load_env():
with open(ENV_PATH) as f:
for line in f:
if "=" not in line:
continue
k,v=line.strip().split("=",1)
os.environ[k]=v.strip('"')

load_env()

conn=psycopg2.connect(
host="localhost",
dbname="traffic_analytics",
user="traffic_admin",
password=os.getenv("PGPASSWORD")
)

cur=conn.cursor()

cur.execute("""
select
campaign_name,
ad_group_id,
queries,
keywords,
avg_ctr,
avg_cpc,
issue
from mart_structure_audit
order by created_at desc
limit 5
""")

rows=cur.fetchall()

text="🧠 DIRECT STRUCTURE AUDIT\n\n"

for r in rows:

campaign, group_id, queries, keywords, ctr, cpc, issue = r

text+=f"""
Кампания
{campaign}

Группа
{group_id}

CTR
{round(ctr*100,2)}%

CPC
{cpc}

Ключи
{keywords}

Запросы
{queries}

Анализ
{issue}

-------------
"""

url=f"https://api.telegram.org/bot{os.getenv('TG_TOKEN')}/sendMessage"

requests.post(
url,
json={
"chat_id":os.getenv("TG_CHAT"),
"text":text[:4000]
}
)

print("structure report sent")
