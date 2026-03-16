import os
import psycopg2
from psycopg2.extras import RealDictCursor

from direct_api.ads import create_text_ad
from direct_api.keywords import add_negative_keywords


def db():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        cursor_factory=RealDictCursor
    )


def execute_ab_tests(cur):

    cur.execute("""
    select *
    from mart_ai_ab_test_actions
    where status='PENDING'
    order by created_at
    limit 20
    """)

    rows = cur.fetchall()

    for r in rows:

        try:

            create_text_ad(
                r["ad_group_id"],
                r["new_title"],
                r["new_title_2"],
                r["new_body_text"]
            )

            cur.execute("""
            update mart_ai_ab_test_actions
            set status='DONE', executed_at=now()
            where id=%s
            """,(r["id"],))

            cur.execute("""
            update mart_ai_creative_candidates
            set decision='EXECUTED'
            where ad_id=%s
            """,(r["ad_id"],))

        except Exception as e:

            cur.execute("""
            update mart_ai_ab_test_actions
            set status='FAILED',
                api_response=%s
            where id=%s
            """,(str(e), r["id"]))



def execute_negatives(cur):

    cur.execute("""
    select *
    from mart_negative_actions
    where status='PENDING'
    """)

    rows = cur.fetchall()

    for r in rows:

        try:

            add_negative_keywords(
                r["campaign_name"],
                r["words_text"]
            )

            cur.execute("""
            update mart_negative_actions
            set status='DONE',
                executed_at=now()
            where id=%s
            """,(r["id"],))

        except Exception as e:

            cur.execute("""
            update mart_negative_actions
            set status='FAILED',
                api_response=%s
            where id=%s
            """,(str(e), r["id"]))



def run():

    conn = db()

    try:

        with conn.cursor() as cur:

            execute_ab_tests(cur)

            execute_negatives(cur)

        conn.commit()

    finally:

        conn.close()



if __name__ == "__main__":
    run()
