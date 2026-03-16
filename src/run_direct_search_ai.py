from datetime import date, timedelta
from src.db import db_cursor

def run(report_date=None):
    if report_date is None:
        report_date = (date.today() - timedelta(days=1)).isoformat()

    with db_cursor() as (_, cur):
        cur.execute("delete from mart_direct_search_ai where date = %s", (report_date,))

        # 1. Низкий CTR по ключу/запросу
        cur.execute("""
        insert into mart_direct_search_ai
        (date,campaign_name,ad_group_name,keyword,search_query,problem_type,metric_value,recommendation,priority)
        select
            date,
            campaign_name,
            ad_group_name,
            keyword,
            search_query,
            'LOW_CTR',
            ctr,
            'Низкий CTR: переписать объявление под запрос, добавить ключ в заголовок, усилить оффер, проверить конкурентность выдачи.',
            'high'
        from stg_direct_search_detail
        where date = %s
          and impressions >= 50
          and ctr < 0.03
        """, (report_date,))

        # 2. Клики без конверсий
        cur.execute("""
        insert into mart_direct_search_ai
        (date,campaign_name,ad_group_name,keyword,search_query,problem_type,metric_value,recommendation,priority)
        select
            date,
            campaign_name,
            ad_group_name,
            keyword,
            search_query,
            'CLICKS_NO_CONVERSIONS',
            clicks,
            'Есть клики без конверсий: проверить интент, посадочную страницу, релевантность оффера и отключить при повторении.',
            'high'
        from stg_direct_search_detail
        where date = %s
          and clicks >= 10
          and conversions = 0
        """, (report_date,))

        # 3. Высокий CPC
        cur.execute("""
        insert into mart_direct_search_ai
        (date,campaign_name,ad_group_name,keyword,search_query,problem_type,metric_value,recommendation,priority)
        select
            date,
            campaign_name,
            ad_group_name,
            keyword,
            search_query,
            'HIGH_CPC',
            avg_cpc,
            'Высокий CPC: проверить ставку, качество объявления, релевантность ключа и качество посадочной страницы.',
            'medium'
        from stg_direct_search_detail
        where date = %s
          and avg_cpc > 100
        """, (report_date,))

        # 4. Кандидаты в минус-слова
        cur.execute("""
        insert into mart_direct_search_ai
        (date,campaign_name,ad_group_name,keyword,search_query,problem_type,metric_value,recommendation,priority)
        select
            date,
            campaign_name,
            ad_group_name,
            keyword,
            search_query,
            'NEGATIVE_KEYWORD_CANDIDATE',
            clicks,
            'Кандидат в минус-слова: запрос приносит клики без конверсий. Проверить вручную и добавить в минус-слова при подтверждении.',
            'high'
        from stg_direct_search_detail
        where date = %s
          and clicks >= 5
          and conversions = 0
        """, (report_date,))

        # 5. Потенциал масштабирования
        cur.execute("""
        insert into mart_direct_search_ai
        (date,campaign_name,ad_group_name,keyword,search_query,problem_type,metric_value,recommendation,priority)
        select
            date,
            campaign_name,
            ad_group_name,
            keyword,
            search_query,
            'SCALE',
            ctr,
            'Запрос/ключ показывает хороший CTR: можно повышать ставки или расширять похожие ключи после проверки качества трафика.',
            'medium'
        from stg_direct_search_detail
        where date = %s
          and impressions >= 30
          and ctr >= 0.06
        """, (report_date,))

    return {"status": "ok", "date": report_date}

if __name__ == "__main__":
    print(run())
