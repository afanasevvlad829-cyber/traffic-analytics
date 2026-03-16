from datetime import date, timedelta
from src.db import db_cursor

def run(report_date=None):
    if report_date is None:
        report_date = (date.today() - timedelta(days=1)).isoformat()

    with db_cursor() as (_, cur):
        cur.execute("delete from mart_direct_search_ai where date = %s", (report_date,))

        # 1. Низкий CTR
        cur.execute("""
        insert into mart_direct_search_ai
        (date,campaign_name,ad_group_id,search_query,problem_type,impressions,clicks,cost,ctr,avg_cpc,recommendation,priority)
        select
            date,
            campaign_name,
            ad_group_id,
            search_query,
            'LOW_CTR',
            impressions,
            clicks,
            cost,
            case when impressions > 0 then round(clicks::numeric / impressions, 4) else 0 end as ctr,
            case when clicks > 0 then round(cost / clicks, 2) else 0 end as avg_cpc,
            'Низкий CTR: переписать объявление под запрос, добавить запрос в заголовок, усилить оффер и проверить конкурентность выдачи.',
            'high'
        from stg_direct_search_queries
        where date = %s
          and impressions >= 20
          and (case when impressions > 0 then clicks::numeric / impressions else 0 end) < 0.03
        """, (report_date,))

        # 2. Клики без результата
        cur.execute("""
        insert into mart_direct_search_ai
        (date,campaign_name,ad_group_id,search_query,problem_type,impressions,clicks,cost,ctr,avg_cpc,recommendation,priority)
        select
            date,
            campaign_name,
            ad_group_id,
            search_query,
            'CLICKS_NO_CONVERSIONS',
            impressions,
            clicks,
            cost,
            case when impressions > 0 then round(clicks::numeric / impressions, 4) else 0 end as ctr,
            case when clicks > 0 then round(cost / clicks, 2) else 0 end as avg_cpc,
            'Есть клики без результата: проверить интент запроса, посадочную страницу, соответствие оффера и отключить при повторении.',
            'high'
        from stg_direct_search_queries
        where date = %s
          and clicks >= 1
        """, (report_date,))

        # 3. Высокий CPC
        cur.execute("""
        insert into mart_direct_search_ai
        (date,campaign_name,ad_group_id,search_query,problem_type,impressions,clicks,cost,ctr,avg_cpc,recommendation,priority)
        select
            date,
            campaign_name,
            ad_group_id,
            search_query,
            'HIGH_CPC',
            impressions,
            clicks,
            cost,
            case when impressions > 0 then round(clicks::numeric / impressions, 4) else 0 end as ctr,
            case when clicks > 0 then round(cost / clicks, 2) else 0 end as avg_cpc,
            'Высокий CPC: проверить ставку, качество объявления, релевантность запроса и качество посадочной страницы.',
            'medium'
        from stg_direct_search_queries
        where date = %s
          and clicks > 0
          and (cost / clicks) > 80
        """, (report_date,))

        # 4. Кандидаты в минус-слова
        cur.execute("""
        insert into mart_direct_search_ai
        (date,campaign_name,ad_group_id,search_query,problem_type,impressions,clicks,cost,ctr,avg_cpc,recommendation,priority)
        select
            date,
            campaign_name,
            ad_group_id,
            search_query,
            'NEGATIVE_KEYWORD_CANDIDATE',
            impressions,
            clicks,
            cost,
            case when impressions > 0 then round(clicks::numeric / impressions, 4) else 0 end as ctr,
            case when clicks > 0 then round(cost / clicks, 2) else 0 end as avg_cpc,
            'Кандидат в минус-слова: запрос не похож на ваш продукт или может приводить нецелевой трафик. Проверить вручную.',
            'high'
        from stg_direct_search_queries
        where date = %s
          and (
              lower(search_query) similar to '%%(заброшенн|ваканс|скачать|бесплат|реферат|что это|страна оз|дубки|маяк|полушкино|инженерной школе|medical|медицинский)%%'
          )
        """, (report_date,))

        # 5. На масштабирование
        cur.execute("""
        insert into mart_direct_search_ai
        (date,campaign_name,ad_group_id,search_query,problem_type,impressions,clicks,cost,ctr,avg_cpc,recommendation,priority)
        select
            date,
            campaign_name,
            ad_group_id,
            search_query,
            'SCALE',
            impressions,
            clicks,
            cost,
            case when impressions > 0 then round(clicks::numeric / impressions, 4) else 0 end as ctr,
            case when clicks > 0 then round(cost / clicks, 2) else 0 end as avg_cpc,
            'Запрос показывает хороший CTR: можно повышать ставку или расширять похожие ключи после проверки качества трафика.',
            'medium'
        from stg_direct_search_queries
        where date = %s
          and impressions >= 3
          and (case when impressions > 0 then clicks::numeric / impressions else 0 end) >= 0.10
        """, (report_date,))

    return {"status": "ok", "date": report_date}

if __name__ == "__main__":
    print(run())
