from datetime import date, timedelta
from src.db import db_cursor

def rebuild_ai_recommendations(report_date=None):
    if report_date is None:
        report_date = (date.today() - timedelta(days=1)).isoformat()

    with db_cursor() as (_, cur):
        cur.execute("delete from mart_direct_ai_recommendations where date = %s", (report_date,))

        # 1. Низкий CTR
        cur.execute("""
        insert into mart_direct_ai_recommendations
        (date, campaign_name, problem_type, metric_value, recommendation, priority)
        select
            date,
            campaign_name,
            'LOW_CTR',
            ctr,
            'Низкий CTR: переписать заголовки, добавить ключ в объявление, усилить оффер и проверить релевантность объявления запросу.',
            'high'
        from mart_direct_campaign_diagnostics
        where date = %s
          and impressions >= 100
          and ctr < 0.03
        """, (report_date,))

        # 2. Клики без лидов
        cur.execute("""
        insert into mart_direct_ai_recommendations
        (date, campaign_name, problem_type, metric_value, recommendation, priority)
        select
            date,
            campaign_name,
            'CLICKS_NO_LEADS',
            clicks,
            'Есть клики, но нет лидов: проверить посадочную страницу, оффер, формы, скорость сайта и соответствие запроса странице.',
            'high'
        from mart_direct_campaign_diagnostics
        where date = %s
          and clicks >= 10
          and leads = 0
        """, (report_date,))

        # 3. Высокий CPC
        cur.execute("""
        insert into mart_direct_ai_recommendations
        (date, campaign_name, problem_type, metric_value, recommendation, priority)
        select
            date,
            campaign_name,
            'HIGH_CPC',
            avg_cpc,
            'Высокий CPC: проверить качество объявления, релевантность ключей и посадочной, снизить ставки на слабых сегментах.',
            'medium'
        from mart_direct_campaign_diagnostics
        where date = %s
          and avg_cpc > 100
          and clicks > 0
        """, (report_date,))

        # 4. Дорогой лид
        cur.execute("""
        insert into mart_direct_ai_recommendations
        (date, campaign_name, problem_type, metric_value, recommendation, priority)
        select
            date,
            campaign_name,
            'EXPENSIVE_LEAD',
            cpl,
            'Лид дорогой: проверить сегментацию, оффер, объявления, минус-слова и конверсию страницы. Возможно, часть бюджета уходит в нецелевой спрос.',
            'high'
        from mart_direct_campaign_diagnostics
        where date = %s
          and cpl is not null
          and cpl > 500
        """, (report_date,))

        # 5. Масштабирование
        cur.execute("""
        insert into mart_direct_ai_recommendations
        (date, campaign_name, problem_type, metric_value, recommendation, priority)
        select
            date,
            campaign_name,
            'SCALE',
            romi,
            'Кампания выглядит сильной: можно осторожно масштабировать бюджет, но только после проверки поисковых запросов и качества лидов.',
            'medium'
        from mart_direct_campaign_diagnostics
        where date = %s
          and romi is not null
          and romi > 3
        """, (report_date,))

    return {"status": "ok", "date": report_date}

def run(report_date=None):
    return rebuild_ai_recommendations(report_date)

if __name__ == "__main__":
    print(run())
