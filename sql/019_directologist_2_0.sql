drop table if exists mart_direct_account_benchmarks_v2;
drop table if exists mart_direct_action_queue_v3;
drop view if exists vw_dashboard_paid_search_actions_v3;
drop view if exists vw_directologist_prompt_payload_v3;

------------------------------------------------------------
-- 1. БЕНЧМАРКИ АККАУНТА
------------------------------------------------------------
create table mart_direct_account_benchmarks_v2 as
with q as (
    select
        date,
        campaign_name,
        search_query,
        impressions,
        clicks,
        cost,
        case when impressions > 0 then clicks::numeric / impressions else 0 end as ctr,
        case when clicks > 0 then cost / clicks else 0 end as avg_cpc
    from stg_direct_search_queries
),
filtered as (
    select *
    from q
    where impressions >= 3
)
select
    current_date as calculated_at,
    round(avg(ctr), 4) as avg_ctr,
    round(percentile_cont(0.25) within group (order by ctr)::numeric, 4) as p25_ctr,
    round(percentile_cont(0.50) within group (order by ctr)::numeric, 4) as p50_ctr,
    round(percentile_cont(0.75) within group (order by ctr)::numeric, 4) as p75_ctr,
    round(avg(avg_cpc), 2) as avg_cpc,
    round(percentile_cont(0.25) within group (order by avg_cpc)::numeric, 2) as p25_cpc,
    round(percentile_cont(0.50) within group (order by avg_cpc)::numeric, 2) as p50_cpc,
    round(percentile_cont(0.75) within group (order by avg_cpc)::numeric, 2) as p75_cpc
from filtered;

------------------------------------------------------------
-- 2. ACTION QUEUE V3
------------------------------------------------------------
create table mart_direct_action_queue_v3 as
with b as (
    select *
    from mart_direct_account_benchmarks_v2
    order by calculated_at desc
    limit 1
),
campaign_age as (
    select
        campaign_name,
        min(date) as first_seen_date,
        current_date - min(date) as campaign_age_days
    from stg_direct_search_queries
    group by campaign_name
),
q as (
    select
        s.date,
        s.campaign_name,
        s.ad_group_id,
        s.search_query,
        s.impressions,
        s.clicks,
        s.cost,
        case when s.impressions > 0 then round(s.clicks::numeric / s.impressions, 4) else 0 end as ctr,
        case when s.clicks > 0 then round(s.cost / s.clicks, 2) else 0 end as avg_cpc,
        coalesce(ca.campaign_age_days, 0) as campaign_age_days,
        b.avg_ctr,
        b.p25_ctr,
        b.p50_ctr,
        b.p75_ctr,
        b.avg_cpc as account_avg_cpc,
        b.p75_cpc
    from stg_direct_search_queries s
    cross join b
    left join campaign_age ca
        on s.campaign_name = ca.campaign_name
),
classified as (
    select
        *,
        case
            when lower(search_query) similar to '%(страна оз|дубки|маяк|полушкино|заброшенн|инженерной школе|энергетик|медицинский лагерь|incamp|derel|practice camp|спутник)%'
                then 'EXCLUDE'

            when campaign_age_days < 3
                then 'WAIT_FOR_DATA'

            when clicks >= 1
                 and lower(search_query) similar to '%(детский лагерь|летний лагерь|лето 2026|лагерь для детей|айти летний лагерь|it лагерь|подмосковье)%'
                 and avg_cpc <= p75_cpc
                 and ctr >= p50_ctr
                then 'LANDING_CHECK'

            when impressions >= 20
                 and ctr < greatest(p25_ctr, avg_ctr * 0.6)
                then 'REWRITE_AD'

            when clicks >= 1
                 and avg_cpc > p75_cpc
                then 'LOWER_BID_OR_SPLIT'

            when impressions >= 3
                 and clicks >= 1
                 and ctr >= greatest(p50_ctr, avg_ctr)
                 and lower(search_query) similar to '%(детский лагерь|летний лагерь|лето 2026|лагерь для детей|айти летний лагерь|it лагерь|подмосковье)%'
                then 'SCALE'

            else 'KEEP_AND_TEST'
        end as decision
    from q
),
scored as (
    select
        *,
        case
            when decision = 'EXCLUDE' then 100
            when decision = 'LANDING_CHECK' then 90
            when decision = 'LOWER_BID_OR_SPLIT' then 85
            when decision = 'REWRITE_AD' then 80
            when decision = 'SCALE' then 70
            when decision = 'WAIT_FOR_DATA' then 20
            else 10
        end as priority_score
    from classified
)
select
    date,
    campaign_name,
    ad_group_id,
    search_query,
    impressions,
    clicks,
    cost,
    ctr,
    avg_cpc,
    campaign_age_days,
    avg_ctr as account_avg_ctr,
    p50_ctr as account_median_ctr,
    p75_cpc as account_p75_cpc,
    decision,

    case
        when decision = 'EXCLUDE'
            then 'Запрос не похож на ваш продукт или относится к чужому лагерю/бренду.'
        when decision = 'WAIT_FOR_DATA'
            then 'Кампания слишком новая, данных ещё мало для уверенного решения.'
        when decision = 'LANDING_CHECK'
            then 'Интент коммерческий, CTR уже нормальный, но запрос ещё не дал результата.'
        when decision = 'REWRITE_AD'
            then 'CTR заметно ниже нормального уровня по аккаунту.'
        when decision = 'LOWER_BID_OR_SPLIT'
            then 'Цена клика выше верхнего нормального диапазона по аккаунту.'
        when decision = 'SCALE'
            then 'Запрос выглядит сильным относительно аккаунта.'
        else 'Данных пока недостаточно для сильного решения.'
    end as reason,

    case
        when decision = 'EXCLUDE'
            then concat(
                'Исключить запрос/кластер. Факт: "', search_query, '". ',
                'Показы=', impressions, ', клики=', clicks, ', cost=', cost, ' ₽. ',
                'Действие: добавить ключевой паттерн в минус-слова на уровне кампании.'
            )

        when decision = 'WAIT_FOR_DATA'
            then concat(
                'Пока не трогать. Кампании ', campaign_age_days, ' дн., ',
                'показы=', impressions, ', клики=', clicks, '. ',
                'Действие: дождаться >=3 дней и накопления статистики.'
            )

        when decision = 'LANDING_CHECK'
            then concat(
                'Проверить посадочную именно под этот интент. Запрос "', search_query, '". ',
                'CTR=', round(ctr*100,2), '% при медиане аккаунта ', round(account_median_ctr*100,2), '%. ',
                'CPC=', avg_cpc, ' ₽. ',
                'Действие: вести на страницу "лагерь / Подмосковье / лето 2026", проверить форму и первый экран.'
            )

        when decision = 'REWRITE_AD'
            then concat(
                'Переписать объявление. CTR=', round(ctr*100,2), '% против среднего по аккаунту ',
                round(account_avg_ctr*100,2), '%. ',
                'Показы=', impressions, '. ',
                'Действие: вынести запрос в отдельную группу, вставить точный интент в заголовок и оффер.'
            )

        when decision = 'LOWER_BID_OR_SPLIT'
            then concat(
                'Снизить ставку или вынести в отдельную дорогую группу. ',
                'CPC=', avg_cpc, ' ₽ при p75 аккаунта ', account_p75_cpc, ' ₽. ',
                'Действие: снизить ставку на 15–30% и проверить, не слишком ли широкий интент.'
            )

        when decision = 'SCALE'
            then concat(
                'Можно усиливать. Запрос "', search_query, '". ',
                'CTR=', round(ctr*100,2), '% против среднего ', round(account_avg_ctr*100,2), '%. ',
                'CPC=', avg_cpc, ' ₽. ',
                'Действие: оставить, поднять ставку осторожно или сделать отдельное объявление под этот запрос.'
            )

        else concat(
            'Оставить под наблюдением. Показы=', impressions,
            ', клики=', clicks, ', CTR=', round(ctr*100,2), '%.'
        )
    end as exact_action,

    priority_score
from scored
order by date desc, priority_score desc, cost desc;

------------------------------------------------------------
-- 3. VIEW
------------------------------------------------------------
create view vw_dashboard_paid_search_actions_v3 as
select
    date,
    campaign_name,
    ad_group_id,
    search_query,
    impressions,
    clicks,
    round(ctr * 100, 2) as ctr_pct,
    avg_cpc,
    cost,
    campaign_age_days,
    round(account_avg_ctr * 100, 2) as account_avg_ctr_pct,
    round(account_median_ctr * 100, 2) as account_median_ctr_pct,
    account_p75_cpc,
    decision,
    reason,
    exact_action,
    priority_score
from mart_direct_action_queue_v3;

------------------------------------------------------------
-- 4. LLM PAYLOAD
------------------------------------------------------------
create view vw_directologist_prompt_payload_v3 as
select
    date,
    campaign_name,
    decision,
    string_agg(
        concat(
            'Запрос: ', search_query,
            ' | показы=', impressions,
            ' | клики=', clicks,
            ' | ctr=', round(ctr*100,2), '%',
            ' | cpc=', avg_cpc, ' ₽',
            ' | решение=', decision,
            ' | действие=', exact_action
        ),
        E'\n'
        order by priority_score desc, cost desc
    ) as payload_text
from mart_direct_action_queue_v3
group by
    date,
    campaign_name,
    decision;
