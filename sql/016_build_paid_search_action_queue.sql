drop table if exists mart_direct_action_queue;
drop view if exists vw_dashboard_paid_search_actions;

create table mart_direct_action_queue as

with base as (
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
        problem_type,
        recommendation,
        priority,

        case
            when lower(search_query) similar to '%(страна оз|дубки|маяк|полушкино|заброшенн|инженерной школе|медицинский лагерь|энергетик)%'
                then 'EXCLUDE'
            when lower(search_query) similar to '%(детский лагерь в подмосковье|летний лагерь в подмосковье|лето 2026|лагерь для детей|айти летний лагерь|it лагерь)%'
                 and problem_type in ('SCALE')
                then 'SCALE'
            when problem_type in ('HIGH_CPC','LOW_CTR')
                then 'REWRITE'
            when problem_type in ('CLICKS_NO_CONVERSIONS')
                then 'LANDING_CHECK'
            else 'KEEP_AND_TEST'
        end as action_type,

        case
            when lower(search_query) similar to '%(страна оз|дубки|маяк|полушкино|заброшенн|инженерной школе|медицинский лагерь|энергетик)%'
                then 'Добавить в минус-слова или исключить группу запросов'
            when lower(search_query) similar to '%(детский лагерь в подмосковье|летний лагерь в подмосковье|лето 2026|лагерь для детей|айти летний лагерь|it лагерь)%'
                 and problem_type in ('SCALE')
                then 'Оставить в работе, поднять ставку осторожно, вынести в отдельную группу/объявление'
            when problem_type = 'HIGH_CPC'
                then 'Снизить ставку или сузить охват, проверить соответствие объявления и страницы'
            when problem_type = 'LOW_CTR'
                then 'Переписать заголовок/текст объявления, добавить точный интент в оффер'
            when problem_type = 'CLICKS_NO_CONVERSIONS'
                then 'Проверить посадочную, оффер, форму, соответствие запросу; при повторении отключить'
            else 'Оставить под наблюдением'
        end as action_note,

        case
            when lower(search_query) similar to '%(страна оз|дубки|маяк|полушкино|заброшенн|инженерной школе|медицинский лагерь|энергетик)%'
                then 100
            when problem_type = 'CLICKS_NO_CONVERSIONS' and clicks >= 1 and cost >= 80
                then 90
            when problem_type = 'HIGH_CPC' and avg_cpc >= 100
                then 80
            when problem_type = 'LOW_CTR'
                then 70
            when problem_type = 'SCALE'
                then 60
            else 10
        end as priority_score
    from mart_direct_search_ai
),

ranked as (
    select *,
           row_number() over (
               partition by date, campaign_name, search_query
               order by priority_score desc, cost desc, clicks desc
           ) as rn
    from base
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
    problem_type,
    action_type,
    action_note,
    priority,
    priority_score
from ranked
where rn = 1
order by date desc, priority_score desc, cost desc;

create view vw_dashboard_paid_search_actions as
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
    problem_type,
    action_type,
    action_note,
    priority,
    priority_score
from mart_direct_action_queue;
