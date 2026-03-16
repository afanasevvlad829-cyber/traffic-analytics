
------------------------------------------------------------
-- 1. БЕНЧМАРКИ АККАУНТА (динамические)
------------------------------------------------------------

drop table if exists mart_direct_account_benchmarks;

create table mart_direct_account_benchmarks as
select
    current_date as calculated_at,
    avg(ctr) as avg_ctr,
    avg(avg_cpc) as avg_cpc,
    percentile_cont(0.75) within group (order by ctr) as p75_ctr,
    percentile_cont(0.25) within group (order by ctr) as p25_ctr,
    percentile_cont(0.75) within group (order by avg_cpc) as p75_cpc
from mart_direct_search_ai
where impressions >= 10;

create index idx_benchmarks_date on mart_direct_account_benchmarks(calculated_at);


------------------------------------------------------------
-- 2. ВОЗРАСТ КАМПАНИИ
------------------------------------------------------------

drop view if exists vw_campaign_age;

create view vw_campaign_age as
select
    campaign_name,
    min(date) as first_seen_date,
    current_date - min(date) as campaign_age_days
from stg_direct_search_queries
group by campaign_name;


------------------------------------------------------------
-- 3. НОВАЯ ACTION QUEUE С ФИЛЬТРАМИ ЗРЕЛОСТИ
------------------------------------------------------------

drop table if exists mart_direct_action_queue_v2;

create table mart_direct_action_queue_v2 as
with b as (
    select * from mart_direct_account_benchmarks
    order by calculated_at desc
    limit 1
),

base as (
    select
        a.date,
        a.campaign_name,
        a.ad_group_id,
        a.search_query,
        a.impressions,
        a.clicks,
        a.cost,
        a.ctr,
        a.avg_cpc,
        a.problem_type,
        ca.campaign_age_days,

        case
            when a.problem_type = 'NEGATIVE_KEYWORD_CANDIDATE'
                then 'EXCLUDE'

            when a.problem_type = 'LOW_CTR'
                and a.impressions >= 50
                and ca.campaign_age_days >= 3
                then 'REWRITE'

            when a.problem_type = 'HIGH_CPC'
                and a.clicks >= 5
                then 'REWRITE'

            when a.problem_type = 'CLICKS_NO_CONVERSIONS'
                and a.clicks >= 3
                then 'LANDING_CHECK'

            when a.problem_type = 'SCALE'
                then 'SCALE'

            else 'KEEP_AND_TEST'
        end as action_type,

        case
            when a.problem_type = 'LOW_CTR'
                then 'CTR ниже среднего — переписать объявление'

            when a.problem_type = 'HIGH_CPC'
                then 'Высокий CPC — проверить релевантность ключа и объявления'

            when a.problem_type = 'CLICKS_NO_CONVERSIONS'
                then 'Клики без заявок — проверить посадочную'

            when a.problem_type = 'NEGATIVE_KEYWORD_CANDIDATE'
                then 'Кандидат в минус-слова'

            when a.problem_type = 'SCALE'
                then 'Можно масштабировать'

            else 'Наблюдать'
        end as recommendation

    from mart_direct_search_ai a
    left join vw_campaign_age ca
    on a.campaign_name = ca.campaign_name
)

select * from base;


------------------------------------------------------------
-- 4. PAYLOAD ДЛЯ НЕЙРОСЕТИ (копи-пейст)
------------------------------------------------------------

drop view if exists vw_direct_ai_prompt_payload;

create view vw_direct_ai_prompt_payload as
select
    date,
    campaign_name,
    action_type,
    recommendation,

    string_agg(
        concat(
            'Query: ', search_query,
            ' | impressions=', impressions,
            ' | clicks=', clicks,
            ' | ctr=', round(ctr*100,2),'%',
            ' | cpc=', avg_cpc
        ),
        E'\n'
    ) as payload_text

from mart_direct_action_queue_v2
group by
    date,
    campaign_name,
    action_type,
    recommendation;


------------------------------------------------------------
-- 5. VIEW ДЛЯ DATALENS
------------------------------------------------------------

drop view if exists vw_dashboard_paid_search_actions_v2;

create view vw_dashboard_paid_search_actions_v2 as
select
    date,
    campaign_name,
    search_query,
    impressions,
    clicks,
    ctr,
    avg_cpc,
    action_type,
    recommendation
from mart_direct_action_queue_v2;


