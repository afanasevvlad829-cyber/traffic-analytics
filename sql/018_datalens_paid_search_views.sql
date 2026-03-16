------------------------------------------------------------
-- 1. ДЕТАЛЬНАЯ ТАБЛИЦА ДЕЙСТВИЙ
------------------------------------------------------------
drop view if exists vw_dashboard_paid_search_actions_final;

create view vw_dashboard_paid_search_actions_final as
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
    action_type,
    recommendation,
    priority
from mart_direct_action_queue_v2;


------------------------------------------------------------
-- 2. СВОДКА ПО ТИПАМ ДЕЙСТВИЙ
------------------------------------------------------------
drop view if exists vw_dashboard_paid_search_summary;

create view vw_dashboard_paid_search_summary as
select
    date,
    campaign_name,
    action_type,
    count(*) as queries_count,
    sum(impressions) as impressions_total,
    sum(clicks) as clicks_total,
    round(avg(ctr) * 100, 2) as avg_ctr_pct,
    round(avg(avg_cpc), 2) as avg_cpc,
    round(sum(cost), 2) as cost_total
from mart_direct_action_queue_v2
group by
    date,
    campaign_name,
    action_type;


------------------------------------------------------------
-- 3. TOP QUERY ПРОБЛЕМЫ
------------------------------------------------------------
drop view if exists vw_dashboard_paid_search_top_issues;

create view vw_dashboard_paid_search_top_issues as
select
    date,
    campaign_name,
    search_query,
    action_type,
    recommendation,
    impressions,
    clicks,
    round(ctr * 100, 2) as ctr_pct,
    avg_cpc,
    cost
from mart_direct_action_queue_v2
where action_type in ('EXCLUDE', 'REWRITE', 'LANDING_CHECK')
order by date desc, cost desc, clicks desc;


------------------------------------------------------------
-- 4. SCALE КАНДИДАТЫ
------------------------------------------------------------
drop view if exists vw_dashboard_paid_search_scale_candidates;

create view vw_dashboard_paid_search_scale_candidates as
select
    date,
    campaign_name,
    search_query,
    impressions,
    clicks,
    round(ctr * 100, 2) as ctr_pct,
    avg_cpc,
    cost,
    recommendation
from mart_direct_action_queue_v2
where action_type = 'SCALE'
order by date desc, ctr desc, impressions desc;


------------------------------------------------------------
-- 5. PAYLOAD ДЛЯ COPY-PASTE В НЕЙРОСЕТЬ
------------------------------------------------------------
drop view if exists vw_dashboard_paid_search_prompt_payload;

create view vw_dashboard_paid_search_prompt_payload as
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
            ' | ctr=', round(ctr * 100, 2), '%',
            ' | cpc=', avg_cpc,
            ' | cost=', cost
        ),
        E'\n'
        order by cost desc, clicks desc
    ) as payload_text
from mart_direct_action_queue_v2
group by
    date,
    campaign_name,
    action_type,
    recommendation;
