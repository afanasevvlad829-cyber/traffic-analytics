drop view if exists vw_direct_ops_report cascade;

create view vw_direct_ops_report as
select
    q.task_id,
    q.date,
    q.campaign_name,
    q.search_query,
    q.decision,
    q.status,

    t.ad_group_id,

    coalesce(cs.ad_title, '(no title from API)') as ad_title,

    q.impressions,
    q.clicks,
    round(coalesce(q.ctr, 0) * 100, 2) as ctr_pct,
    q.avg_cpc,
    q.cost,
    q.priority_score,

    q.exact_action,
    q.forecast_text,
    q.competitor_domains,

    round(coalesce(t.account_avg_ctr, 0) * 100, 2) as account_avg_ctr_pct,
    round(coalesce(cs.account_avg_cpc, 0), 2) as account_avg_cpc,
    round(coalesce(cs.relevance_score, 40), 2) as relevance_score,
    round(coalesce(cs.traffic_quality_score, 50), 2) as traffic_quality_score,

    q.ai_title_1,
    q.ai_title_2,
    q.ai_text,

    q.ai_title_1_b,
    q.ai_title_2_b,
    q.ai_text_b,

    q.ai_title_1_c,
    q.ai_title_2_c,
    q.ai_text_c,

    q.minus_words,

    q.created_at,
    q.completed_at,
    q.completed_by

from mart_direct_ai_actions_v3 q
left join mart_direct_task_queue t
    on q.task_id = t.task_id
left join mart_direct_creative_score cs
    on cs.campaign_name = q.campaign_name
   and cs.ad_group_id = t.ad_group_id;
