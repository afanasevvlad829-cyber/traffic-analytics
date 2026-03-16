
drop view if exists vw_direct_ops_report cascade;

create view vw_direct_ops_report as
select
    q.task_id,
    q.campaign_name,
    q.search_query,
    q.decision,
    q.impressions,
    q.clicks,
    q.ctr_pct,
    q.avg_cpc,
    q.cost,
    q.priority_score,
    q.exact_action,
    q.forecast_text,
    q.competitor_domains,

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

    q.status,

    -- новые поля для аналитики
    q.account_avg_ctr_pct,
    q.account_avg_cpc,
    q.relevance_score,
    q.traffic_quality_score,

    -- мета объявления
    q.ad_group_id,
    q.ad_title

from mart_direct_ai_actions_v3 q;

