drop view if exists vw_direct_ops_report;
drop table if exists mart_direct_ai_actions_v3;

create table mart_direct_ai_actions_v3 as
with tasks as (
    select
        t.task_id,
        t.date,
        t.campaign_name,
        t.search_query,
        t.decision,
        t.exact_action,
        t.reason,
        t.impressions,
        t.clicks,
        t.cost,
        t.ctr,
        t.avg_cpc,
        t.account_avg_ctr,
        t.account_median_ctr,
        t.account_p75_cpc,
        t.priority_score,
        t.status,
        t.created_at,
        t.completed_at,
        t.completed_by,
        t.forecast_target_ctr_pct,
        t.forecast_target_cpc,
        t.forecast_cost_effect
    from mart_direct_task_queue t
),
ai as (
    select distinct on (campaign_name, search_query, decision)
        campaign_name,
        search_query,
        decision,
        ai_title_1,
        ai_title_2,
        ai_text,
        ai_title_1_b,
        ai_title_2_b,
        ai_text_b,
        ai_title_1_c,
        ai_title_2_c,
        ai_text_c,
        minus_words,
        raw_llm_response,
        created_at as ai_created_at
    from mart_direct_ai_creatives
    order by campaign_name, search_query, decision, created_at desc
),
comp as (
    select
        keyword,
        max(relevance_score) as top_competitor_score,
        string_agg(distinct domain, ', ' order by domain) as competitor_domains
    from mart_competitor_serp_alerts
    where report_date = current_date
    group by keyword
)
select
    t.*,
    a.ai_title_1,
    a.ai_title_2,
    a.ai_text,
    a.ai_title_1_b,
    a.ai_title_2_b,
    a.ai_text_b,
    a.ai_title_1_c,
    a.ai_title_2_c,
    a.ai_text_c,
    a.minus_words,
    a.raw_llm_response,
    a.ai_created_at,
    c.top_competitor_score,
    c.competitor_domains,

    case
        when t.decision = 'REWRITE_AD' then
            concat(
                'Прогноз: CTR ',
                coalesce(round((t.ctr * 100)::numeric,2)::text, '0'),
                '% → ',
                coalesce(t.forecast_target_ctr_pct::text, 'n/a'),
                '%, CPC ',
                coalesce(round(t.avg_cpc::numeric,2)::text, '0'),
                ' ₽ → ',
                coalesce(t.forecast_target_cpc::text, 'n/a'),
                ' ₽.'
            )
        when t.decision = 'LOWER_BID_OR_SPLIT' then
            concat(
                'Прогноз: CPC ',
                coalesce(round(t.avg_cpc::numeric,2)::text, '0'),
                ' ₽ → ',
                coalesce(t.forecast_target_cpc::text, 'n/a'),
                ' ₽.'
            )
        when t.decision = 'EXCLUDE' then
            concat(
                'Прогноз: убрать слив минимум на ',
                coalesce(t.forecast_cost_effect::text, '0'),
                ' ₽ по уже замеченному мусорному спросу.'
            )
        when t.decision = 'SCALE' then
            concat(
                'Прогноз: удержать/поднять CTR до ',
                coalesce(t.forecast_target_ctr_pct::text, 'n/a'),
                '%.'
            )
        else
            'Прогноз: проверить гипотезу и пересчитать через 1–2 дня.'
    end as forecast_text
from tasks t
left join ai a
    on a.campaign_name = t.campaign_name
   and a.search_query = t.search_query
   and a.decision = t.decision
left join comp c
    on c.keyword = t.search_query;

create view vw_direct_ops_report as
select
    task_id,
    date,
    campaign_name,
    search_query,
    decision,
    status,
    impressions,
    clicks,
    round(ctr * 100, 2) as ctr_pct,
    avg_cpc,
    cost,
    priority_score,
    exact_action,
    forecast_text,
    competitor_domains,
    round(top_competitor_score, 2) as top_competitor_score,
    ai_title_1,
    ai_title_2,
    ai_text,
    ai_title_1_b,
    ai_title_2_b,
    ai_text_b,
    ai_title_1_c,
    ai_title_2_c,
    ai_text_c,
    minus_words,
    created_at,
    completed_at,
    completed_by
from mart_direct_ai_actions_v3;
