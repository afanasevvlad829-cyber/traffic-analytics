drop view if exists vw_creative_score_report cascade;
drop table if exists mart_direct_creative_score cascade;

create table mart_direct_creative_score as
with bench as (
    select *
    from mart_direct_account_benchmarks_v2
    limit 1
),

ads_meta as (
    select
        ad_group_id,
        max(campaign_id) as campaign_id,
        max(coalesce(nullif(title, ''), '(no title from API)')) as ad_title,
        max(coalesce(nullif(title2, ''), '')) as ad_title_2,
        max(coalesce(nullif(href, ''), '')) as href
    from stg_direct_ads_meta
    group by ad_group_id
),

query_agg as (
    select
        s.date,
        s.campaign_name,
        s.ad_group_id,
        sum(s.impressions) as impressions,
        sum(s.clicks) as clicks,
        round(sum(s.cost), 2) as cost,
        case when sum(s.impressions) > 0 then round(sum(s.clicks)::numeric / sum(s.impressions), 4) else 0 end as ctr,
        case when sum(s.clicks) > 0 then round(sum(s.cost) / sum(s.clicks), 2) else 0 end as avg_cpc
    from stg_direct_search_queries s
    group by s.date, s.campaign_name, s.ad_group_id
),

decision_agg as (
    select
        campaign_name,
        ad_group_id,
        count(*) as total_queries,
        count(*) filter (where decision = 'EXCLUDE') as exclude_queries,
        count(*) filter (where decision = 'REWRITE_AD') as rewrite_queries,
        count(*) filter (where decision = 'LANDING_CHECK') as landing_queries,
        count(*) filter (where decision = 'LOWER_BID_OR_SPLIT') as expensive_queries,
        count(*) filter (where decision = 'SCALE') as scale_queries,
        string_agg(distinct search_query, ' | ' order by search_query) as sample_queries
    from mart_direct_action_queue_v3
    group by campaign_name, ad_group_id
),

relevance_calc as (
    select
        d.campaign_name,
        d.ad_group_id,
        avg(
            case
                when coalesce(m.ad_title, '') = '' then 40
                else
                    least(
                        100,
                        20
                        + 40 * (
                            case
                                when lower(d.search_query) like '%' || lower(split_part(coalesce(m.ad_title,''), ' ', 1)) || '%'
                                then 1 else 0 end
                        )
                        + 20 * (
                            case
                                when lower(coalesce(m.ad_title,'')) like '%лагерь%' then 1 else 0 end
                        )
                        + 20 * (
                            case
                                when lower(coalesce(m.ad_title,'')) like '%подмосков%' then 1 else 0 end
                        )
                    )
            end
        ) as relevance_score
    from mart_direct_action_queue_v3 d
    left join ads_meta m
      on d.ad_group_id = m.ad_group_id
    group by d.campaign_name, d.ad_group_id
),

base as (
    select
        q.date,
        q.campaign_name,
        q.ad_group_id,
        coalesce(m.ad_title, '(no title from API)') as ad_title,
        coalesce(m.ad_title_2, '') as ad_title_2,
        coalesce(m.href, '') as href,
        q.impressions,
        q.clicks,
        q.cost,
        q.ctr,
        q.avg_cpc,
        coalesce(d.total_queries, 0) as total_queries,
        coalesce(d.exclude_queries, 0) as exclude_queries,
        coalesce(d.rewrite_queries, 0) as rewrite_queries,
        coalesce(d.landing_queries, 0) as landing_queries,
        coalesce(d.expensive_queries, 0) as expensive_queries,
        coalesce(d.scale_queries, 0) as scale_queries,
        coalesce(d.sample_queries, '') as sample_queries,
        coalesce(r.relevance_score, 40) as relevance_score,
        b.avg_ctr as account_avg_ctr,
        b.p50_ctr as account_median_ctr,
        b.avg_cpc as account_avg_cpc,
        b.p75_cpc as account_p75_cpc
    from query_agg q
    cross join bench b
    left join ads_meta m
      on q.ad_group_id = m.ad_group_id
    left join decision_agg d
      on q.campaign_name = d.campaign_name
     and q.ad_group_id = d.ad_group_id
    left join relevance_calc r
      on q.campaign_name = r.campaign_name
     and q.ad_group_id = r.ad_group_id
),

scored as (
    select
        *,
        least(120, round(100 * ctr / nullif(account_avg_ctr, 0.0001), 2)) as ctr_score,
        least(120, round(100 * account_avg_cpc / nullif(avg_cpc, 0.01), 2)) as cpc_score,
        round(
            100
            - least(
                100,
                case
                    when total_queries > 0 then (exclude_queries::numeric / total_queries) * 100
                    else 0
                end
            ),
            2
        ) as traffic_quality_score
    from base
),

final as (
    select
        *,
        round(
            0.35 * greatest(0, least(100, ctr_score)) +
            0.20 * greatest(0, least(100, cpc_score)) +
            0.25 * greatest(0, least(100, relevance_score)) +
            0.20 * greatest(0, least(100, traffic_quality_score))
        , 2) as creative_score
    from scored
)

select
    date,
    campaign_name,
    ad_group_id,
    ad_title,
    ad_title_2,
    href,
    impressions,
    clicks,
    cost,
    ctr,
    avg_cpc,
    total_queries,
    exclude_queries,
    rewrite_queries,
    landing_queries,
    expensive_queries,
    scale_queries,
    sample_queries,
    account_avg_ctr,
    account_median_ctr,
    account_avg_cpc,
    account_p75_cpc,
    relevance_score,
    ctr_score,
    cpc_score,
    traffic_quality_score,
    creative_score,

    case
        when creative_score >= 85 then 'STRONG'
        when creative_score >= 70 then 'NORMAL'
        when creative_score >= 50 then 'WEAK'
        else 'BAD'
    end as creative_grade,

    case
        when creative_score < 50 then 'REWRITE_NOW'
        when creative_score < 70 then 'REWRITE_TEST'
        when creative_score >= 85 and scale_queries > 0 then 'SCALE'
        else 'KEEP'
    end as recommended_action
from final
order by date desc, creative_score asc, cost desc;

create view vw_creative_score_report as
select
    date,
    campaign_name,
    ad_group_id,
    ad_title,
    ad_title_2,
    href,
    impressions,
    clicks,
    round(ctr * 100, 2) as ctr_pct,
    avg_cpc,
    round(cost, 2) as cost,
    round(account_avg_ctr * 100, 2) as account_avg_ctr_pct,
    round(account_median_ctr * 100, 2) as account_median_ctr_pct,
    round(account_avg_cpc, 2) as account_avg_cpc,
    round(account_p75_cpc, 2) as account_p75_cpc,
    round(relevance_score, 2) as relevance_score,
    round(ctr_score, 2) as ctr_score,
    round(cpc_score, 2) as cpc_score,
    round(traffic_quality_score, 2) as traffic_quality_score,
    round(creative_score, 2) as creative_score,
    creative_grade,
    recommended_action,
    total_queries,
    exclude_queries,
    rewrite_queries,
    landing_queries,
    expensive_queries,
    scale_queries,
    sample_queries
from mart_direct_creative_score;
