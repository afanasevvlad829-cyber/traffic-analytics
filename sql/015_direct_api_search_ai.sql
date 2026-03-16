drop view if exists vw_dashboard_direct_search_ai;

drop table if exists mart_direct_search_ai;
drop table if exists stg_direct_search_queries;
drop table if exists stg_direct_ads_meta;

create table stg_direct_search_queries (
    date date,
    campaign_id bigint,
    campaign_name text,
    ad_group_id bigint,
    search_query text,
    impressions bigint,
    clicks bigint,
    cost numeric(14,2),
    loaded_at timestamp default now()
);

create index idx_stg_direct_search_queries_date on stg_direct_search_queries(date);
create index idx_stg_direct_search_queries_campaign on stg_direct_search_queries(campaign_name);
create index idx_stg_direct_search_queries_group on stg_direct_search_queries(ad_group_id);

create table stg_direct_ads_meta (
    ad_id bigint,
    ad_group_id bigint,
    campaign_id bigint,
    title text,
    title2 text,
    body_text text,
    href text,
    loaded_at timestamp default now()
);

create index idx_stg_direct_ads_meta_group on stg_direct_ads_meta(ad_group_id);

create table mart_direct_search_ai (
    created_at timestamp default now(),
    date date,
    campaign_name text,
    ad_group_id bigint,
    search_query text,
    problem_type text,
    impressions bigint,
    clicks bigint,
    cost numeric(14,2),
    ctr numeric(8,4),
    avg_cpc numeric(14,2),
    recommendation text,
    priority text
);

create index idx_mart_direct_search_ai_date on mart_direct_search_ai(date);
create index idx_mart_direct_search_ai_problem on mart_direct_search_ai(problem_type);

create view vw_dashboard_direct_search_ai as
select
    created_at,
    date,
    campaign_name,
    ad_group_id,
    search_query,
    problem_type,
    impressions,
    clicks,
    cost,
    ctr,
    avg_cpc,
    recommendation,
    priority
from mart_direct_search_ai;
