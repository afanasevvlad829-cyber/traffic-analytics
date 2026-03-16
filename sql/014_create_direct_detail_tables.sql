create table if not exists stg_direct_search_detail (
    date date,
    campaign_name text,
    ad_group_name text,
    keyword text,
    search_query text,
    impressions bigint,
    clicks bigint,
    cost numeric(14,2),
    avg_cpc numeric(14,2),
    ctr numeric(8,4),
    conversions bigint,
    loaded_at timestamp default now()
);

create table if not exists mart_direct_search_ai (
    created_at timestamp default now(),
    date date,
    campaign_name text,
    ad_group_name text,
    keyword text,
    search_query text,
    problem_type text,
    metric_value numeric,
    recommendation text,
    priority text
);

drop view if exists vw_dashboard_direct_search_ai;

create view vw_dashboard_direct_search_ai as
select
    created_at,
    date,
    campaign_name,
    ad_group_name,
    keyword,
    search_query,
    problem_type,
    metric_value,
    recommendation,
    priority
from mart_direct_search_ai;
