create table if not exists stg_direct_search_report (
    date date,
    campaign_id bigint,
    campaign_name text,
    ad_group_id bigint,
    ad_group_name text,
    search_query text,
    impressions bigint,
    clicks bigint,
    cost numeric(14,2),
    ctr numeric(8,4),
    avg_cpc numeric(14,2),
    loaded_at timestamp default now()
);

create table if not exists stg_direct_campaigns (
    campaign_id bigint primary key,
    campaign_name text,
    state text,
    status text,
    serving_status text,
    loaded_at timestamp default now()
);

create table if not exists stg_direct_adgroups (
    ad_group_id bigint primary key,
    campaign_id bigint,
    ad_group_name text,
    status text,
    serving_status text,
    loaded_at timestamp default now()
);

create table if not exists stg_direct_keywords (
    keyword_id bigint primary key,
    ad_group_id bigint,
    campaign_id bigint,
    keyword_text text,
    state text,
    status text,
    loaded_at timestamp default now()
);

create table if not exists stg_direct_ads (
    ad_id bigint primary key,
    ad_group_id bigint,
    campaign_id bigint,
    title text,
    title2 text,
    ad_text text,
    status text,
    state text,
    loaded_at timestamp default now()
);

create table if not exists mart_direct_search_ai (
    created_at timestamp default now(),
    date date,
    campaign_name text,
    ad_group_name text,
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
    search_query,
    problem_type,
    metric_value,
    recommendation,
    priority
from mart_direct_search_ai;
