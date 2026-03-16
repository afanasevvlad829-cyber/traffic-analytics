drop view if exists vw_headline_pattern_lift_report cascade;

create table if not exists mart_headline_pattern_lift (
    calculated_at timestamp default now(),
    campaign_name text,
    pattern text,
    pattern_type text,         -- BIGRAM / TRIGRAM
    ads_count int,
    total_impressions bigint,
    total_clicks bigint,
    avg_ctr numeric,
    account_avg_ctr numeric,
    ctr_lift numeric,
    verdict text
);

create index if not exists idx_mart_headline_pattern_lift_campaign
    on mart_headline_pattern_lift(campaign_name);

create view vw_headline_pattern_lift_report as
select
    campaign_name,
    pattern,
    pattern_type,
    ads_count,
    total_impressions,
    total_clicks,
    round(avg_ctr * 100, 2) as avg_ctr_pct,
    round(account_avg_ctr * 100, 2) as account_avg_ctr_pct,
    round(ctr_lift * 100, 2) as ctr_lift_pct,
    verdict,
    calculated_at
from mart_headline_pattern_lift;
