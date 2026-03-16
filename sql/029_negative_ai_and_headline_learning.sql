drop view if exists vw_campaign_negative_keywords_ai_safe_copy_paste cascade;
drop view if exists vw_campaign_negative_keywords_ai_blocked_copy_paste cascade;
drop view if exists vw_headline_word_lift_report cascade;

create table if not exists mart_campaign_negative_keywords_ai (
    created_at timestamp default now(),
    report_date date,
    campaign_name text,
    candidate_word text,
    decision text,              -- SAFE_NEGATIVE / BLOCKED_NEGATIVE
    rationale text
);

create index if not exists idx_mart_campaign_negative_keywords_ai_date
    on mart_campaign_negative_keywords_ai(report_date);

create index if not exists idx_mart_campaign_negative_keywords_ai_campaign
    on mart_campaign_negative_keywords_ai(campaign_name);

create table if not exists mart_headline_word_lift (
    calculated_at timestamp default now(),
    campaign_name text,
    word text,
    ads_count int,
    total_impressions bigint,
    total_clicks bigint,
    avg_ctr numeric,
    account_avg_ctr numeric,
    ctr_lift numeric,
    verdict text
);

create index if not exists idx_mart_headline_word_lift_campaign
    on mart_headline_word_lift(campaign_name);

create view vw_campaign_negative_keywords_ai_safe_copy_paste as
select
    campaign_name,
    string_agg(candidate_word, ', ' order by candidate_word) as safe_negative_keywords_copy_paste,
    count(*) as keywords_count
from (
    select distinct campaign_name, candidate_word
    from mart_campaign_negative_keywords_ai
    where report_date = current_date
      and decision = 'SAFE_NEGATIVE'
) s
group by campaign_name
order by campaign_name;

create view vw_campaign_negative_keywords_ai_blocked_copy_paste as
select
    campaign_name,
    string_agg(candidate_word, ', ' order by candidate_word) as blocked_negative_keywords_copy_paste,
    count(*) as keywords_count
from (
    select distinct campaign_name, candidate_word
    from mart_campaign_negative_keywords_ai
    where report_date = current_date
      and decision = 'BLOCKED_NEGATIVE'
) s
group by campaign_name
order by campaign_name;

create view vw_headline_word_lift_report as
select
    campaign_name,
    word,
    ads_count,
    total_impressions,
    total_clicks,
    round(avg_ctr * 100, 2) as avg_ctr_pct,
    round(account_avg_ctr * 100, 2) as account_avg_ctr_pct,
    round(ctr_lift * 100, 2) as ctr_lift_pct,
    verdict,
    calculated_at
from mart_headline_word_lift;
