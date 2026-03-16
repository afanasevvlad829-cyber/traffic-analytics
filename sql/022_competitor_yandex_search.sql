create table if not exists stg_competitor_serp_daily (
    collected_at timestamp default now(),
    report_date date,
    keyword text,
    result_type text,
    position int,
    domain text,
    url text,
    title text,
    headline text,
    passage text,
    raw_xml text
);

create table if not exists mart_competitor_serp_alerts (
    created_at timestamp default now(),
    report_date date,
    keyword text,
    result_type text,
    position int,
    domain text,
    url text,
    title text,
    relevance_score numeric,
    verdict text,
    action_note text
);

drop view if exists vw_competitor_serp_alerts;

create view vw_competitor_serp_alerts as
select
    report_date,
    keyword,
    result_type,
    position,
    domain,
    url,
    title,
    round(relevance_score, 2) as relevance_score,
    verdict,
    action_note
from mart_competitor_serp_alerts;
