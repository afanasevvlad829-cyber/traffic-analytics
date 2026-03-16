drop table if exists mart_ai_creative_candidates cascade;

create table mart_ai_creative_candidates (
    created_at timestamp default now(),
    campaign_name text,
    ad_id bigint,
    ad_group_id bigint,
    original_title text,
    score numeric,
    ctr numeric,
    cpc numeric,
    ai_title_1 text,
    ai_title_2 text,
    ai_title_3 text,
    decision text default 'PENDING'
);

create index idx_ai_creative_candidates_ad
on mart_ai_creative_candidates(ad_id);
