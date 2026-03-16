drop table if exists mart_group_builder cascade;
drop table if exists mart_ai_creative_candidates cascade;

create table mart_group_builder (
    created_at timestamp default now(),
    campaign_name text,
    ad_group_id bigint,
    queries text,
    recommendation text
);

create index idx_group_builder_group
on mart_group_builder(ad_group_id);

create table mart_ai_creative_candidates (
    created_at timestamp default now(),
    campaign_name text,
    ad_id bigint,
    ad_group_id bigint,

    original_title text,
    original_title_2 text,
    original_body_text text,
    sample_queries text,

    score numeric,
    ctr numeric,
    cpc numeric,

    ai_title_1 text,
    ai_title_2 text,
    ai_body_1 text,

    ai_title_1_b text,
    ai_title_2_b text,
    ai_body_2 text,

    ai_title_1_c text,
    ai_title_2_c text,
    ai_body_3 text,

    predicted_ctr_pct numeric,
    predicted_cpc numeric,
    predicted_relevance numeric,
    prediction_confidence text,
    prediction_reason text,

    decision text default 'PENDING'
);

create index idx_ai_creative_candidates_ad
on mart_ai_creative_candidates(ad_id);
