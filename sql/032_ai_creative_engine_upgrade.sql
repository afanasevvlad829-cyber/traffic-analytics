drop table if exists mart_ai_creative_candidates cascade;
drop table if exists mart_ai_ab_test_actions cascade;

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

    decision text default 'PENDING'   -- PENDING / APPROVED / IGNORED / EXECUTED
);

create index if not exists idx_ai_creative_candidates_ad
on mart_ai_creative_candidates(ad_id);

create table mart_ai_ab_test_actions (
    created_at timestamp default now(),
    executed_at timestamp,
    campaign_name text,
    ad_id bigint,
    ad_group_id bigint,

    source_title text,
    source_title_2 text,
    source_body_text text,

    new_title text,
    new_title_2 text,
    new_body_text text,

    action_type text default 'CREATE_AB_TEST',   -- CREATE_AB_TEST / PAUSE_OLD_AD
    status text default 'PENDING',               -- PENDING / DONE / ERROR
    api_response text
);

create index if not exists idx_ai_ab_test_actions_ad
on mart_ai_ab_test_actions(ad_id);
