create table if not exists mart_ai_ab_test_actions (
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

    action_type text default 'CREATE_AB_TEST',
    status text default 'PENDING',
    api_response text
);

create index if not exists idx_ai_ab_test_actions_ad
on mart_ai_ab_test_actions(ad_id);

create index if not exists idx_ai_ab_test_actions_status
on mart_ai_ab_test_actions(status);
