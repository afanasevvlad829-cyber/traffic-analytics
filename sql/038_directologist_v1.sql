create table if not exists mart_ai_ab_test_actions (
    id bigserial primary key,
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
    api_response text,
    requested_by text default 'webapp'
);

create index if not exists idx_ai_ab_test_actions_ad on mart_ai_ab_test_actions(ad_id);
create index if not exists idx_ai_ab_test_actions_status on mart_ai_ab_test_actions(status);

create table if not exists mart_negative_actions (
    id bigserial primary key,
    created_at timestamp default now(),
    executed_at timestamp,
    campaign_name text,
    words_text text,
    keywords_count int,
    action_type text default 'APPLY_SAFE_NEGATIVES',
    status text default 'PENDING',
    api_response text,
    requested_by text default 'webapp'
);

create index if not exists idx_negative_actions_status on mart_negative_actions(status);

create table if not exists mart_structure_actions (
    id bigserial primary key,
    created_at timestamp default now(),
    executed_at timestamp,
    campaign_name text,
    ad_group_id bigint,
    recommendation text,
    action_type text default 'APPLY_SPLIT',
    status text default 'PENDING',
    api_response text,
    requested_by text default 'webapp'
);

create index if not exists idx_structure_actions_status on mart_structure_actions(status);
create index if not exists idx_structure_actions_group on mart_structure_actions(ad_group_id);

create table if not exists ui_decision_log (
    id bigserial primary key,
    created_at timestamp default now(),
    entity_type text,
    entity_key text,
    action text,
    status text,
    details text,
    actor text default 'webapp'
);

create index if not exists idx_ui_decision_log_created_at on ui_decision_log(created_at desc);

create table if not exists snoozed_items (
    id bigserial primary key,
    created_at timestamp default now(),
    entity_type text,
    entity_key text,
    snoozed_until timestamp,
    reason text,
    actor text default 'webapp'
);

create index if not exists idx_snoozed_items_entity on snoozed_items(entity_type, entity_key);
create index if not exists idx_snoozed_items_until on snoozed_items(snoozed_until);

alter table if exists mart_ai_creative_candidates
    add column if not exists decision text default 'PENDING';
