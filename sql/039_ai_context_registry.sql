create table if not exists ai_context_registry (
    id bigserial primary key,
    context_code text unique,
    context_type text,
    entity_id text,
    campaign_name text,
    payload_json jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now()
);

create index if not exists idx_ai_context_code
on ai_context_registry(context_code);

create index if not exists idx_ai_context_type
on ai_context_registry(context_type);
