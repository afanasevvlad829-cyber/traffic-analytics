-- Phase 4 extension: DB-backed cache + mandatory failure persistence metadata.
-- Additive / idempotent.

alter table if exists audit_runs add column if not exists source text;
alter table if exists audit_runs add column if not exists cache_hit boolean not null default false;
alter table if exists audit_runs add column if not exists cache_key text;
alter table if exists audit_runs add column if not exists prompt_version text;
alter table if exists audit_runs add column if not exists input_hash text;
alter table if exists audit_runs add column if not exists context_hash text;
alter table if exists audit_runs add column if not exists raw_response_json jsonb;
alter table if exists audit_runs add column if not exists last_error_type text;
alter table if exists audit_runs add column if not exists last_error_text text;
alter table if exists audit_runs add column if not exists retryable boolean;
alter table if exists audit_runs add column if not exists transport_status_code integer;
alter table if exists audit_runs add column if not exists external_status text;

create index if not exists idx_audit_runs_cache_key on audit_runs(cache_key);
create index if not exists idx_audit_runs_source_created_at on audit_runs(source, created_at desc);

create table if not exists audit_cache (
    id bigserial primary key,
    cache_key text not null unique,
    project_id text not null,
    audit_level text not null,
    prompt_version text not null,
    input_hash text not null,
    context_hash text not null,
    response_json jsonb not null,
    response_markdown text,
    overall_score numeric,
    verdict text,
    created_at timestamptz not null default now(),
    expires_at timestamptz,
    hit_count bigint not null default 0
);

create index if not exists idx_audit_cache_project_level on audit_cache(project_id, audit_level, created_at desc);
create index if not exists idx_audit_cache_expires_at on audit_cache(expires_at);

create table if not exists audit_issues (
    id bigserial primary key,
    audit_run_id bigint not null references audit_runs(id) on delete cascade,
    severity text,
    category text,
    title text not null,
    description text,
    recommended_action text,
    status text not null default 'new',
    linked_task_id bigint,
    fix_verification_status text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_audit_issues_run on audit_issues(audit_run_id);
create index if not exists idx_audit_issues_status on audit_issues(status, created_at desc);
