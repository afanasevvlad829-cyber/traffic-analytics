-- OpenRouter audit worker reliability foundation.
-- Additive and idempotent.

create table if not exists audit_runs (
    id bigserial primary key,
    project_id text not null,
    branch text not null,
    stage text not null,
    audit_level text not null,
    status text not null default 'pending',
    overall_score numeric,
    architecture_score numeric,
    code_hygiene_score numeric,
    scalability_score numeric,
    production_readiness_score numeric,
    verdict text,
    can_proceed boolean,
    top_risks_json jsonb not null default '[]'::jsonb,
    required_fixes_json jsonb not null default '[]'::jsonb,
    strengths_json jsonb not null default '[]'::jsonb,
    changed_modules_json jsonb not null default '[]'::jsonb,
    report_markdown text,
    response_json jsonb,
    source_report_path text,
    attempt_count integer not null default 0,
    last_error text,
    latency_ms integer,
    last_attempt_at timestamptz,
    retryable_failure boolean not null default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_audit_runs_status_created_at on audit_runs(status, created_at desc);
create index if not exists idx_audit_runs_project_branch on audit_runs(project_id, branch, created_at desc);
create index if not exists idx_audit_runs_last_attempt_at on audit_runs(last_attempt_at desc);
create index if not exists idx_audit_runs_retryable_failure on audit_runs(retryable_failure);

-- Upgrade existing audit_runs if table already exists from previous iterations.
alter table if exists audit_runs add column if not exists attempt_count integer not null default 0;
alter table if exists audit_runs add column if not exists last_error text;
alter table if exists audit_runs add column if not exists latency_ms integer;
alter table if exists audit_runs add column if not exists last_attempt_at timestamptz;
alter table if exists audit_runs add column if not exists retryable_failure boolean not null default false;

create table if not exists checkpoint_runs (
    id bigserial primary key,
    project_id text not null,
    branch text not null,
    target_branch text,
    stage text,
    checkpoint_type text not null,
    initiated_by text,
    status text not null default 'running',
    git_status_json jsonb,
    code_status_json jsonb,
    db_status_json jsonb,
    runtime_status_json jsonb,
    audit_status_json jsonb,
    final_summary text,
    can_merge boolean,
    created_at timestamptz not null default now(),
    finished_at timestamptz
);

create index if not exists idx_checkpoint_runs_project_branch on checkpoint_runs(project_id, branch, created_at desc);
create index if not exists idx_checkpoint_runs_status on checkpoint_runs(status, created_at desc);
