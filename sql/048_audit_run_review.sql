-- Post-audit user review workflow fields.
-- Additive / idempotent.

alter table if exists audit_runs add column if not exists review_status text;
alter table if exists audit_runs add column if not exists reviewed_at timestamptz;
alter table if exists audit_runs add column if not exists reviewed_by text;
alter table if exists audit_runs add column if not exists review_comment text;

create index if not exists idx_audit_runs_review_status_created_at on audit_runs(review_status, created_at desc);

