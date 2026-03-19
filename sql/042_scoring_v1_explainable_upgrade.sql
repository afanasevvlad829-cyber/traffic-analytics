alter table if exists mart_visitor_scoring
    add column if not exists human_explanation text;

alter table if exists mart_visitor_scoring
    add column if not exists short_reason text;

alter table if exists mart_visitor_scoring
    add column if not exists recommended_action text;

alter table if exists mart_visitor_scoring
    add column if not exists data_source text;

update mart_visitor_scoring
set
    recommended_action = coalesce(recommended_action, recommendation),
    human_explanation = coalesce(human_explanation, ''),
    short_reason = coalesce(short_reason, '')
where recommended_action is null
   or human_explanation is null
   or short_reason is null;

create index if not exists idx_mart_visitor_scoring_short_reason
    on mart_visitor_scoring(short_reason);
