create table if not exists stg_metrica_visitors_features (
    visitor_id text primary key,
    session_id text,
    first_seen_at timestamp,
    last_seen_at timestamp,
    sessions_count integer not null default 0,
    total_time_sec integer not null default 0,
    pageviews integer not null default 0,
    visited_price_page boolean not null default false,
    visited_program_page boolean not null default false,
    visited_booking_page boolean not null default false,
    clicked_booking_button boolean not null default false,
    scroll_70 boolean not null default false,
    return_visitor boolean not null default false,
    traffic_source text,
    utm_source text,
    utm_medium text,
    device_type text,
    is_bounce boolean not null default false,
    source_table text not null default 'manual',
    loaded_at timestamp not null default now(),
    updated_at timestamp not null default now()
);

create index if not exists idx_stg_metrica_visitors_features_last_seen
    on stg_metrica_visitors_features(last_seen_at desc);

create index if not exists idx_stg_metrica_visitors_features_source
    on stg_metrica_visitors_features(traffic_source);

create table if not exists mart_visitor_scoring (
    visitor_id text primary key,
    session_id text,
    first_seen_at timestamp,
    last_seen_at timestamp,
    sessions_count integer not null default 0,
    total_time_sec integer not null default 0,
    pageviews integer not null default 0,
    visited_price_page boolean not null default false,
    visited_program_page boolean not null default false,
    visited_booking_page boolean not null default false,
    clicked_booking_button boolean not null default false,
    scroll_70 boolean not null default false,
    return_visitor boolean not null default false,
    traffic_source text,
    utm_source text,
    utm_medium text,
    device_type text,
    raw_score integer not null,
    normalized_score numeric(6,4) not null,
    segment text not null,
    explanation_json jsonb not null default '{}'::jsonb,
    human_explanation text,
    short_reason text,
    recommendation text,
    recommended_action text,
    data_source text,
    scoring_version text not null,
    scored_at timestamp not null default now(),
    constraint chk_mart_visitor_scoring_segment
        check (segment in ('hot', 'warm', 'cold'))
);

create index if not exists idx_mart_visitor_scoring_segment
    on mart_visitor_scoring(segment);

create index if not exists idx_mart_visitor_scoring_source
    on mart_visitor_scoring(traffic_source);

create index if not exists idx_scoring_scored_at
    on mart_visitor_scoring(scored_at desc);

create index if not exists idx_mart_visitor_scoring_norm
    on mart_visitor_scoring(normalized_score desc);

create index if not exists idx_mart_visitor_scoring_short_reason
    on mart_visitor_scoring(short_reason);
