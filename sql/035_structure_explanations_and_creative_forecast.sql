alter table if exists mart_ai_creative_candidates
    add column if not exists predicted_ctr_pct numeric,
    add column if not exists predicted_cpc numeric,
    add column if not exists predicted_relevance numeric,
    add column if not exists prediction_confidence text,
    add column if not exists prediction_reason text;

create table if not exists mart_ai_creative_forecast_review (
    created_at timestamp default now(),
    review_date date,
    campaign_name text,
    ad_group_id bigint,
    ad_id bigint,
    variant text,
    predicted_ctr_pct numeric,
    predicted_cpc numeric,
    predicted_relevance numeric,
    actual_ctr_pct numeric,
    actual_cpc numeric,
    actual_relevance numeric,
    forecast_status text,
    comment text
);

create index if not exists idx_mart_ai_creative_forecast_review_date
    on mart_ai_creative_forecast_review(review_date);

create index if not exists idx_mart_ai_creative_forecast_review_ad
    on mart_ai_creative_forecast_review(ad_id);
