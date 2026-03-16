drop view if exists vw_image_creative_audit_report cascade;
drop table if exists mart_direct_image_audit cascade;

create table mart_direct_image_audit (
    created_at timestamp default now(),
    audit_date date,
    campaign_name text,
    ad_group_id bigint,
    ad_title text,
    ad_type text,
    thumbnail_url text,
    preview_url text,

    visual_score numeric,
    readability_score numeric,
    focus_score numeric,
    offer_match_score numeric,
    child_fit_score numeric,
    clutter_score numeric,

    verdict text,
    recommendation text,

    raw_llm_response text
);

create index if not exists idx_mart_direct_image_audit_date
    on mart_direct_image_audit(audit_date);

create index if not exists idx_mart_direct_image_audit_group
    on mart_direct_image_audit(ad_group_id);

create view vw_image_creative_audit_report as
select
    audit_date,
    campaign_name,
    ad_group_id,
    ad_title,
    ad_type,
    thumbnail_url,
    preview_url,
    round(visual_score, 2) as visual_score,
    round(readability_score, 2) as readability_score,
    round(focus_score, 2) as focus_score,
    round(offer_match_score, 2) as offer_match_score,
    round(child_fit_score, 2) as child_fit_score,
    round(clutter_score, 2) as clutter_score,
    verdict,
    recommendation,
    created_at
from mart_direct_image_audit;
