alter table if exists stg_direct_ads_meta
    add column if not exists ad_type text,
    add column if not exists ad_subtype text,
    add column if not exists ad_image_hash text,
    add column if not exists creative_id bigint,
    add column if not exists thumbnail_url text,
    add column if not exists preview_url text;

create index if not exists idx_stg_direct_ads_meta_ad_group_id
    on stg_direct_ads_meta(ad_group_id);

create index if not exists idx_stg_direct_ads_meta_campaign_id
    on stg_direct_ads_meta(campaign_id);
