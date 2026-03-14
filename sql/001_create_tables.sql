create table if not exists stg_direct_campaign_daily (
    date date not null,
    campaign_id bigint not null,
    campaign_name text,
    impressions bigint,
    clicks bigint,
    cost numeric(14,2),
    loaded_at timestamp default now(),
    primary key (date, campaign_id)
);

create table if not exists stg_metrica_source_daily (
    date date not null,
    counter_id bigint not null,
    traffic_source text,
    source_engine text,
    source_medium text,
    campaign_name text,
    sessions bigint,
    users bigint,
    loaded_at timestamp default now(),
    primary key (date, counter_id, traffic_source, source_engine, source_medium, campaign_name)
);

create table if not exists stg_webmaster_query_daily (
    date date not null,
    site_id text not null,
    query_text text not null,
    page_url text,
    impressions bigint,
    clicks bigint,
    ctr numeric(8,4),
    avg_position numeric(8,2),
    loaded_at timestamp default now(),
    primary key (date, site_id, query_text, page_url)
);

create table if not exists mart_channel_daily (
    date date not null,
    channel_group text not null,
    sessions bigint,
    users bigint,
    leads bigint,
    cost numeric(14,2),
    cpl numeric(14,2),
    conversion_rate numeric(8,4)
);
